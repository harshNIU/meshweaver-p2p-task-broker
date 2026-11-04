"""Authenticated TCP transport for MeshWeaver task execution.

Supports three connection configurations:
- HMAC-SHA256 authentication using a shared secret.
- Ed25519 authentication using trusted peer keys and replay protection.
- TLS encryption without mandatory message-level authentication.

TLS is independent of message authentication.
"""

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import asdict
from uuid import uuid4

from execution.models import TaskResult, TaskStatus
from execution.serializer import deserialize_task, task_to_dict
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.tcp_node import MAX_MESSAGE_SIZE, read_message, send_message
from security.message_signing import (
    MessageAuthenticationError,
)
from security.message_signing import (
    sign_message as sign_hmac_message,
)
from security.message_signing import (
    verify_message as verify_hmac_message,
)
from security.replay import ReplayGuard
from security.signing import sign_message as sign_ed25519_message


async def send_task(
    peer,
    task,
    *,
    ssl_context=None,
    secret=None,
    secure=False,
    private_key=None,
    trust_store=None,
    sender_id="task-client",
):
    """Submit a task remotely and verify the worker's response.

    Select either HMAC authentication or Ed25519 authentication.
    TLS can be used independently with either authentication mode,
    or without message-level authentication.
    """
    if secret is not None and secure:
        raise ValueError("Choose either HMAC or Ed25519 authentication, not both")

    if secure and (private_key is None or trust_store is None):
        raise ValueError("Secure client requires private_key and trust_store")

    nonce = uuid4().hex if secure else None

    payload = {"task": task_to_dict(task)}
    if secure:
        payload["nonce"] = nonce

    request = Message(
        TASK_REQUEST,
        sender_id,
        1,
        payload,
        ts=time.time(),
    )

    if secret is not None:
        sign_hmac_message(request, secret)
    elif secure:
        sign_ed25519_message(request, private_key)

    reader, writer = await asyncio.open_connection(
        peer.host,
        peer.port,
        ssl=ssl_context,
        limit=MAX_MESSAGE_SIZE,
    )

    try:
        await send_message(writer, request)
        response = await read_message(reader)

        if response.type != TASK_RESULT:
            raise ValueError("Invalid task response type")

        if not isinstance(response.payload, dict):
            raise TypeError("Invalid task response payload")

        if secret is not None:
            if not verify_hmac_message(response, secret):
                raise MessageAuthenticationError("Task response authentication failed")

        elif secure:
            if not trust_store.verify_message(response):
                raise ValueError("Worker response signature is invalid")

            if response.payload.get("nonce") != nonce:
                raise ValueError("Worker response nonce does not match request")

        data = response.payload

        if data.get("task_id") != task.task_id:
            raise ValueError("Mismatched task ID")

        return TaskResult(
            task.task_id,
            TaskStatus(data["status"]),
            attempts=data.get("attempts", 0),
            value=data.get("value"),
            error=data.get("error"),
            duration_seconds=data.get("duration_seconds", 0.0),
        )

    finally:
        writer.close()
        await writer.wait_closed()


class TaskServer:
    """Bound concurrent work and coalesce duplicate task IDs."""

    def __init__(
        self,
        engine,
        *,
        workers=4,
        cache_size=1000,
        secret=None,
        secure=False,
        trust_store=None,
        private_key=None,
        replay_guard=None,
        sender_id="task-worker",
    ):
        if workers < 1 or cache_size < 1:
            raise ValueError("workers and cache_size must be positive")

        if secret is not None and secure:
            raise ValueError("Choose either HMAC or Ed25519 authentication, not both")

        if secure and (trust_store is None or private_key is None):
            raise ValueError("Secure server requires trust_store and private_key")

        self.engine = engine
        self.secret = secret
        self.secure = secure
        self.trust_store = trust_store
        self.private_key = private_key
        self.replay_guard = replay_guard if replay_guard is not None else ReplayGuard()
        self.sender_id = sender_id

        self._slots = asyncio.Semaphore(workers)
        self._cache_size = cache_size
        self._workers = workers
        self._results = OrderedDict()
        self._running = {}
        self._handlers = set()
        self.server = None

    async def start(self, host="127.0.0.1", port=0, *, ssl_context=None):
        """Start the TCP task server."""
        self.server = await asyncio.start_server(
            self._handle,
            host,
            port,
            ssl=ssl_context,
            limit=MAX_MESSAGE_SIZE,
        )
        return self

    @property
    def address(self):
        """Return the bound server address."""
        return self.server.sockets[0].getsockname()[:2]

    async def _execute(self, task):
        """Execute a task and cache its result."""
        async with self._slots:
            result = await asyncio.to_thread(self.engine.execute, task)

            try:
                json.dumps(asdict(result), allow_nan=False)
            except (TypeError, ValueError):
                result = TaskResult(
                    task.task_id,
                    TaskStatus.FAILED,
                    error="Task result is not JSON serializable",
                )

            self._results[task.task_id] = result
            self._results.move_to_end(task.task_id)

            while len(self._results) > self._cache_size:
                self._results.popitem(last=False)

            return result

    async def _handle(self, reader, writer):
        """Authenticate, execute, and return a task result."""
        handler = asyncio.current_task()
        self._handlers.add(handler)

        try:
            request = await asyncio.wait_for(
                read_message(reader),
                timeout=10,
            )

            if request.type != TASK_REQUEST:
                raise ValueError("Expected TASK_REQUEST")

            # Authenticate before reading or deserializing task data.
            if self.secret is not None:
                verify_hmac_message(request, self.secret)

            elif self.secure:
                if not self.trust_store.verify_message(request):
                    raise ValueError("Invalid task request signature")

                # Only authenticated messages reach replay protection.
                self.replay_guard.check_message(request)

            if not isinstance(request.payload, dict):
                raise TypeError("Task request payload must be an object")

            nonce = request.payload.get("nonce") if self.secure else None

            task_data = request.payload.get("task")
            if not isinstance(task_data, dict):
                raise TypeError("Task request must contain a task object")

            task = deserialize_task(json.dumps(task_data))

            result = self._results.get(task.task_id)

            if result is not None:
                self._results.move_to_end(task.task_id)

            else:
                pending = self._running.get(task.task_id)

                if pending is None:
                    if len(self._running) >= self._workers:
                        raise ValueError("Worker is busy")

                    pending = asyncio.create_task(self._execute(task))
                    self._running[task.task_id] = pending

                    pending.add_done_callback(
                        lambda done, key=task.task_id: self._running.pop(key, None)
                    )

                result = await asyncio.shield(pending)

            response_payload = asdict(result)

            if self.secure:
                response_payload["nonce"] = nonce

            response = Message(
                TASK_RESULT,
                self.sender_id,
                request.seq,
                response_payload,
                ts=time.time(),
                correlation_id=request.correlation_id,
            )

            if self.secret is not None:
                sign_hmac_message(response, self.secret)

            elif self.secure:
                sign_ed25519_message(response, self.private_key)

            await send_message(writer, response)

        except (
            MessageAuthenticationError,
            ValueError,
            TypeError,
            KeyError,
            EOFError,
            OSError,
            TimeoutError,
        ):
            # Invalid or unauthenticated requests are rejected
            # without executing their task.
            pass

        finally:
            writer.close()

            try:
                await writer.wait_closed()
            except OSError:
                pass

            self._handlers.discard(handler)

    async def close(self):
        """Close the server and drain running task executions."""
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

        for handler in list(self._handlers):
            handler.cancel()

        await asyncio.gather(
            *list(self._handlers),
            return_exceptions=True,
        )

        # Running Python functions cannot safely be killed.
        # Drain them gracefully during shutdown.
        await asyncio.gather(
            *list(self._running.values()),
            return_exceptions=True,
        )
