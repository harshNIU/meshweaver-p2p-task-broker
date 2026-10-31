
"""TCP adapter for MeshWeaver's registered-function task executor."""

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import asdict
from uuid import uuid4

from execution.models import TaskResult, TaskStatus
from execution.serializer import deserialize_task, task_to_dict
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.tcp_node import (
    MAX_MESSAGE_SIZE,
    read_message,
    send_message,
)
from security.replay import ReplayGuard
from security.signing import sign_message


async def send_task(
    peer,
    task,
    *,
    ssl_context=None,
    secure=False,
    private_key=None,
    trust_store=None,
    sender_id="task-client",
):
    """Send a task and return its result.

    In secure mode, the request is signed and the worker's signed response
    is verified against the client's trusted-peer registry.
    """
    if secure and (private_key is None or trust_store is None):
        raise ValueError(
            "Secure client requires private_key and trust_store"
        )

    nonce = uuid4().hex

    request = Message(
        TASK_REQUEST,
        sender_id,
        1,
        {
            "task": task_to_dict(task),
            "nonce": nonce,
        } if secure else {"task": task_to_dict(task)},
        ts=time.time(),
    )

    if secure:
        sign_message(request, private_key)

    reader, writer = await asyncio.open_connection(
        peer.host,
        peer.port,
        ssl=ssl_context,
        limit=MAX_MESSAGE_SIZE,
    )

    try:
        await send_message(writer, request)
        response = await read_message(reader)

        if response.type != TASK_RESULT or not isinstance(
            response.payload, dict
        ):
            raise ValueError("Invalid task response")

        if secure:
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
    """Bound concurrent work and coalesce duplicate IDs on this worker."""

    def __init__(
        self,
        engine,
        *,
        workers=4,
        cache_size=1000,
        secure=False,
        trust_store=None,
        private_key=None,
        replay_guard=None,
        sender_id="task-worker",
    ):
        if workers < 1 or cache_size < 1:
            raise ValueError("workers and cache_size must be positive")

        if secure and (trust_store is None or private_key is None):
            raise ValueError(
                "Secure server requires trust_store and private_key"
            )

        self.engine = engine
        self._slots = asyncio.Semaphore(workers)
        self._cache_size = cache_size
        self._workers = workers
        self._results = OrderedDict()
        self._running = {}
        self._handlers = set()
        self.server = None

        self.secure = secure
        self.trust_store = trust_store
        self.private_key = private_key
        self.replay_guard = (
            replay_guard if replay_guard is not None else ReplayGuard()
        )
        self.sender_id = sender_id

    async def start(self, host="127.0.0.1", port=0, *, ssl_context=None):
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
        return self.server.sockets[0].getsockname()[:2]

    async def _execute(self, task):
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

            while len(self._results) > self._cache_size:
                self._results.popitem(last=False)

            return result

    async def _handle(self, reader, writer):
        handler = asyncio.current_task()
        self._handlers.add(handler)

        try:
            request = await asyncio.wait_for(read_message(reader), 10)

            if request.type != TASK_REQUEST:
                raise ValueError("Expected TASK_REQUEST")

            if self.secure:
                if not self.trust_store.verify_message(request):
                    raise ValueError("Invalid task request signature")

                # Check the nonce only after authenticating the sender.
                self.replay_guard.check_message(request)

            if not isinstance(request.payload, dict):
                raise TypeError("Task request payload must be an object")

            if self.secure:
                nonce = request.payload.get("nonce")
            else:
                nonce = None

            task_data = request.payload.get("task")
            if not isinstance(task_data, dict):
                raise TypeError("Task request must contain a task object")

            task = deserialize_task(json.dumps(task_data))

            result = self._results.get(task.task_id)

            if result is None:
                pending = self._running.get(task.task_id)

                if pending is None:
                    if len(self._running) >= self._workers:
                        raise ValueError("Worker is busy")

                    pending = asyncio.create_task(self._execute(task))
                    self._running[task.task_id] = pending
                    pending.add_done_callback(
                        lambda done, key=task.task_id:
                        self._running.pop(key, None)
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

            if self.secure:
                sign_message(response, self.private_key)

            await send_message(writer, response)

        except (
            ValueError,
            TypeError,
            KeyError,
            EOFError,
            OSError,
            TimeoutError,
        ):
            # Invalid or unauthenticated requests are rejected by closing
            # the connection without executing the task.
            pass
        finally:
            writer.close()

            try:
                await writer.wait_closed()
            except OSError:
                pass

            self._handlers.discard(handler)

    async def close(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

        for handler in list(self._handlers):
            handler.cancel()

        await asyncio.gather(
            *list(self._handlers),
            return_exceptions=True,
        )

        # Running Python functions cannot safely be killed; drain them.
        await asyncio.gather(
            *list(self._running.values()),
            return_exceptions=True,
        )
