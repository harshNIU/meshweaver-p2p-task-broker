"""TCP adapter for the team's JSON registered-function executor.

This is a trusted-LAN review adapter. It does not deserialize arbitrary code.
Optional SSL contexts allow the networking/security owner to supply TLS.
"""

import asyncio
import json
from collections import OrderedDict
from dataclasses import asdict

from execution.models import TaskResult, TaskStatus
from execution.serializer import deserialize_task, task_to_dict
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.tcp_node import read_message, send_message

MAX_FRAME_BYTES = 1024 * 1024


async def send_task(peer, task, *, ssl_context=None):
    reader, writer = await asyncio.open_connection(
        peer.host, peer.port, ssl=ssl_context, limit=MAX_FRAME_BYTES
    )
    try:
        await send_message(
            writer,
            Message(TASK_REQUEST, "task-client", 1, {"task": task_to_dict(task)}),
        )
        response = await read_message(reader)
        if response.type != TASK_RESULT or not isinstance(response.payload, dict):
            raise ValueError("Invalid task response")
        data = response.payload
        if data.get("task_id") != task.task_id:
            raise ValueError("Mismatched task ID")
        return TaskResult(
            task.task_id,
            TaskStatus(data["status"]),
            value=data.get("value"),
            error=data.get("error"),
            duration_seconds=data.get("duration_seconds", 0.0),
        )
    finally:
        writer.close()
        await writer.wait_closed()


class TaskServer:
    """Bound concurrent work and coalesce duplicate IDs on this worker."""

    def __init__(self, engine, *, workers=4, cache_size=1000):
        if workers < 1 or cache_size < 1:
            raise ValueError("workers and cache_size must be positive")
        self.engine = engine
        self._slots = asyncio.Semaphore(workers)
        self._cache_size = cache_size
        self._workers = workers
        self._results = OrderedDict()
        self._running = {}
        self._handlers = set()
        self.server = None

    async def start(self, host="127.0.0.1", port=0, *, ssl_context=None):
        self.server = await asyncio.start_server(
            self._handle, host, port, ssl=ssl_context, limit=MAX_FRAME_BYTES
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
            task = deserialize_task(json.dumps(request.payload["task"]))
            result = self._results.get(task.task_id)
            if result is None:
                pending = self._running.get(task.task_id)
                if pending is None:
                    # Reject excess requests rather than growing an unbounded queue.
                    if len(self._running) >= self._workers:
                        raise ValueError("Worker is busy")
                    pending = asyncio.create_task(self._execute(task))
                    self._running[task.task_id] = pending
                    pending.add_done_callback(
                        lambda done, key=task.task_id: self._running.pop(key, None)
                    )
                result = await asyncio.shield(pending)
            await send_message(
                writer, Message(TASK_RESULT, "task-worker", request.seq, asdict(result))
            )
        except (ValueError, TypeError, KeyError, EOFError, OSError, TimeoutError):
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
        await asyncio.gather(*list(self._handlers), return_exceptions=True)
        # Running Python functions cannot safely be killed; drain them on shutdown.
        await asyncio.gather(*list(self._running.values()), return_exceptions=True)
