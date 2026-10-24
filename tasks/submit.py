"""Route registered tasks through an injectable asynchronous transport."""

import asyncio
import math
from dataclasses import replace

from execution.models import Task, TaskResult, TaskStatus
from tasks.router import NoAvailableNodes, select_node


class TaskClient:
    """Read a fresh gossip snapshot per attempt and retry transport failures.

    snapshot: callable returning NodeInfo values or a node_id -> NodeInfo mapping.
    send: async callable (NodeInfo, Task) -> TaskResult, e.g. send_task.
    Application failures are returned without automatically repeating side effects.
    """

    def __init__(self, snapshot, send, *, timeout=5.0, **routing_policy):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be positive")
        self.snapshot, self.send = snapshot, send
        self.timeout = timeout
        self.routing_policy = routing_policy

    async def submit_task(
        self, task_name, *args, max_retries=2, timeout_seconds=None, **kwargs
    ):
        return await self.submit(
            Task(
                task_name,
                args=args,
                kwargs=kwargs,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
            )
        )

    async def submit(self, task):
        excluded = set()
        attempts = 0
        last_error = "No worker available"
        for _ in range(task.max_retries + 1):
            try:
                peer = select_node(
                    self.snapshot(), exclude=excluded, **self.routing_policy
                )
            except NoAvailableNodes as exc:
                last_error = str(exc)
                break
            attempts += 1
            try:
                result = await asyncio.wait_for(
                    self.send(peer, task), task.timeout_seconds or self.timeout
                )
                if not isinstance(result, TaskResult) or result.task_id != task.task_id:
                    raise ValueError("Worker returned a mismatched task result")
                if result.status not in (
                    TaskStatus.COMPLETED,
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                ):
                    raise ValueError("Worker returned a non-terminal task status")
                return replace(result, attempts=attempts)
            except (TimeoutError, OSError, EOFError, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                excluded.add(peer.node_id)
        return TaskResult(
            task.task_id, TaskStatus.FAILED, attempts=attempts, error=last_error
        )
