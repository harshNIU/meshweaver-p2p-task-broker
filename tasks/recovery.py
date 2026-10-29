"""Coordinate task recovery and replacement-worker submission."""

import inspect
from collections.abc import Callable
from threading import RLock

from execution.models import Task, TaskResult, TaskStatus
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.router import NoAvailableNodes, select_node


class TaskRecoveryCoordinator:
    """Recover running tasks after a worker failure.

    The submit callback accepts (replacement_node, original_task) and may
    return a TaskResult directly or an awaitable resolving to TaskResult.

    A submission error is treated as an uncertain execution outcome. The
    lifecycle is marked FAILED to prevent an automatic duplicate submission.
    This does not guarantee exactly-once execution.
    """

    def __init__(
        self,
        lifecycle: TaskLifecycle,
        snapshot: Callable,
        submit: Callable,
        *,
        max_cpu: float = 95.0,
        max_ram: float = 95.0,
        max_age: float = 15.0,
    ) -> None:
        if not callable(snapshot):
            raise TypeError("snapshot must be callable")
        if not callable(submit):
            raise TypeError("submit must be callable")

        self.lifecycle = lifecycle
        self.snapshot = snapshot
        self.submit = submit
        self.routing_policy = {
            "max_cpu": max_cpu,
            "max_ram": max_ram,
            "max_age": max_age,
        }
        self._tasks: dict[str, Task] = {}
        self._lock = RLock()

    def register(self, task: Task, node_id: str) -> None:
        """Register a running task and retain its original payload."""
        if not isinstance(task, Task):
            raise TypeError("task must be a Task")
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")

        with self._lock:
            if task.task_id in self._tasks:
                raise ValueError(f"Task already registered: {task.task_id}")

            self.lifecycle.create(task.task_id, task.task_name)
            self.lifecycle.transition(task.task_id, TaskState.ASSIGNED, node=node_id)
            self.lifecycle.transition(task.task_id, TaskState.RUNNING)
            self._tasks[task.task_id] = task

    def _mark_failed(self, task_id: str) -> None:
        """Mark an assigned recovery attempt as failed."""
        record = self.lifecycle.get(task_id)
        if record is not None and record.state == TaskState.ASSIGNED:
            self.lifecycle.transition(task_id, TaskState.FAILED)

    async def recover_node(self, dead_node_id: str) -> list[TaskResult]:
        """Recover tasks from a dead node using healthy replacement workers."""
        if not isinstance(dead_node_id, str) or not dead_node_id.strip():
            raise ValueError("dead_node_id must be a non-empty string")

        results: list[TaskResult] = []

        with self._lock:
            candidates = [
                item
                for item in self.lifecycle.snapshot()
                if item["node"] == dead_node_id
                and item["state"] == TaskState.RUNNING.value
            ]

        for item in candidates:
            task_id = str(item["task_id"])

            with self._lock:
                task = self._tasks.get(task_id)

            if task is None:
                continue

            try:
                replacement = select_node(
                    self.snapshot(),
                    exclude={dead_node_id},
                    **self.routing_policy,
                )
            except NoAvailableNodes:
                # Keep the task eligible for a later recovery attempt.
                continue

            # Reserve the task before submitting it. Concurrent recovery
            # attempts will no longer find it RUNNING on the dead node.
            try:
                with self._lock:
                    self.lifecycle.reassign_running(
                        task_id, dead_node_id, replacement.node_id
                    )
            except (KeyError, ValueError):
                # Another recovery or lifecycle operation may have won.
                continue

            try:
                submission = self.submit(replacement, task)
                if inspect.isawaitable(submission):
                    submission = await submission

                if not isinstance(submission, TaskResult):
                    raise TypeError("submit must return a TaskResult")

                if submission.task_id != task.task_id:
                    raise ValueError("Worker returned a mismatched task result")

                if submission.status not in (
                    TaskStatus.COMPLETED,
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                ):
                    raise ValueError("Worker returned a non-terminal task result")

            except Exception as exc:
                self._mark_failed(task_id)
                results.append(
                    TaskResult(
                        task.task_id,
                        TaskStatus.FAILED,
                        error=(
                            "Recovery outcome uncertain: "
                            f"{type(exc).__name__}: {exc}"
                        ),
                    )
                )
                continue

            with self._lock:
                record = self.lifecycle.get(task_id)

                # Do not overwrite state changed by another operation.
                if (
                    record is None
                    or record.state != TaskState.ASSIGNED
                    or record.node != replacement.node_id
                ):
                    continue

                if submission.status == TaskStatus.COMPLETED:
                    self.lifecycle.transition(task_id, TaskState.RUNNING)
                    self.lifecycle.transition(task_id, TaskState.DONE)
                else:
                    self.lifecycle.transition(task_id, TaskState.FAILED)

            results.append(submission)

        return results
