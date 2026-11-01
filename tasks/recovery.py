"""Coordinate task recovery and replacement-worker submission."""

import inspect
from collections.abc import Callable
from threading import RLock

from execution.models import Task, TaskResult, TaskStatus
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.router import NoAvailableNodes, select_node
from tasks.store import SQLiteTaskStore


class TaskRecoveryCoordinator:
    """Recover tasks after worker failure or coordinator interruption.

    The submit callback accepts (replacement_node, original_task) and may
    return a TaskResult directly or an awaitable resolving to TaskResult.

    Recovery is at-least-once. It does not guarantee exactly-once execution.
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
        store: SQLiteTaskStore | None = None,
    ) -> None:
        if not callable(snapshot):
            raise TypeError("snapshot must be callable")
        if not callable(submit):
            raise TypeError("submit must be callable")

        self.lifecycle = lifecycle
        self.snapshot = snapshot
        self.submit = submit
        self.store = store
        self.routing_policy = {
            "max_cpu": max_cpu,
            "max_ram": max_ram,
            "max_age": max_age,
        }
        self._tasks: dict[str, Task] = {}
        self._lock = RLock()

        if self.store is not None:
            self._tasks = {
                task.task_id: task for task in self.store.list_task_payloads()
            }

    def register(self, task: Task, node_id: str) -> None:
        """Register a running task and retain its original payload."""
        if not isinstance(task, Task):
            raise TypeError("task must be a Task")
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")

        with self._lock:
            if task.task_id in self._tasks:
                raise ValueError(f"Task already registered: {task.task_id}")
            if self.lifecycle.get(task.task_id) is not None:
                raise ValueError(f"Task already exists: {task.task_id}")
            if self.store is not None and self.store.get(task.task_id) is not None:
                raise ValueError(f"Task already exists in storage: {task.task_id}")

            payload_saved = False
            lifecycle_created = False

            try:
                if self.store is not None:
                    self.store.save_task(task)
                    payload_saved = True

                self.lifecycle.create(task.task_id, task.task_name)
                lifecycle_created = True

                self.lifecycle.transition(
                    task.task_id,
                    TaskState.ASSIGNED,
                    node=node_id,
                )
                self.lifecycle.transition(task.task_id, TaskState.RUNNING)

            except Exception:
                if lifecycle_created:
                    self.lifecycle.discard(task.task_id)

                if payload_saved and self.store is not None:
                    self.store.delete_task(task.task_id)

                raise

            self._tasks[task.task_id] = task

    def _mark_failed(self, task_id: str) -> None:
        """Mark a reserved recovery attempt as failed."""
        with self._lock:
            record = self.lifecycle.get(task_id)
            if record is not None and record.state == TaskState.ASSIGNED:
                self.lifecycle.transition(task_id, TaskState.FAILED)

    async def _submit_reserved(
        self,
        task: Task,
        replacement: object,
    ) -> TaskResult:
        """Submit a task already reserved on a replacement node."""
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

        except Exception as exc:  # noqa: BLE001
            self._mark_failed(task.task_id)
            return TaskResult(
                task.task_id,
                TaskStatus.FAILED,
                error=("Recovery outcome uncertain: " f"{type(exc).__name__}: {exc}"),
            )

        with self._lock:
            record = self.lifecycle.get(task.task_id)

            if (
                record is None
                or record.state != TaskState.ASSIGNED
                or record.node != replacement.node_id
            ):
                return TaskResult(
                    task.task_id,
                    TaskStatus.FAILED,
                    error="Recovery reservation changed before result was recorded",
                )

            if submission.status == TaskStatus.COMPLETED:
                self.lifecycle.transition(task.task_id, TaskState.RUNNING)
                self.lifecycle.transition(task.task_id, TaskState.DONE)
            else:
                self.lifecycle.transition(task.task_id, TaskState.FAILED)

        return submission

    async def recover_node(self, dead_node_id: str) -> list[TaskResult]:
        """Recover RUNNING tasks from a dead worker."""
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
                continue

            try:
                with self._lock:
                    self.lifecycle.reassign_running(
                        task_id, dead_node_id, replacement.node_id
                    )
            except (KeyError, ValueError):
                continue

            results.append(await self._submit_reserved(task, replacement))

        return results

    async def recover_interrupted(self) -> list[TaskResult]:
        """Recover persisted ASSIGNED tasks after coordinator restart.

        The previously assigned worker is excluded because the coordinator
        cannot safely assume that the old submission did not start.
        """
        results: list[TaskResult] = []

        with self._lock:
            candidates = [
                item
                for item in self.lifecycle.snapshot()
                if item["state"] == TaskState.ASSIGNED.value
            ]

        for item in candidates:
            task_id = str(item["task_id"])
            previous_node = str(item["node"])

            with self._lock:
                task = self._tasks.get(task_id)

            if task is None:
                # Keep the durable record ASSIGNED. The payload may be
                # temporarily unavailable or stored separately.
                continue

            try:
                replacement = select_node(
                    self.snapshot(),
                    exclude={previous_node},
                    **self.routing_policy,
                )
            except NoAvailableNodes:
                # Leave ASSIGNED so a later recovery pass can retry.
                continue

            try:
                with self._lock:
                    self.lifecycle.reassign_assigned(
                        task_id,
                        previous_node,
                        replacement.node_id,
                    )
            except (KeyError, ValueError):
                continue

            results.append(await self._submit_reserved(task, replacement))

        return results
