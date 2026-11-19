
"""Coordinate task recovery and replacement-worker submission."""

import asyncio
import inspect
import math
import sqlite3
import uuid
from collections.abc import Callable
from pathlib import Path
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
        recovery_lease_seconds: float = 30.0,
    ) -> None:
        if not isinstance(lifecycle, TaskLifecycle):
            raise TypeError("lifecycle must be a TaskLifecycle")

        if not callable(snapshot):
            raise TypeError("snapshot must be callable")

        if not callable(submit):
            raise TypeError("submit must be callable")

        if (
            isinstance(recovery_lease_seconds, bool)
            or not isinstance(recovery_lease_seconds, (int, float))
            or not math.isfinite(recovery_lease_seconds)
            or recovery_lease_seconds <= 0
        ):
            raise ValueError(
                "recovery_lease_seconds must be a finite positive number"
            )

        lifecycle_store = lifecycle.store

        if store is None:
            store = lifecycle_store
        elif lifecycle_store is None:
            raise ValueError(
                "A recovery store requires TaskLifecycle to use the same store"
            )
        elif store is not lifecycle_store:
            lifecycle_path = Path(lifecycle_store.db_path).resolve()
            recovery_path = Path(store.db_path).resolve()

            if lifecycle_path != recovery_path:
                raise ValueError(
                    "TaskRecoveryCoordinator and TaskLifecycle must use "
                    "the same persistence database"
                )

            store = lifecycle_store

        self.lifecycle = lifecycle
        self.snapshot = snapshot
        self.submit = submit
        self.store = store

        self.routing_policy = {
            "max_cpu": max_cpu,
            "max_ram": max_ram,
            "max_age": max_age,
        }

        self.recovery_lease_seconds = float(recovery_lease_seconds)
        self._recovery_owner_id = uuid.uuid4().hex

        self._tasks: dict[str, Task] = {}
        self._lock = RLock()
        self._recovering: set[str] = set()

        if self.store is not None:
            self._tasks = {
                task.task_id: task for task in self.store.list_task_payloads()
            }

    def _claim_recovery(self, task_id: str) -> Task | None:
        """Acquire both the database and in-memory recovery claims."""
        if self.store is not None:
            claimed = self.store.claim_recovery(
                task_id,
                self._recovery_owner_id,
                self.recovery_lease_seconds,
            )

            if not claimed:
                return None

        with self._lock:
            if task_id in self._recovering:
                if self.store is not None:
                    self.store.release_recovery_claim(
                        task_id,
                        self._recovery_owner_id,
                    )
                return None

            task = self._tasks.get(task_id)

            if task is None:
                if self.store is not None:
                    self.store.release_recovery_claim(
                        task_id,
                        self._recovery_owner_id,
                    )
                return None

            self._recovering.add(task_id)
            return task

    def _release_recovery(self, task_id: str) -> None:
        """Release the database and in-memory recovery claims."""
        try:
            if self.store is not None:
                self.store.release_recovery_claim(
                    task_id,
                    self._recovery_owner_id,
                )
        finally:
            with self._lock:
                self._recovering.discard(task_id)

    async def _maintain_recovery_claim(
        self,
        task_id: str,
        stop_event: asyncio.Event,
        lost_event: asyncio.Event,
    ) -> None:
        """Renew a recovery lease while its recovery operation is active."""
        if self.store is None:
            return

        interval = max(self.recovery_lease_seconds / 3, 0.1)

        while True:
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=interval,
                )
                return
            except TimeoutError:
                pass

            try:
                renewed = self.store.renew_recovery_claim(
                    task_id,
                    self._recovery_owner_id,
                    self.recovery_lease_seconds,
                )
            except sqlite3.Error:
                lost_event.set()
                return

            if not renewed:
                lost_event.set()
                return

    def _start_claim_heartbeat(
        self,
        task_id: str,
    ) -> tuple[asyncio.Event, asyncio.Event, asyncio.Task | None]:
        """Start automatic lease renewal for one recovery operation."""
        stop_event = asyncio.Event()
        lost_event = asyncio.Event()

        if self.store is None:
            return stop_event, lost_event, None

        heartbeat = asyncio.create_task(
            self._maintain_recovery_claim(
                task_id,
                stop_event,
                lost_event,
            )
        )

        return stop_event, lost_event, heartbeat

    async def _stop_claim_heartbeat(
        self,
        stop_event: asyncio.Event,
        heartbeat: asyncio.Task | None,
    ) -> None:
        """Stop and await the recovery lease renewal task."""
        stop_event.set()

        if heartbeat is not None:
            await heartbeat

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
                    self.lifecycle.restore(
                        task_id=task.task_id,
                        name=task.task_name,
                        state=TaskState.PENDING,
                        node="-",
                        attempts=0,
                    )
                else:
                    self.lifecycle.create(
                        task.task_id,
                        task.task_name,
                    )

                lifecycle_created = True

                self.lifecycle.transition(
                    task.task_id,
                    TaskState.ASSIGNED,
                    node=node_id,
                )

                self.lifecycle.transition(
                    task.task_id,
                    TaskState.RUNNING,
                )

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
                self.lifecycle.transition(
                    task_id,
                    TaskState.FAILED,
                )

    async def _submit_reserved(
        self,
        task: Task,
        replacement: object,
        claim_lost: asyncio.Event | None = None,
    ) -> TaskResult:
        """Submit a task already reserved on a replacement node."""
        if claim_lost is not None and claim_lost.is_set():
            return TaskResult(
                task.task_id,
                TaskStatus.FAILED,
                error="Recovery claim was lost before submission",
            )

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
                error=f"Recovery outcome uncertain: {type(exc).__name__}: {exc}",
            )

        if claim_lost is not None and claim_lost.is_set():
            return TaskResult(
                task.task_id,
                TaskStatus.FAILED,
                error="Recovery claim was lost while submission was in progress",
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
                self.lifecycle.transition(
                    task.task_id,
                    TaskState.RUNNING,
                )

                self.lifecycle.transition(
                    task.task_id,
                    TaskState.DONE,
                )
            else:
                self.lifecycle.transition(
                    task.task_id,
                    TaskState.FAILED,
                )

        return submission

    async def recover_node(
        self,
        dead_node_id: str,
    ) -> list[TaskResult]:
        """Recover RUNNING and ASSIGNED tasks from a dead worker."""
        if not isinstance(dead_node_id, str) or not dead_node_id.strip():
            raise ValueError("dead_node_id must be a non-empty string")

        results: list[TaskResult] = []

        with self._lock:
            candidates = [
                item
                for item in self.lifecycle.snapshot()
                if item["node"] == dead_node_id
                and item["state"]
                in (
                    TaskState.RUNNING.value,
                    TaskState.ASSIGNED.value,
                )
            ]

        for item in candidates:
            task_id = str(item["task_id"])
            previous_state = item["state"]

            task = self._claim_recovery(task_id)

            if task is None:
                continue

            stop_event, claim_lost, heartbeat = (
                self._start_claim_heartbeat(task_id)
            )

            try:
                try:
                    replacement = select_node(
                        self.snapshot(),
                        exclude={dead_node_id},
                        **self.routing_policy,
                    )
                except NoAvailableNodes:
                    continue

                if claim_lost.is_set():
                    continue

                try:
                    with self._lock:
                        current = self.lifecycle.get(task_id)

                        if (
                            current is None
                            or current.node != dead_node_id
                            or current.state.value != previous_state
                        ):
                            continue

                        if previous_state == TaskState.RUNNING.value:
                            self.lifecycle.reassign_running(
                                task_id,
                                dead_node_id,
                                replacement.node_id,
                            )
                        else:
                            self.lifecycle.reassign_assigned(
                                task_id,
                                dead_node_id,
                                replacement.node_id,
                            )

                except (KeyError, ValueError):
                    continue

                if claim_lost.is_set():
                    continue

                results.append(
                    await self._submit_reserved(
                        task,
                        replacement,
                        claim_lost,
                    )
                )

            finally:
                try:
                    await self._stop_claim_heartbeat(stop_event, heartbeat)
                finally:
                    self._release_recovery(task_id)

        return results

    async def recover_interrupted(self) -> list[TaskResult]:
        """Recover persisted PENDING, RUNNING and ASSIGNED tasks.

        RUNNING and ASSIGNED tasks are moved to a replacement worker because
        the coordinator cannot safely assume the previous submission did not
        start.

        PENDING tasks have not been assigned to a worker, so they can be
        assigned to any currently available worker.
        """
        results: list[TaskResult] = []

        with self._lock:
            candidates = [
                item
                for item in self.lifecycle.snapshot()
                if item["state"]
                in (
                    TaskState.PENDING.value,
                    TaskState.RUNNING.value,
                    TaskState.ASSIGNED.value,
                )
            ]

        for item in candidates:
            task_id = str(item["task_id"])
            previous_node = str(item["node"])
            previous_state = item["state"]

            task = self._claim_recovery(task_id)

            if task is None:
                continue

            stop_event, claim_lost, heartbeat = (
                self._start_claim_heartbeat(task_id)
            )

            try:
                try:
                    if previous_state == TaskState.PENDING.value:
                        replacement = select_node(
                            self.snapshot(),
                            **self.routing_policy,
                        )
                    else:
                        replacement = select_node(
                            self.snapshot(),
                            exclude={previous_node},
                            **self.routing_policy,
                        )
                except NoAvailableNodes:
                    continue

                if claim_lost.is_set():
                    continue

                try:
                    with self._lock:
                        current = self.lifecycle.get(task_id)

                        if (
                            current is None
                            or current.state.value != previous_state
                            or current.node != previous_node
                        ):
                            continue

                        if previous_state == TaskState.PENDING.value:
                            self.lifecycle.transition(
                                task_id,
                                TaskState.ASSIGNED,
                                node=replacement.node_id,
                            )
                        elif previous_state == TaskState.RUNNING.value:
                            self.lifecycle.reassign_running(
                                task_id,
                                previous_node,
                                replacement.node_id,
                            )
                        else:
                            self.lifecycle.reassign_assigned(
                                task_id,
                                previous_node,
                                replacement.node_id,
                            )

                except (KeyError, ValueError):
                    continue

                if claim_lost.is_set():
                    continue

                results.append(
                    await self._submit_reserved(
                        task,
                        replacement,
                        claim_lost,
                    )
                )

            finally:
                try:
                    await self._stop_claim_heartbeat(stop_event, heartbeat)
                finally:
                    self._release_recovery(task_id)

        return results
