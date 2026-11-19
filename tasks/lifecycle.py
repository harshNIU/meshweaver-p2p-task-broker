"""Task lifecycle tracking for MeshWeaver."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from tasks.store import SQLiteTaskStore


class TaskState(str, Enum):
    """Lifecycle states defined by the MeshWeaver project contract."""

    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


class StaleTaskStateError(RuntimeError):
    """Raised when persisted task state changed since it was loaded locally."""


@dataclass
class TaskRecord:
    """Public task fields shared with routing and dashboard components."""

    task_id: str
    name: str
    state: TaskState = TaskState.PENDING
    node: str = "-"
    attempts: int = 0


class TaskLifecycle:
    """Thread-safe task state tracker with optional SQLite persistence."""

    _TRANSITIONS: ClassVar[dict[TaskState, set[TaskState]]] = {
        TaskState.PENDING: {TaskState.ASSIGNED, TaskState.FAILED},
        TaskState.ASSIGNED: {TaskState.RUNNING, TaskState.FAILED},
        TaskState.RUNNING: {TaskState.DONE, TaskState.FAILED},
        TaskState.DONE: set(),
        TaskState.FAILED: set(),
    }

    def __init__(self, store: SQLiteTaskStore | None = None) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._lock = RLock()
        self._store = store

        if store is not None:
            for item in store.list_tasks():
                self.restore(
                    task_id=item["task_id"],
                    name=item["name"],
                    state=TaskState(item["state"]),
                    node=item["node"],
                    attempts=item["attempts"],
                )

    @property
    def store(self) -> SQLiteTaskStore | None:
        """Return the persistence store used by this lifecycle tracker."""
        return self._store

    def _persist_new(self, record: TaskRecord) -> None:
        """Insert a new record without overwriting an existing task."""
        if self._store is not None:
            self._store.insert_record(
                record.task_id,
                record.name,
                record.state,
                record.node,
                record.attempts,
            )

    def _persist_transition(
        self,
        record: TaskRecord,
        *,
        expected_state: TaskState,
        expected_node: str,
        expected_attempts: int,
    ) -> None:
        """Persist a transition only if its expected prior version still exists."""
        if self._store is None:
            return

        updated = self._store.compare_and_set_state(
            task_id=record.task_id,
            expected_state=expected_state,
            expected_node=expected_node,
            expected_attempts=expected_attempts,
            new_state=record.state,
            new_node=record.node,
            new_attempts=record.attempts,
        )
        if not updated:
            raise StaleTaskStateError(
                f"Persisted state for task {record.task_id} changed concurrently"
            )

    def create(self, task_id: str, name: str) -> TaskRecord:
        """Create a task in PENDING state."""
        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("task_id must be a non-empty string")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")

        with self._lock:
            if task_id in self._tasks:
                raise ValueError(f"Task already exists: {task_id}")

            record = TaskRecord(task_id=task_id, name=name)
            self._persist_new(record)
            self._tasks[task_id] = record
            return record

    def restore(
        self,
        task_id: str,
        name: str,
        state: TaskState,
        node: str,
        attempts: int,
    ) -> TaskRecord:
        """Restore a persisted task record without replaying transitions."""
        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("task_id must be a non-empty string")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        if not isinstance(state, TaskState):
            raise TypeError("state must be a TaskState")
        if not isinstance(node, str):
            raise TypeError("node must be a string")
        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
            raise ValueError("attempts must be a non-negative integer")

        with self._lock:
            if task_id in self._tasks:
                raise ValueError(f"Task already exists: {task_id}")

            record = TaskRecord(
                task_id=task_id,
                name=name,
                state=state,
                node=node,
                attempts=attempts,
            )
            self._tasks[task_id] = record
            return record

    def update_running_attempts(
        self,
        task_id: str,
        attempts: int,
    ) -> TaskRecord:
        """Persist an updated retry count while the task remains RUNNING."""
        if isinstance(attempts, bool) or not isinstance(attempts, int):
            raise TypeError("attempts must be an integer")
        if attempts < 0:
            raise ValueError("attempts must be a non-negative integer")

        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                raise KeyError(f"Unknown task: {task_id}")
            if record.state != TaskState.RUNNING:
                raise ValueError("Attempts can only be updated for running tasks")
            if attempts <= record.attempts:
                return record

            old_attempts = record.attempts
            record.attempts = attempts

            try:
                self._persist_transition(
                    record,
                    expected_state=TaskState.RUNNING,
                    expected_node=record.node,
                    expected_attempts=old_attempts,
                )
            except Exception:
                record.attempts = old_attempts
                raise

            return record

    def transition(
        self,
        task_id: str,
        new_state: TaskState,
        *,
        node: str | None = None,
    ) -> TaskRecord:
        """Move a task to a valid next state using an atomic persisted update."""
        if not isinstance(new_state, TaskState):
            raise TypeError("new_state must be a TaskState")

        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                raise KeyError(f"Unknown task: {task_id}")

            if new_state not in self._TRANSITIONS[record.state]:
                raise ValueError(
                    f"Invalid task transition: {record.state.value} "
                    f"-> {new_state.value}"
                )

            old_state = record.state
            old_node = record.node
            old_attempts = record.attempts

            if node is not None:
                if not isinstance(node, str) or not node.strip():
                    raise ValueError("node must be a non-empty string")
                record.node = node

            if new_state == TaskState.RUNNING:
                record.attempts += 1

            record.state = new_state

            try:
                self._persist_transition(
                    record,
                    expected_state=old_state,
                    expected_node=old_node,
                    expected_attempts=old_attempts,
                )
            except Exception:
                record.state = old_state
                record.node = old_node
                record.attempts = old_attempts
                raise

            return record

    def reassign_running(
        self,
        task_id: str,
        dead_node_id: str,
        new_node_id: str,
    ) -> TaskRecord:
        """Reassign a running task from a dead worker to a replacement."""
        if not isinstance(dead_node_id, str) or not dead_node_id.strip():
            raise ValueError("dead_node_id must be a non-empty string")
        if not isinstance(new_node_id, str) or not new_node_id.strip():
            raise ValueError("new_node_id must be a non-empty string")
        if new_node_id == dead_node_id:
            raise ValueError("new_node_id must differ from dead_node_id")

        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                raise KeyError(f"Unknown task: {task_id}")
            if record.state != TaskState.RUNNING:
                raise ValueError("Only running tasks can be reassigned")
            if record.node != dead_node_id:
                raise ValueError("Task is not assigned to the dead node")

            old_node = record.node
            old_state = record.state
            old_attempts = record.attempts
            record.node = new_node_id
            record.state = TaskState.ASSIGNED

            try:
                self._persist_transition(
                    record,
                    expected_state=old_state,
                    expected_node=old_node,
                    expected_attempts=old_attempts,
                )
            except Exception:
                record.node = old_node
                record.state = old_state
                raise

            return record

    def reassign_assigned(
        self,
        task_id: str,
        current_node_id: str,
        new_node_id: str,
    ) -> TaskRecord:
        """Move an interrupted ASSIGNED task to another worker.

        The task remains ASSIGNED; the next successful transition to RUNNING
        increments its attempt count.
        """
        if not isinstance(current_node_id, str) or not current_node_id.strip():
            raise ValueError("current_node_id must be a non-empty string")
        if not isinstance(new_node_id, str) or not new_node_id.strip():
            raise ValueError("new_node_id must be a non-empty string")
        if new_node_id == current_node_id:
            raise ValueError("new_node_id must differ from current_node_id")

        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                raise KeyError(f"Unknown task: {task_id}")
            if record.state != TaskState.ASSIGNED:
                raise ValueError("Only assigned tasks can be reassigned")
            if record.node != current_node_id:
                raise ValueError("Task is not assigned to the current node")

            old_node = record.node
            old_state = record.state
            old_attempts = record.attempts
            record.node = new_node_id

            try:
                self._persist_transition(
                    record,
                    expected_state=old_state,
                    expected_node=old_node,
                    expected_attempts=old_attempts,
                )
            except Exception:
                record.node = old_node
                raise

            return record

    def discard(self, task_id: str) -> None:
        """Remove a task record during rollback of incomplete registration."""
        with self._lock:
            if task_id not in self._tasks:
                return

            if self._store is not None:
                self._store.delete_task_record(task_id)

            self._tasks.pop(task_id, None)

    def get(self, task_id: str) -> TaskRecord | None:
        """Return a task record, or None if it does not exist."""
        with self._lock:
            return self._tasks.get(task_id)

    def snapshot(self) -> list[dict[str, object]]:
        """Return dashboard-friendly task dictionaries."""
        with self._lock:
            return [
                {
                    "task_id": task.task_id,
                    "name": task.name,
                    "state": task.state.value,
                    "node": task.node,
                    "attempts": task.attempts,
                }
                for task in self._tasks.values()
            ]
