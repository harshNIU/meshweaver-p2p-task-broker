"""Task lifecycle tracking for MeshWeaver."""

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import ClassVar


class TaskState(str, Enum):
    """Lifecycle states defined by the MeshWeaver project contract."""

    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


@dataclass
class TaskRecord:
    """Public task fields shared with routing and dashboard components."""

    task_id: str
    name: str
    state: TaskState = TaskState.PENDING
    node: str = "-"
    attempts: int = 0


class TaskLifecycle:
    """Thread-safe task state tracker with validated transitions."""

    _TRANSITIONS: ClassVar[dict[TaskState, set[TaskState]]] = {
        TaskState.PENDING: {TaskState.ASSIGNED, TaskState.FAILED},
        TaskState.ASSIGNED: {TaskState.RUNNING, TaskState.FAILED},
        TaskState.RUNNING: {TaskState.DONE, TaskState.FAILED},
        TaskState.DONE: set(),
        TaskState.FAILED: set(),
    }

    def __init__(self) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._lock = RLock()

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
            self._tasks[task_id] = record
            return record

    def transition(
        self,
        task_id: str,
        new_state: TaskState,
        *,
        node: str | None = None,
    ) -> TaskRecord:
        """Move a task to a valid next state."""
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

            if node is not None:
                if not isinstance(node, str) or not node.strip():
                    raise ValueError("node must be a non-empty string")
                record.node = node

            if new_state == TaskState.RUNNING:
                record.attempts += 1

            record.state = new_state
            return record

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
