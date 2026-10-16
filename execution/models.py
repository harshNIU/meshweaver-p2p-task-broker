
"""Core data models for MeshWeaver task execution."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4


class TaskStatus(str, Enum):
    """Lifecycle states for an execution task."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Task:
    """A registered task and its arguments."""

    task_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: str(uuid4()))
    max_retries: int = 0
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        if not self.task_name.strip():
            raise ValueError("task_name cannot be empty")

        if not self.task_id.strip():
            raise ValueError("task_id cannot be empty")

        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        if self.timeout_seconds is not None:
            if self.timeout_seconds <= 0:
                raise ValueError("timeout_seconds must be positive")


@dataclass
class TaskResult:
    """The outcome of executing a task."""

    task_id: str
    status: TaskStatus
    attempts: int = 0
    value: Any = None
    error: str | None = None
    duration_seconds: float = 0.0
