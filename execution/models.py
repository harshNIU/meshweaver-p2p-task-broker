"""Core data models for MeshWeaver task execution."""

import math
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
        if not isinstance(self.task_name, str) or not self.task_name.strip():
            raise ValueError("task_name must be a non-empty string")

        if not isinstance(self.task_id, str) or not self.task_id.strip():
            raise ValueError("task_id must be a non-empty string")

        if not isinstance(self.args, tuple):
            raise TypeError("args must be a tuple")

        if not isinstance(self.kwargs, dict):
            raise TypeError("kwargs must be a dictionary")

        if not all(isinstance(key, str) for key in self.kwargs):
            raise TypeError("kwargs keys must be strings")

        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or self.max_retries < 0
        ):
            raise ValueError("max_retries must be a non-negative integer")

        if self.timeout_seconds is not None and (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a finite positive number")


@dataclass
class TaskResult:
    """The outcome of executing a task."""

    task_id: str
    status: TaskStatus
    attempts: int = 0
    value: Any = None
    error: str | None = None
    duration_seconds: float = 0.0
