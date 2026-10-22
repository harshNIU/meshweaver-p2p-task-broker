"""Task registration and execution for MeshWeaver."""

import time
from collections.abc import Callable
from typing import Any

from execution.models import Task, TaskResult, TaskStatus


class TaskRegistrationError(ValueError):
    """Raised when a task function cannot be registered."""


class ExecutionEngine:
    """Execute tasks using explicitly registered functions."""

    def __init__(self) -> None:
        self._registry: dict[str, Callable[..., Any]] = {}

    def register(
        self,
        name: str,
        function: Callable[..., Any],
        *,
        replace: bool = False,
    ) -> None:
        """Register a callable under a task name."""
        if not isinstance(name, str) or not name.strip():
            raise TaskRegistrationError("Task name must be a non-empty string.")

        if not callable(function):
            raise TaskRegistrationError("Registered task must be callable.")

        if name in self._registry and not replace:
            raise TaskRegistrationError(f"Task '{name}' is already registered.")

        self._registry[name] = function

    def unregister(self, name: str) -> bool:
        """Remove a registered function and report whether it existed."""
        return self._registry.pop(name, None) is not None

    def registered_tasks(self) -> tuple[str, ...]:
        """Return registered task names in registration order."""
        return tuple(self._registry)

    def execute(self, task: Task) -> TaskResult:
        """Execute a task and capture its result or failure."""
        function = self._registry.get(task.task_name)

        if function is None:
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                attempts=0,
                error=f"Unknown task: {task.task_name}",
            )

        started_at = time.perf_counter()

        try:
            value = function(*task.args, **task.kwargs)
        except Exception as exc:  # noqa: BLE001
            duration = time.perf_counter() - started_at
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                attempts=1,
                error=f"{type(exc).__name__}: {exc}",
                duration_seconds=duration,
            )

        duration = time.perf_counter() - started_at
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.COMPLETED,
            attempts=1,
            value=value,
            duration_seconds=duration,
        )
