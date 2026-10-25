"""Task registration and execution for MeshWeaver."""
import time
from collections.abc import Callable
from typing import Any

from execution.models import Task, TaskResult, TaskStatus
from execution.task_registry import TaskRegistry


class TaskRegistrationError(ValueError):
    """Raised when a task function cannot be registered."""


class ExecutionEngine:
    """Execute explicitly registered functions and record real lifecycle state."""

    def __init__(self, task_registry: TaskRegistry | None = None) -> None:
        self._registry: dict[str, Callable[..., Any]] = {}
        self.task_registry = task_registry

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
        """Return task names in registration order."""
        return tuple(self._registry)

    def execute(self, task: Task) -> TaskResult:
        """Execute one task and record PENDING/RUNNING/terminal states."""
        if self.task_registry is not None:
            self.task_registry.mark_pending(task)

        function = self._registry.get(task.task_name)
        if function is None:
            result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                attempts=0,
                error=f"Unknown task: {task.task_name}",
            )
            if self.task_registry is not None:
                self.task_registry.mark_result(task, result)
            return result

        if self.task_registry is not None:
            self.task_registry.mark_running(task, attempt=1)

        started_at = time.perf_counter()
        try:
            value = function(*task.args, **task.kwargs)
        except Exception as exc:  # noqa: BLE001
            result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                attempts=1,
                error=f"{type(exc).__name__}: {exc}",
                duration_seconds=time.perf_counter() - started_at,
            )
        else:
            result = TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                attempts=1,
                value=value,
                duration_seconds=time.perf_counter() - started_at,
            )

        if self.task_registry is not None:
            self.task_registry.mark_result(task, result)
        return result
