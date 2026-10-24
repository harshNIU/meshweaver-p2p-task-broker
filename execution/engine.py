"""Task registration and execution for MeshWeaver."""

import queue
import threading
import time
from collections.abc import Callable
from typing import Any

from execution.models import Task, TaskResult, TaskStatus


class TaskRegistrationError(ValueError):
    """Raised when a task function cannot be registered."""


class ExecutionEngine:
    """Execute registered tasks with retries and timeout reporting."""

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

    @staticmethod
    def _execute_with_timeout(
        function: Callable[..., Any],
        task: Task,
    ) -> tuple[bool, Any, str | None, float]:
        """Run a task in a daemon thread with a caller-side timeout.

        A timeout stops waiting for the result, but cannot terminate the
        underlying Python function. Timed-out functions may continue running.
        """
        result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            try:
                result_queue.put((True, function(*task.args, **task.kwargs)))
            except Exception as exc:  # noqa: BLE001
                result_queue.put((False, exc))

        started_at = time.perf_counter()
        worker = threading.Thread(target=invoke, daemon=True)
        worker.start()
        worker.join(task.timeout_seconds)

        duration = time.perf_counter() - started_at

        if worker.is_alive():
            return (
                False,
                None,
                f"Task timed out after {task.timeout_seconds} seconds.",
                duration,
            )

        succeeded, value = result_queue.get()

        if not succeeded:
            return (
                False,
                None,
                f"{type(value).__name__}: {value}",
                duration,
            )

        return True, value, None, duration

    def execute(self, task: Task) -> TaskResult:
        """Execute a task, retrying failures up to the configured limit."""
        function = self._registry.get(task.task_name)

        if function is None:
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                attempts=0,
                error=f"Unknown task: {task.task_name}",
            )

        total_duration = 0.0
        attempts = 0
        last_error = None

        for attempt in range(task.max_retries + 1):
            attempts = attempt + 1

            if task.timeout_seconds is None:
                started_at = time.perf_counter()

                try:
                    value = function(*task.args, **task.kwargs)
                except Exception as exc:  # noqa: BLE001
                    total_duration += time.perf_counter() - started_at
                    last_error = f"{type(exc).__name__}: {exc}"
                else:
                    total_duration += time.perf_counter() - started_at
                    return TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.COMPLETED,
                        attempts=attempts,
                        value=value,
                        duration_seconds=total_duration,
                    )
            else:
                succeeded, value, error, duration = self._execute_with_timeout(
                    function,
                    task,
                )
                total_duration += duration

                if succeeded:
                    return TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.COMPLETED,
                        attempts=attempts,
                        value=value,
                        duration_seconds=total_duration,
                    )

                last_error = error

                # A timed-out function may still be running. Do not automatically
                # repeat it, as doing so could duplicate side effects.
                if last_error and "Task timed out after" in last_error:
                    break

            if attempt < task.max_retries:
                continue

        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.FAILED,
            attempts=attempts,
            error=last_error,
            duration_seconds=total_duration,
        )
