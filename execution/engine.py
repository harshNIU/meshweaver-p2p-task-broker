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
    """Execute registered tasks with retries, timeouts, and state tracking."""

    def __init__(self) -> None:
        self._registry: dict[str, Callable[..., Any]] = {}
        self._results: dict[str, TaskResult] = {}
        self._statuses: dict[str, TaskStatus] = {}
        self._state_lock = threading.RLock()

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

        with self._state_lock:
            if name in self._registry and not replace:
                raise TaskRegistrationError(f"Task '{name}' is already registered.")

            self._registry[name] = function

    def unregister(self, name: str) -> bool:
        """Remove a registered function and report whether it existed."""
        with self._state_lock:
            return self._registry.pop(name, None) is not None

    def registered_tasks(self) -> tuple[str, ...]:
        """Return registered task names in registration order."""
        with self._state_lock:
            return tuple(self._registry)

    def get_task_status(self, task_id: str) -> TaskStatus | None:
        """Return a task's latest known status, or None if unknown."""
        with self._state_lock:
            return self._statuses.get(task_id)

    def get_task_result(self, task_id: str) -> TaskResult | None:
        """Return a task's result if available, or None if unavailable."""
        with self._state_lock:
            return self._results.get(task_id)

    def _set_status(self, task_id: str, status: TaskStatus) -> None:
        """Update a task's status safely."""
        with self._state_lock:
            self._statuses[task_id] = status

    def _store_result(self, result: TaskResult) -> TaskResult:
        """Store a result and its final status."""
        with self._state_lock:
            self._results[result.task_id] = result
            self._statuses[result.task_id] = result.status

        return result

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
        """Execute a task, tracking its status and retrying failures."""
        with self._state_lock:
            function = self._registry.get(task.task_name)

            if task.task_id in self._statuses:
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    attempts=0,
                    error=f"Task ID already exists: {task.task_id}",
                )

            self._statuses[task.task_id] = TaskStatus.PENDING

        if function is None:
            return self._store_result(
                TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    attempts=0,
                    error=f"Unknown task: {task.task_name}",
                )
            )

        self._set_status(task.task_id, TaskStatus.RUNNING)

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

                    return self._store_result(
                        TaskResult(
                            task_id=task.task_id,
                            status=TaskStatus.COMPLETED,
                            attempts=attempts,
                            value=value,
                            duration_seconds=total_duration,
                        )
                    )

            else:
                succeeded, value, error, duration = self._execute_with_timeout(
                    function,
                    task,
                )
                total_duration += duration

                if succeeded:
                    return self._store_result(
                        TaskResult(
                            task_id=task.task_id,
                            status=TaskStatus.COMPLETED,
                            attempts=attempts,
                            value=value,
                            duration_seconds=total_duration,
                        )
                    )

                last_error = error

                # Timed-out functions may continue running. Retrying them could
                # duplicate side effects, so stop after a timeout.
                if last_error and "Task timed out after" in last_error:
                    break

            if attempt < task.max_retries:
                continue

        return self._store_result(
            TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                attempts=attempts,
                error=last_error,
                duration_seconds=total_duration,
            )
        )
