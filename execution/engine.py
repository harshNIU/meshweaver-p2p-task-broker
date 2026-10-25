"""Task registration and execution for MeshWeaver."""

import inspect
import queue
import threading
import time
from collections.abc import Callable
from typing import Any

from execution.models import Task, TaskResult, TaskStatus
from execution.task_registry import TaskRegistry


class TaskRegistrationError(ValueError):
    """Raised when a task function cannot be registered."""


class ExecutionEngine:
    """Execute registered tasks with retries, timeouts, cancellation, and tracking."""

    def __init__(self, task_registry: TaskRegistry | None = None) -> None:
        self._registry: dict[str, Callable[..., Any]] = {}
        self.task_registry = task_registry
        self._results: dict[str, TaskResult] = {}
        self._statuses: dict[str, TaskStatus] = {}
        self._cancel_events: dict[str, threading.Event] = {}
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

    def cancel_task(self, task_id: str) -> bool:
        """Request cancellation of a running task.

        The task must cooperate by checking its injected cancel_event.
        """
        with self._state_lock:
            if self._statuses.get(task_id) != TaskStatus.RUNNING:
                return False

            cancel_event = self._cancel_events.get(task_id)
            if cancel_event is None or cancel_event.is_set():
                return False

            cancel_event.set()
            return True

    def _store_result(self, result: TaskResult) -> TaskResult:
        """Store the result and its terminal status."""
        with self._state_lock:
            self._results[result.task_id] = result
            self._statuses[result.task_id] = result.status
            self._cancel_events.pop(result.task_id, None)

        return result

    @staticmethod
    def _accepts_cancel_event(function: Callable[..., Any]) -> bool:
        """Check whether a callable explicitly accepts cancel_event."""
        try:
            parameters = inspect.signature(function).parameters
        except (TypeError, ValueError):
            return False

        parameter = parameters.get("cancel_event")
        return parameter is not None and parameter.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )

    @classmethod
    def _invoke(
        cls,
        function: Callable[..., Any],
        task: Task,
        cancel_event: threading.Event,
    ) -> Any:
        """Invoke a task, injecting a cancellation event when supported."""
        kwargs = dict(task.kwargs)

        if cls._accepts_cancel_event(function):
            if "cancel_event" in kwargs:
                raise ValueError(
                    "Do not provide cancel_event in task kwargs; "
                    "the engine supplies it automatically."
                )
            kwargs["cancel_event"] = cancel_event

        return function(*task.args, **kwargs)

    @staticmethod
    def _execute_with_timeout(
        function: Callable[..., Any],
        task: Task,
        cancel_event: threading.Event,
    ) -> tuple[bool, Any, str | None, float]:
        """Run a task in a daemon thread and wait up to its timeout.

        Timing out stops waiting for the result but cannot forcibly terminate
        the underlying Python function.
        """
        result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            try:
                value = ExecutionEngine._invoke(function, task, cancel_event)
                result_queue.put((True, value))
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
            return False, None, f"{type(value).__name__}: {value}", duration

        return True, value, None, duration

    def execute(self, task: Task) -> TaskResult:
        """Execute a task with retries, timeout handling, and lifecycle tracking."""
        with self._state_lock:
            function = self._registry.get(task.task_name)

            if task.task_id in self._statuses:
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    attempts=0,
                    error=f"Task ID already exists: {task.task_id}",
                )

            cancel_event = threading.Event()
            self._cancel_events[task.task_id] = cancel_event
            self._statuses[task.task_id] = TaskStatus.PENDING

        if self.task_registry is not None:
            self.task_registry.mark_pending(task)

        if function is None:
            return self._store_result(
                self._record_result(
                    task,
                    TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.FAILED,
                        attempts=0,
                        error=f"Unknown task: {task.task_name}",
                    ),
                )
            )

        with self._state_lock:
            if cancel_event.is_set():
                return self._finish(
                    task,
                    TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.CANCELLED,
                        attempts=0,
                        error="Task cancelled before execution.",
                    ),
                )

            self._statuses[task.task_id] = TaskStatus.RUNNING

        if self.task_registry is not None:
            self.task_registry.mark_running(task, attempt=1)

        total_duration = 0.0
        attempts = 0
        last_error = None

        for attempt in range(task.max_retries + 1):
            if cancel_event.is_set():
                return self._finish(
                    task,
                    TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.CANCELLED,
                        attempts=attempts,
                        error="Task cancellation requested.",
                        duration_seconds=total_duration,
                    ),
                )

            attempts = attempt + 1

            if task.timeout_seconds is None:
                started_at = time.perf_counter()
                try:
                    value = self._invoke(function, task, cancel_event)
                except Exception as exc:  # noqa: BLE001
                    total_duration += time.perf_counter() - started_at
                    last_error = f"{type(exc).__name__}: {exc}"
                else:
                    total_duration += time.perf_counter() - started_at

                    if cancel_event.is_set():
                        return self._finish(
                            task,
                            TaskResult(
                                task_id=task.task_id,
                                status=TaskStatus.CANCELLED,
                                attempts=attempts,
                                error="Task cancellation requested.",
                                duration_seconds=total_duration,
                            ),
                        )

                    return self._finish(
                        task,
                        TaskResult(
                            task_id=task.task_id,
                            status=TaskStatus.COMPLETED,
                            attempts=attempts,
                            value=value,
                            duration_seconds=total_duration,
                        ),
                    )
            else:
                succeeded, value, error, duration = self._execute_with_timeout(
                    function,
                    task,
                    cancel_event,
                )
                total_duration += duration

                if cancel_event.is_set():
                    return self._finish(
                        task,
                        TaskResult(
                            task_id=task.task_id,
                            status=TaskStatus.CANCELLED,
                            attempts=attempts,
                            error="Task cancellation requested.",
                            duration_seconds=total_duration,
                        ),
                    )

                if succeeded:
                    return self._finish(
                        task,
                        TaskResult(
                            task_id=task.task_id,
                            status=TaskStatus.COMPLETED,
                            attempts=attempts,
                            value=value,
                            duration_seconds=total_duration,
                        ),
                    )

                last_error = error

                # Timed-out functions may continue running. Retrying could
                # duplicate side effects, so stop after a timeout.
                if last_error and "Task timed out after" in last_error:
                    break

            if cancel_event.is_set():
                return self._finish(
                    task,
                    TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.CANCELLED,
                        attempts=attempts,
                        error="Task cancellation requested.",
                        duration_seconds=total_duration,
                    ),
                )

        status = TaskStatus.CANCELLED if cancel_event.is_set() else TaskStatus.FAILED
        error = (
            "Task cancellation requested."
            if status == TaskStatus.CANCELLED
            else last_error
        )

        return self._finish(
            task,
            TaskResult(
                task_id=task.task_id,
                status=status,
                attempts=attempts,
                error=error,
                duration_seconds=total_duration,
            ),
        )

    def _record_result(self, task: Task, result: TaskResult) -> TaskResult:
        """Update the dashboard registry with a task result."""
        if self.task_registry is not None:
            self.task_registry.mark_result(task, result)
        return result

    def _finish(self, task: Task, result: TaskResult) -> TaskResult:
        """Store the result and update the dashboard registry."""
        self._record_result(task, result)
        return self._store_result(result)
