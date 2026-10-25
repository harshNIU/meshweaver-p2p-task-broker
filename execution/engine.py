"""Task registration and execution for MeshWeaver."""

import inspect
import queue
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from execution.models import Task, TaskResult, TaskStatus


class TaskRegistrationError(ValueError):
    """Raised when a task function cannot be registered."""


class ExecutionEngine:
    """Execute tasks synchronously or asynchronously.

    Supports retries, timeouts, cooperative cancellation, state tracking,
    background submission, result retrieval, and executor shutdown.
    """

    def __init__(self, max_workers: int = 4) -> None:
        if (
            isinstance(max_workers, bool)
            or not isinstance(max_workers, int)
            or max_workers < 1
        ):
            raise ValueError("max_workers must be a positive integer.")

        self._registry: dict[str, Callable[..., Any]] = {}
        self._results: dict[str, TaskResult] = {}
        self._statuses: dict[str, TaskStatus] = {}
        self._cancel_events: dict[str, threading.Event] = {}
        self._futures: dict[str, Future[TaskResult]] = {}
        self._state_lock = threading.RLock()
        self._shutdown = False
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="meshweaver-task",
        )

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
        """Return a task's stored result, or None if not yet available."""
        with self._state_lock:
            return self._results.get(task_id)

    def cancel_task(self, task_id: str) -> bool:
        """Request cancellation of a running task.

        The task must cooperate by checking its injected ``cancel_event``.
        This method does not forcibly terminate arbitrary Python functions.
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
        """Store a result and its final status safely."""
        with self._state_lock:
            self._results[result.task_id] = result
            self._statuses[result.task_id] = result.status
            self._cancel_events.pop(result.task_id, None)
            self._futures.pop(result.task_id, None)
        return result

    @staticmethod
    def _accepts_cancel_event(function: Callable[..., Any]) -> bool:
        """Check whether a function explicitly accepts cancel_event."""
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
        """Invoke a task, injecting its cancellation event when supported."""
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
        """Run a task in a daemon thread with a caller-side timeout.

        A timeout stops waiting for the result, but cannot terminate the
        underlying Python function. That function may continue running.
        """
        result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            try:
                result_queue.put(
                    (True, ExecutionEngine._invoke(function, task, cancel_event))
                )
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

    def _reserve_task(self, task: Task) -> threading.Event:
        """Reserve a task ID before execution or background scheduling."""
        with self._state_lock:
            if self._shutdown:
                raise RuntimeError("ExecutionEngine has been shut down.")

            if task.task_id in self._statuses:
                raise ValueError(f"Task ID already exists: {task.task_id}")

            cancel_event = threading.Event()
            self._cancel_events[task.task_id] = cancel_event
            self._statuses[task.task_id] = TaskStatus.PENDING
            return cancel_event

    def execute(self, task: Task) -> TaskResult:
        """Execute a task synchronously and return its result."""
        self._reserve_task(task)
        return self._execute_reserved(task)

    def submit_task(self, task: Task) -> Future[TaskResult]:
        """Submit a task in the background and return its Future.

        The task ID is reserved before scheduling, preventing simultaneous
        submissions from executing the same task ID.
        """
        self._reserve_task(task)

        try:
            with self._state_lock:
                future = self._executor.submit(self._execute_reserved, task)
                self._futures[task.task_id] = future
                future.add_done_callback(
                    lambda completed, task_id=task.task_id: self._handle_future_done(
                        task_id, completed
                    )
                )
                return future
        except Exception:
            with self._state_lock:
                # Roll back only if execution has not already started.
                if self._statuses.get(task.task_id) == TaskStatus.PENDING:
                    self._statuses.pop(task.task_id, None)
                    self._cancel_events.pop(task.task_id, None)
                    self._futures.pop(task.task_id, None)
            raise

    def _handle_future_done(
        self,
        task_id: str,
        future: Future[TaskResult],
    ) -> None:
        """Record a result when a background Future is cancelled pre-start."""
        if not future.cancelled():
            return

        with self._state_lock:
            if task_id in self._results:
                return

        self._store_result(
            TaskResult(
                task_id=task_id,
                status=TaskStatus.CANCELLED,
                attempts=0,
                error="Task cancelled before execution.",
            )
        )

    def _execute_reserved(self, task: Task) -> TaskResult:
        """Execute a task whose ID has already been reserved."""
        with self._state_lock:
            function = self._registry.get(task.task_name)
            cancel_event = self._cancel_events.get(task.task_id)

            if cancel_event is None:
                # This can happen only if a pre-start cancellation completed.
                existing_result = self._results.get(task.task_id)
                if existing_result is not None:
                    return existing_result

                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.CANCELLED,
                    attempts=0,
                    error="Task cancelled before execution.",
                )

            if cancel_event.is_set():
                return self._store_result(
                    TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.CANCELLED,
                        attempts=0,
                        error="Task cancelled before execution.",
                    )
                )

            if function is None:
                return self._store_result(
                    TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.FAILED,
                        attempts=0,
                        error=f"Unknown task: {task.task_name}",
                    )
                )

            self._statuses[task.task_id] = TaskStatus.RUNNING

        total_duration = 0.0
        attempts = 0
        last_error = None

        for attempt in range(task.max_retries + 1):
            if cancel_event.is_set():
                return self._store_result(
                    TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.CANCELLED,
                        attempts=attempts,
                        error="Task cancellation requested.",
                        duration_seconds=total_duration,
                    )
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
                        return self._store_result(
                            TaskResult(
                                task_id=task.task_id,
                                status=TaskStatus.CANCELLED,
                                attempts=attempts,
                                error="Task cancellation requested.",
                                duration_seconds=total_duration,
                            )
                        )

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
                    function, task, cancel_event
                )
                total_duration += duration

                if cancel_event.is_set():
                    return self._store_result(
                        TaskResult(
                            task_id=task.task_id,
                            status=TaskStatus.CANCELLED,
                            attempts=attempts,
                            error="Task cancellation requested.",
                            duration_seconds=total_duration,
                        )
                    )

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

                # Timed-out functions may continue running. Retrying could
                # duplicate side effects, so stop after a timeout.
                if last_error and "Task timed out after" in last_error:
                    break

            if cancel_event.is_set():
                return self._store_result(
                    TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.CANCELLED,
                        attempts=attempts,
                        error="Task cancellation requested.",
                        duration_seconds=total_duration,
                    )
                )

            if attempt < task.max_retries:
                continue

        if cancel_event.is_set():
            return self._store_result(
                TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.CANCELLED,
                    attempts=attempts,
                    error="Task cancellation requested.",
                    duration_seconds=total_duration,
                )
            )

        return self._store_result(
            TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                attempts=attempts,
                error=last_error,
                duration_seconds=total_duration,
            )
        )

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shut down background execution.

        If cancel_futures is True, tasks still waiting in the executor queue
        are cancelled. Running tasks are not forcibly terminated.
        """
        with self._state_lock:
            if self._shutdown:
                return
            self._shutdown = True

        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
