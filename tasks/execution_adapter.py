"""Bridge task execution results into the dashboard lifecycle contract."""

from concurrent.futures import Future, ThreadPoolExecutor
from threading import RLock

from execution.engine import ExecutionEngine
from execution.models import Task, TaskResult, TaskStatus
from tasks.lifecycle import TaskLifecycle, TaskState


class LifecycleExecutionAdapter:
    """Execute tasks while publishing their lifecycle to the shared tracker."""

    def __init__(
        self,
        engine: ExecutionEngine,
        lifecycle: TaskLifecycle,
        *,
        node_id: str = "local",
        max_workers: int = 4,
    ) -> None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if isinstance(max_workers, bool) or not isinstance(max_workers, int):
            raise TypeError("max_workers must be an integer")
        if max_workers < 1:
            raise ValueError("max_workers must be a positive integer")

        self.engine = engine
        self.lifecycle = lifecycle
        self.node_id = node_id
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="meshweaver-lifecycle",
        )
        self._lock = RLock()
        self._closed = False

    def _prepare(self, task: Task) -> None:
        """Register a task and mark it as assigned."""
        self.lifecycle.create(task.task_id, task.task_name)
        self.lifecycle.transition(
            task.task_id,
            TaskState.ASSIGNED,
            node=self.node_id,
        )

    def _run_prepared(self, task: Task) -> TaskResult:
        """Run an assigned task and publish its terminal lifecycle state."""
        if task.task_name not in self.engine.registered_tasks():
            result = self.engine.execute(task)
            self.lifecycle.transition(task.task_id, TaskState.FAILED)
            return result

        self.lifecycle.transition(
            task.task_id,
            TaskState.RUNNING,
            node=self.node_id,
        )

        try:
            result = self.engine.execute(task)
        except Exception:
            self.lifecycle.transition(task.task_id, TaskState.FAILED)
            raise

        record = self.lifecycle.get(task.task_id)
        if record is not None:
            # RUNNING increments attempts once. Adjust for retries reported by
            # the engine, without changing the public lifecycle API.
            additional_attempts = max(0, result.attempts - record.attempts)
            record.attempts += additional_attempts

        terminal_state = (
            TaskState.DONE
            if result.status == TaskStatus.COMPLETED
            else TaskState.FAILED
        )
        self.lifecycle.transition(task.task_id, terminal_state)
        return result

    def execute(self, task: Task) -> TaskResult:
        """Execute synchronously and update lifecycle state."""
        self._prepare(task)
        return self._run_prepared(task)

    def submit_task(self, task: Task) -> Future[TaskResult]:
        """Submit execution in the background and return its Future."""
        with self._lock:
            if self._closed:
                raise RuntimeError("LifecycleExecutionAdapter has been shut down.")

            self._prepare(task)
            return self._executor.submit(self._run_prepared, task)

    def shutdown(self, *, wait: bool = True) -> None:
        """Stop accepting submissions and shut down the worker pool."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

        self._executor.shutdown(wait=wait)
