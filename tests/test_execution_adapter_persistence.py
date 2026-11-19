"""Tests for persistent execution adapter lifecycle updates."""

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from tasks.execution_adapter import LifecycleExecutionAdapter
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.store import SQLiteTaskStore


def test_retry_attempts_are_persisted_in_sqlite(tmp_path):
    attempts = 0

    def fail_twice_then_succeed():
        nonlocal attempts
        attempts += 1

        if attempts < 3:
            raise RuntimeError("temporary failure")

        return "success"

    store = SQLiteTaskStore(tmp_path / "tasks.db")
    lifecycle = TaskLifecycle(store)
    engine = ExecutionEngine()
    engine.register("retry_task", fail_twice_then_succeed)

    adapter = LifecycleExecutionAdapter(engine, lifecycle)

    try:
        task = Task(
            task_name="retry_task",
            task_id="sqlite-retry-persistence",
            max_retries=2,
        )

        result = adapter.execute(task)

        record = lifecycle.get(task.task_id)
        persisted = store.get(task.task_id)

        assert result.status == TaskStatus.COMPLETED
        assert result.attempts == 3
        assert result.value == "success"

        assert record is not None
        assert record.state == TaskState.DONE
        assert record.attempts == 3

        assert persisted is not None
        assert persisted["state"] == TaskState.DONE.value
        assert persisted["attempts"] == 3
    finally:
        adapter.shutdown()
        engine.shutdown()