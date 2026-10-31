"""Tests for execution-to-lifecycle integration."""

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from tasks.execution_adapter import LifecycleExecutionAdapter
from tasks.lifecycle import TaskLifecycle, TaskState


def test_successful_execution_reports_done_and_node():
    engine = ExecutionEngine()
    engine.register("add", lambda a, b: a + b)
    lifecycle = TaskLifecycle()
    adapter = LifecycleExecutionAdapter(
        engine,
        lifecycle,
        node_id="node-4",
    )

    result = adapter.execute(
        Task(task_name="add", args=(2, 3), task_id="adapter-success")
    )

    record = lifecycle.get("adapter-success")
    assert result.status == TaskStatus.COMPLETED
    assert result.value == 5
    assert record is not None
    assert record.state == TaskState.DONE
    assert record.node == "node-4"
    assert record.attempts == 1
    adapter.shutdown()


def test_failed_execution_reports_failed():
    def fail():
        raise RuntimeError("deliberate failure")

    engine = ExecutionEngine()
    engine.register("fail", fail)
    lifecycle = TaskLifecycle()
    adapter = LifecycleExecutionAdapter(engine, lifecycle)

    result = adapter.execute(Task(task_name="fail", task_id="adapter-failure"))

    record = lifecycle.get("adapter-failure")
    assert result.status == TaskStatus.FAILED
    assert record is not None
    assert record.state == TaskState.FAILED
    assert record.attempts == 1
    adapter.shutdown()


def test_unknown_task_reports_failed():
    engine = ExecutionEngine()
    lifecycle = TaskLifecycle()
    adapter = LifecycleExecutionAdapter(engine, lifecycle)

    result = adapter.execute(Task(task_name="missing", task_id="adapter-unknown"))

    record = lifecycle.get("adapter-unknown")
    assert result.status == TaskStatus.FAILED
    assert record is not None
    assert record.state == TaskState.FAILED
    assert record.attempts == 0
    adapter.shutdown()


def test_background_submission_reports_done():
    engine = ExecutionEngine()
    engine.register("greet", lambda: "hello")
    lifecycle = TaskLifecycle()
    adapter = LifecycleExecutionAdapter(engine, lifecycle)

    future = adapter.submit_task(Task(task_name="greet", task_id="adapter-background"))
    result = future.result(timeout=3)

    record = lifecycle.get("adapter-background")
    assert result.status == TaskStatus.COMPLETED
    assert record is not None
    assert record.state == TaskState.DONE

    adapter.shutdown()
