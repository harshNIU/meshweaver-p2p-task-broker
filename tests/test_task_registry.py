"""Tests for real task lifecycle tracking."""
from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from execution.task_registry import TaskRegistry


def test_successful_execution_is_recorded_in_registry():
    registry = TaskRegistry()
    engine = ExecutionEngine(task_registry=registry)
    engine.register("add", lambda a, b: a + b)

    result = engine.execute(Task(task_name="add", args=(2, 3), task_id="actual-1"))

    assert result.status == TaskStatus.COMPLETED
    records = registry.snapshot_tasks()
    assert len(records) == 1
    assert records[0]["task_id"] == "actual-1"
    assert records[0]["name"] == "add"
    assert records[0]["status"] == "COMPLETED"
    assert records[0]["attempt"] == 1
    assert any("started" in event["message"] for event in registry.snapshot_events())


def test_failed_execution_is_recorded_in_registry():
    registry = TaskRegistry()
    engine = ExecutionEngine(task_registry=registry)

    result = engine.execute(Task(task_name="missing", task_id="actual-2"))

    assert result.status == TaskStatus.FAILED
    record = registry.snapshot_tasks()[0]
    assert record["task_id"] == "actual-2"
    assert record["status"] == "FAILED"
    assert "Unknown task" in record["error"]
