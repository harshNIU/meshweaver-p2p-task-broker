"""Tests for the MeshWeaver execution engine."""

import pytest

from execution.engine import ExecutionEngine, TaskRegistrationError
from execution.models import Task, TaskStatus


def test_registered_task_executes_successfully():
    engine = ExecutionEngine()
    engine.register("add", lambda a, b: a + b)

    result = engine.execute(Task(task_name="add", args=(2, 3), task_id="task-1"))

    assert result.task_id == "task-1"
    assert result.status == TaskStatus.COMPLETED
    assert result.value == 5
    assert result.attempts == 1
    assert result.error is None
    assert result.duration_seconds >= 0


def test_unknown_task_returns_failed_result():
    engine = ExecutionEngine()

    result = engine.execute(Task(task_name="missing", task_id="task-2"))

    assert result.status == TaskStatus.FAILED
    assert result.attempts == 0
    assert "Unknown task" in result.error


def test_function_exception_returns_failed_result():
    def broken_task():
        raise RuntimeError("deliberate test failure")

    engine = ExecutionEngine()
    engine.register("broken", broken_task)

    result = engine.execute(Task(task_name="broken"))

    assert result.status == TaskStatus.FAILED
    assert result.attempts == 1
    assert "RuntimeError" in result.error
    assert "deliberate test failure" in result.error


def test_duplicate_registration_is_rejected():
    engine = ExecutionEngine()
    engine.register("add", lambda a, b: a + b)

    with pytest.raises(TaskRegistrationError):
        engine.register("add", lambda a, b: a * b)


def test_registration_can_explicitly_replace_function():
    engine = ExecutionEngine()
    engine.register("calculate", lambda: 1)
    engine.register("calculate", lambda: 2, replace=True)

    result = engine.execute(Task(task_name="calculate"))

    assert result.status == TaskStatus.COMPLETED
    assert result.value == 2


def test_non_callable_registration_is_rejected():
    engine = ExecutionEngine()

    with pytest.raises(TaskRegistrationError):
        engine.register("invalid", 123)


def test_unregister_removes_registered_task():
    engine = ExecutionEngine()
    engine.register("add", lambda a, b: a + b)

    assert engine.unregister("add") is True
    assert engine.unregister("add") is False
    assert engine.registered_tasks() == ()
