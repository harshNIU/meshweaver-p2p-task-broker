"""Tests for the MeshWeaver execution engine."""

import time

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


def test_task_retries_after_failure_and_then_succeeds():
    calls = 0

    def flaky_task():
        nonlocal calls
        calls += 1

        if calls < 3:
            raise RuntimeError("temporary failure")

        return "success"

    engine = ExecutionEngine()
    engine.register("flaky", flaky_task)

    result = engine.execute(Task(task_name="flaky", max_retries=2))

    assert result.status == TaskStatus.COMPLETED
    assert result.value == "success"
    assert result.attempts == 3
    assert calls == 3


def test_task_fails_after_all_retries_are_exhausted():
    calls = 0

    def broken_task():
        nonlocal calls
        calls += 1
        raise RuntimeError("persistent failure")

    engine = ExecutionEngine()
    engine.register("broken", broken_task)

    result = engine.execute(Task(task_name="broken", max_retries=2))

    assert result.status == TaskStatus.FAILED
    assert result.attempts == 3
    assert calls == 3
    assert "persistent failure" in result.error


def test_task_with_no_retries_runs_only_once():
    calls = 0

    def broken_task():
        nonlocal calls
        calls += 1
        raise RuntimeError("failure")

    engine = ExecutionEngine()
    engine.register("broken", broken_task)

    result = engine.execute(Task(task_name="broken"))

    assert result.status == TaskStatus.FAILED
    assert result.attempts == 1
    assert calls == 1


def test_timed_out_task_returns_failed_result():
    engine = ExecutionEngine()
    engine.register("slow", lambda: time.sleep(0.3))

    result = engine.execute(
        Task(
            task_name="slow",
            timeout_seconds=0.02,
            task_id="timeout-1",
        )
    )

    assert result.task_id == "timeout-1"
    assert result.status == TaskStatus.FAILED
    assert result.attempts == 1
    assert "timed out" in result.error.lower()
    assert result.duration_seconds >= 0.02


def test_timed_out_task_is_not_retried():
    calls = 0

    def slow_task():
        nonlocal calls
        calls += 1
        time.sleep(0.2)

    engine = ExecutionEngine()
    engine.register("slow", slow_task)

    result = engine.execute(
        Task(
            task_name="slow",
            timeout_seconds=0.02,
            max_retries=3,
        )
    )

    assert result.status == TaskStatus.FAILED
    assert result.attempts == 1
