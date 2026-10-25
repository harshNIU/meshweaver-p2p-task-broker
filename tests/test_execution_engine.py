"""Tests for the MeshWeaver execution engine."""

import threading
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


def test_successful_task_status_and_result_are_tracked():
    engine = ExecutionEngine()
    engine.register("add", lambda a, b: a + b)

    result = engine.execute(
        Task(task_name="add", args=(2, 3), task_id="tracked-success")
    )

    assert result.status == TaskStatus.COMPLETED
    assert engine.get_task_status("tracked-success") == TaskStatus.COMPLETED
    assert engine.get_task_result("tracked-success") == result


def test_failed_task_status_and_result_are_tracked():
    def fail():
        raise RuntimeError("expected failure")

    engine = ExecutionEngine()
    engine.register("fail", fail)

    result = engine.execute(Task(task_name="fail", task_id="tracked-failure"))

    assert result.status == TaskStatus.FAILED
    assert engine.get_task_status("tracked-failure") == TaskStatus.FAILED
    assert engine.get_task_result("tracked-failure") == result


def test_unknown_task_status_and_result_are_tracked():
    engine = ExecutionEngine()

    result = engine.execute(Task(task_name="missing", task_id="tracked-unknown"))

    assert result.status == TaskStatus.FAILED
    assert engine.get_task_status("tracked-unknown") == TaskStatus.FAILED
    assert engine.get_task_result("tracked-unknown") == result


def test_unknown_task_id_has_no_status_or_result():
    engine = ExecutionEngine()

    assert engine.get_task_status("does-not-exist") is None
    assert engine.get_task_result("does-not-exist") is None


def test_running_status_can_be_observed_during_execution():
    started = threading.Event()
    release = threading.Event()

    def wait_for_release():
        started.set()
        release.wait(timeout=2)
        return "finished"

    engine = ExecutionEngine()
    engine.register("wait", wait_for_release)
    task = Task(task_name="wait", task_id="tracked-running")

    worker = threading.Thread(target=engine.execute, args=(task,))
    worker.start()

    try:
        assert started.wait(timeout=1)
        assert engine.get_task_status(task.task_id) == TaskStatus.RUNNING
        assert engine.get_task_result(task.task_id) is None
    finally:
        release.set()
        worker.join(timeout=2)

    assert not worker.is_alive()
    assert engine.get_task_status(task.task_id) == TaskStatus.COMPLETED


def test_running_task_can_be_cancelled_cooperatively():
    started = threading.Event()
    task_id = "cancel-cooperative"

    def cancellable_task(cancel_event):
        started.set()
        cancel_event.wait(timeout=2)
        return "stopped"

    engine = ExecutionEngine()
    engine.register("cancellable", cancellable_task)
    task = Task(task_name="cancellable", task_id=task_id)
    results = []

    worker = threading.Thread(target=lambda: results.append(engine.execute(task)))
    worker.start()

    try:
        assert started.wait(timeout=1)
        assert engine.get_task_status(task_id) == TaskStatus.RUNNING
        assert engine.cancel_task(task_id) is True
        assert engine.cancel_task(task_id) is False
    finally:
        worker.join(timeout=2)

    assert not worker.is_alive()
    assert results[0].status == TaskStatus.CANCELLED
    assert engine.get_task_status(task_id) == TaskStatus.CANCELLED
    assert engine.get_task_result(task_id) == results[0]


def test_cancel_unknown_task_returns_false():
    engine = ExecutionEngine()

    assert engine.cancel_task("unknown-task") is False


def test_completed_task_cannot_be_cancelled():
    engine = ExecutionEngine()
    engine.register("quick", lambda: "done")
    task = Task(task_name="quick", task_id="already-completed")

    result = engine.execute(task)

    assert result.status == TaskStatus.COMPLETED
    assert engine.cancel_task(task.task_id) is False


def test_task_without_cancel_event_parameter_still_works():
    engine = ExecutionEngine()
    engine.register("add", lambda a, b: a + b)

    result = engine.execute(Task(task_name="add", args=(4, 6), task_id="ordinary-task"))

    assert result.status == TaskStatus.COMPLETED
    assert result.value == 10


def test_cancelled_task_is_not_retried():
    calls = 0
    started = threading.Event()

    def cancellable_task(cancel_event):
        nonlocal calls
        calls += 1
        started.set()
        cancel_event.wait(timeout=2)
        raise RuntimeError("cancelled work stopped")

    engine = ExecutionEngine()
    engine.register("cancellable", cancellable_task)
    task = Task(
        task_name="cancellable",
        task_id="cancel-no-retry",
        max_retries=3,
    )
    results = []

    worker = threading.Thread(target=lambda: results.append(engine.execute(task)))
    worker.start()

    try:
        assert started.wait(timeout=1)
        assert engine.cancel_task(task.task_id) is True
    finally:
        worker.join(timeout=2)

    assert not worker.is_alive()
    assert calls == 1
    assert results[0].status == TaskStatus.CANCELLED
    assert results[0].attempts == 1
