"""Tests for asynchronous task execution."""

import threading

import pytest

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus


@pytest.fixture
def engine():
    instance = ExecutionEngine(max_workers=2)
    yield instance
    instance.shutdown()


def test_submit_task_returns_future_and_result(engine):
    engine.register("add", lambda a, b: a + b)
    task = Task(task_name="add", args=(2, 3))

    future = engine.submit_task(task)
    result = future.result(timeout=3)

    assert result.status == TaskStatus.COMPLETED
    assert result.value == 5
    assert engine.get_task_status(task.task_id) == TaskStatus.COMPLETED
    assert engine.get_task_result(task.task_id) == result


def test_submit_task_runs_in_background(engine):
    started = threading.Event()
    release = threading.Event()

    def slow_task():
        started.set()
        release.wait(timeout=3)
        return "finished"

    engine.register("slow", slow_task)
    task = Task(task_name="slow")

    future = engine.submit_task(task)

    try:
        assert started.wait(timeout=2)
        assert not future.done()
    finally:
        release.set()

    assert future.result(timeout=3).value == "finished"


def test_submit_task_rejects_duplicate_task_id(engine):
    engine.register("work", lambda: "done")
    task = Task(task_name="work", task_id="duplicate-id")

    future = engine.submit_task(task)
    assert future.result(timeout=3).status == TaskStatus.COMPLETED

    with pytest.raises(ValueError, match="Task ID already exists"):
        engine.submit_task(task)


def test_submit_task_records_failure(engine):
    def failing_task():
        raise RuntimeError("intentional test failure")

    engine.register("fail", failing_task)
    task = Task(task_name="fail")

    result = engine.submit_task(task).result(timeout=3)

    assert result.status == TaskStatus.FAILED
    assert "intentional test failure" in result.error
    assert engine.get_task_status(task.task_id) == TaskStatus.FAILED


def test_submit_unknown_task_returns_failed_result(engine):
    task = Task(task_name="not-registered")

    result = engine.submit_task(task).result(timeout=3)

    assert result.status == TaskStatus.FAILED
    assert "Unknown task" in result.error


def test_shutdown_rejects_new_submissions():
    engine = ExecutionEngine()
    engine.shutdown()

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit_task(Task(task_name="work"))


def test_shutdown_can_be_called_twice():
    engine = ExecutionEngine()
    engine.shutdown()
    engine.shutdown()


def test_submit_multiple_tasks(engine):
    engine.register("square", lambda value: value * value)
    tasks = [Task(task_name="square", args=(n,)) for n in range(5)]

    futures = [engine.submit_task(task) for task in tasks]
    results = [future.result(timeout=3) for future in futures]

    assert [result.value for result in results] == [0, 1, 4, 9, 16]
    assert all(result.status == TaskStatus.COMPLETED for result in results)


def test_submit_task_status_is_available(engine):
    started = threading.Event()
    release = threading.Event()

    def waiting_task():
        started.set()
        release.wait(timeout=3)
        return "done"

    engine.register("waiting", waiting_task)
    task = Task(task_name="waiting")

    future = engine.submit_task(task)

    try:
        assert started.wait(timeout=2)
        assert engine.get_task_status(task.task_id) == TaskStatus.RUNNING
        assert engine.get_task_result(task.task_id) is None
    finally:
        release.set()

    assert future.result(timeout=3).status == TaskStatus.COMPLETED


def test_async_submission_does_not_change_sync_execution(engine):
    engine.register("multiply", lambda a, b: a * b)

    sync_task = Task(task_name="multiply", args=(3, 4))
    async_task = Task(task_name="multiply", args=(5, 6))

    sync_result = engine.execute(sync_task)
    async_result = engine.submit_task(async_task).result(timeout=3)

    assert sync_result.value == 12
    assert async_result.value == 30
