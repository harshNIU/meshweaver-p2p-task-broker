import pytest

from execution.models import Task, TaskResult, TaskStatus


def test_task_defaults_are_valid():
    task = Task(task_name="add")

    assert task.task_name == "add"
    assert task.task_id
    assert task.max_retries == 0
    assert task.timeout_seconds is None


def test_task_rejects_empty_name():
    with pytest.raises(ValueError):
        Task(task_name="")


def test_task_rejects_negative_retries():
    with pytest.raises(ValueError):
        Task(task_name="add", max_retries=-1)


def test_task_rejects_invalid_timeout():
    with pytest.raises(ValueError):
        Task(task_name="add", timeout_seconds=0)


def test_task_result_records_success():
    result = TaskResult(
        task_id="task-123",
        status=TaskStatus.COMPLETED,
        attempts=1,
        value=42,
    )

    assert result.status == TaskStatus.COMPLETED
    assert result.value == 42
    assert result.attempts == 1
