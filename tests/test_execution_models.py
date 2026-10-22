"""Tests for MeshWeaver task execution models."""

import pytest

from execution.models import Task, TaskResult, TaskStatus


def test_task_defaults_are_valid():
    task = Task(task_name="example")

    assert task.task_name == "example"
    assert task.args == ()
    assert task.kwargs == {}
    assert task.task_id
    assert task.max_retries == 0
    assert task.timeout_seconds is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("task_name", ""),
        ("task_name", "   "),
        ("task_name", None),
        ("task_id", ""),
        ("task_id", "   "),
        ("task_id", None),
    ],
)
def test_task_rejects_invalid_names_and_ids(field, value):
    with pytest.raises(ValueError):
        Task(
            **(
                {field: value, "task_name": "example"}
                if field == "task_id"
                else {field: value}
            )
        )


def test_task_rejects_negative_retries():
    with pytest.raises(ValueError, match="max_retries"):
        Task(task_name="example", max_retries=-1)


@pytest.mark.parametrize("value", [1.5, "2", True, None])
def test_task_rejects_invalid_retry_types(value):
    with pytest.raises(ValueError, match="max_retries"):
        Task(task_name="example", max_retries=value)


def test_task_rejects_invalid_timeout():
    with pytest.raises(ValueError, match="timeout_seconds"):
        Task(task_name="example", timeout_seconds=0)


@pytest.mark.parametrize("value", [-1, float("inf"), float("nan"), True, "2"])
def test_task_rejects_invalid_timeout_values(value):
    with pytest.raises(ValueError, match="timeout_seconds"):
        Task(task_name="example", timeout_seconds=value)


def test_task_rejects_invalid_argument_containers():
    with pytest.raises(TypeError, match="args"):
        Task(task_name="example", args=[1, 2])

    with pytest.raises(TypeError, match="kwargs"):
        Task(task_name="example", kwargs=[("value", 1)])


def test_task_rejects_non_string_keyword_names():
    with pytest.raises(TypeError, match="kwargs keys"):
        Task(task_name="example", kwargs={1: "value"})


def test_task_result_records_success():
    result = TaskResult(
        task_id="task-1",
        status=TaskStatus.COMPLETED,
        attempts=1,
        value=42,
    )

    assert result.task_id == "task-1"
    assert result.status == TaskStatus.COMPLETED
    assert result.attempts == 1
    assert result.value == 42
    assert result.error is None
