"""Tests for task failure handling when a node dies."""

import pytest

from tasks.failover import mark_running_tasks_failed
from tasks.lifecycle import TaskLifecycle, TaskState


def make_running_task(
    lifecycle: TaskLifecycle,
    task_id: str,
    node_id: str,
) -> None:
    """Create a task and move it to RUNNING on a node."""
    lifecycle.create(task_id, "demo_task")
    lifecycle.transition(task_id, TaskState.ASSIGNED, node=node_id)
    lifecycle.transition(task_id, TaskState.RUNNING, node=node_id)


def test_dead_node_tasks_are_marked_failed():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-1", "node-dead")
    make_running_task(lifecycle, "task-2", "node-dead")

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == ["task-1", "task-2"]
    assert lifecycle.get("task-1").state == TaskState.FAILED
    assert lifecycle.get("task-2").state == TaskState.FAILED


def test_tasks_on_other_nodes_remain_running():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-dead", "node-dead")
    make_running_task(lifecycle, "task-alive", "node-alive")

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == ["task-dead"]
    assert lifecycle.get("task-alive").state == TaskState.RUNNING


def test_assigned_tasks_on_dead_node_are_not_changed():
    lifecycle = TaskLifecycle()
    lifecycle.create("task-assigned", "demo_task")
    lifecycle.transition(
        "task-assigned",
        TaskState.ASSIGNED,
        node="node-dead",
    )

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == []
    assert lifecycle.get("task-assigned").state == TaskState.ASSIGNED


def test_completed_tasks_on_dead_node_remain_done():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-done", "node-dead")
    lifecycle.transition("task-done", TaskState.DONE)

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == []
    assert lifecycle.get("task-done").state == TaskState.DONE


def test_repeated_dead_node_notification_is_safe():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-1", "node-dead")

    first = mark_running_tasks_failed(lifecycle, "node-dead")
    second = mark_running_tasks_failed(lifecycle, "node-dead")

    assert first == ["task-1"]
    assert second == []
    assert lifecycle.get("task-1").state == TaskState.FAILED


def test_unknown_node_returns_no_failed_tasks():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-1", "node-alive")

    failed = mark_running_tasks_failed(lifecycle, "unknown-node")

    assert failed == []
    assert lifecycle.get("task-1").state == TaskState.RUNNING


@pytest.mark.parametrize("node_id", ["", "   "])
def test_empty_dead_node_id_is_rejected(node_id):
    lifecycle = TaskLifecycle()

    with pytest.raises(ValueError, match="dead_node_id"):
        mark_running_tasks_failed(lifecycle, node_id)


def test_dead_node_recovery_candidates_preserve_task_identity():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "recover-1", "node-dead")
    make_running_task(lifecycle, "keep-1", "node-alive")

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == ["recover-1"]
    assert lifecycle.get("recover-1").task_id == "recover-1"
    assert lifecycle.get("recover-1").state == TaskState.FAILED
    assert lifecycle.get("keep-1").state == TaskState.RUNNING
