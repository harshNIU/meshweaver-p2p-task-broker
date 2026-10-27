import pytest

from tasks.lifecycle import TaskLifecycle, TaskState


def test_task_follows_successful_lifecycle():
    lifecycle = TaskLifecycle()
    task = lifecycle.create("task-1", "matrix_mult")

    assert task.state == TaskState.PENDING
    lifecycle.transition("task-1", TaskState.ASSIGNED, node="node-a")
    lifecycle.transition("task-1", TaskState.RUNNING)

    result = lifecycle.transition("task-1", TaskState.DONE)

    assert result.state == TaskState.DONE
    assert result.node == "node-a"
    assert result.attempts == 1


def test_task_can_fail():
    lifecycle = TaskLifecycle()
    lifecycle.create("task-2", "monte_carlo_pi")
    lifecycle.transition("task-2", TaskState.FAILED)

    assert lifecycle.get("task-2").state == TaskState.FAILED


def test_invalid_transition_is_rejected():
    lifecycle = TaskLifecycle()
    lifecycle.create("task-3", "sample")

    with pytest.raises(ValueError, match="Invalid task transition"):
        lifecycle.transition("task-3", TaskState.DONE)


def test_duplicate_task_id_is_rejected():
    lifecycle = TaskLifecycle()
    lifecycle.create("task-4", "sample")

    with pytest.raises(ValueError, match="already exists"):
        lifecycle.create("task-4", "another")


def test_unknown_task_is_rejected():
    lifecycle = TaskLifecycle()

    with pytest.raises(KeyError, match="Unknown task"):
        lifecycle.transition("missing", TaskState.FAILED)


def test_snapshot_matches_dashboard_contract():
    lifecycle = TaskLifecycle()
    lifecycle.create("task-5", "matrix_mult")
    lifecycle.transition("task-5", TaskState.ASSIGNED, node="node-b")
    lifecycle.transition("task-5", TaskState.RUNNING)

    item = lifecycle.snapshot()[0]

    assert set(item) == {"task_id", "name", "state", "node", "attempts"}
    assert item == {
        "task_id": "task-5",
        "name": "matrix_mult",
        "state": "RUNNING",
        "node": "node-b",
        "attempts": 1,
    }
