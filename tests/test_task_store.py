import pytest

from tasks.lifecycle import TaskState
from tasks.store import SQLiteTaskStore


def test_store_creates_and_retrieves_task(tmp_path):
    db_path = tmp_path / "tasks.db"
    store = SQLiteTaskStore(db_path)

    store.save(
        task_id="task-1",
        name="calculate",
        state=TaskState.PENDING,
        node="-",
        attempts=0,
    )

    record = store.get("task-1")

    assert record == {
        "task_id": "task-1",
        "name": "calculate",
        "state": "PENDING",
        "node": "-",
        "attempts": 0,
    }


def test_store_updates_existing_task(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    store.save("task-1", "calculate", TaskState.PENDING, "-", 0)
    store.save("task-1", "calculate", TaskState.RUNNING, "node-a", 1)

    record = store.get("task-1")

    assert record["state"] == "RUNNING"
    assert record["node"] == "node-a"
    assert record["attempts"] == 1


def test_store_lists_tasks(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    store.save("task-1", "calculate", TaskState.PENDING, "-", 0)
    store.save("task-2", "transform", TaskState.RUNNING, "node-a", 1)

    records = store.list_tasks()

    assert [record["task_id"] for record in records] == [
        "task-1",
        "task-2",
    ]


def test_store_persists_tasks_after_reopen(tmp_path):
    db_path = tmp_path / "tasks.db"

    first_store = SQLiteTaskStore(db_path)
    first_store.save(
        "task-1",
        "calculate",
        TaskState.RUNNING,
        "node-a",
        1,
    )

    reopened_store = SQLiteTaskStore(db_path)

    assert reopened_store.get("task-1") == {
        "task_id": "task-1",
        "name": "calculate",
        "state": "RUNNING",
        "node": "node-a",
        "attempts": 1,
    }


def test_store_rejects_invalid_attempt_count(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    with pytest.raises(ValueError, match="attempts"):
        store.save("task-1", "calculate", TaskState.PENDING, "-", -1)


def test_store_rejects_invalid_state(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    with pytest.raises(TypeError, match="TaskState"):
        store.save("task-1", "calculate", "RUNNING", "node-a", 1)


def test_store_persists_and_restores_complete_task(tmp_path):
    from execution.models import Task

    store = SQLiteTaskStore(tmp_path / "tasks.db")
    original = Task(
        task_name="calculate",
        args=(10, 20),
        kwargs={"operation": "add"},
        task_id="task-payload-1",
        max_retries=3,
        timeout_seconds=12.5,
    )

    store.save_task(original)
    restored = store.get_task(original.task_id)

    assert restored == original


def test_store_returns_none_for_missing_task_payload(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    assert store.get_task("missing-task") is None


def test_store_lists_restored_task_payloads(tmp_path):
    from execution.models import Task

    store = SQLiteTaskStore(tmp_path / "tasks.db")
    first = Task("calculate", args=(2, 3), task_id="payload-a")
    second = Task("transform", args=("hello",), task_id="payload-b")

    store.save_task(first)
    store.save_task(second)

    restored = store.list_task_payloads()

    assert {task.task_id for task in restored} == {
        "payload-a",
        "payload-b",
    }


from unittest.mock import AsyncMock

from execution.models import Task
from tasks.lifecycle import TaskLifecycle
from tasks.recovery import TaskRecoveryCoordinator


def test_registration_transition_failure_rolls_back_sqlite_task(tmp_path):
    store = SQLiteTaskStore(tmp_path / "rollback.db")
    lifecycle = TaskLifecycle(store=store)
    original_transition = lifecycle.transition

    def fail_running_transition(task_id, new_state, *, node=None):
        if new_state == TaskState.RUNNING:
            raise OSError("simulated transition failure")
        return original_transition(task_id, new_state, node=node)

    lifecycle.transition = fail_running_transition

    coordinator = TaskRecoveryCoordinator(
        lifecycle,
        snapshot=list,
        submit=AsyncMock(),
        store=store,
    )
    task = Task("calculate", args=(2, 3), task_id="rollback-task")

    with pytest.raises(OSError, match="simulated transition failure"):
        coordinator.register(task, "node-a")

    assert lifecycle.get(task.task_id) is None
    assert store.get(task.task_id) is None
    assert store.get_task(task.task_id) is None
