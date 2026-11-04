import sqlite3

import pytest

from execution.models import Task
from tasks.lifecycle import TaskState
from tasks.store import SQLiteTaskStore


def make_task(
    task_id="task-1",
    task_name="add",
    args=(10, 20),
    kwargs=None,
):
    return Task(
        task_name=task_name,
        args=args,
        kwargs=kwargs or {},
        task_id=task_id,
        max_retries=2,
        timeout_seconds=10.0,
    )


def test_store_creates_and_retrieves_task(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    store.save("task-1", "add", TaskState.PENDING, "-", 0)

    record = store.get("task-1")

    assert record is not None
    assert record["task_id"] == "task-1"
    assert record["name"] == "add"
    assert record["state"] == TaskState.PENDING.value
    assert record["node"] == "-"
    assert record["attempts"] == 0


def test_store_updates_task_metadata(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    store.save("task-1", "add", TaskState.PENDING, "-", 0)
    store.save("task-1", "add", TaskState.RUNNING, "node-1", 1)

    record = store.get("task-1")

    assert record is not None
    assert record["state"] == TaskState.RUNNING.value
    assert record["node"] == "node-1"
    assert record["attempts"] == 1


def test_store_lists_tasks(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    store.save("task-2", "multiply", TaskState.PENDING, "-", 0)
    store.save("task-1", "add", TaskState.PENDING, "-", 0)

    records = store.list_tasks()

    assert [record["task_id"] for record in records] == [
        "task-1",
        "task-2",
    ]


def test_store_persists_across_reopen(tmp_path):
    db_path = tmp_path / "tasks.db"

    first_store = SQLiteTaskStore(db_path)
    first_store.save(
        "task-1",
        "add",
        TaskState.RUNNING,
        "node-1",
        1,
    )

    second_store = SQLiteTaskStore(db_path)
    record = second_store.get("task-1")

    assert record is not None
    assert record["state"] == TaskState.RUNNING.value
    assert record["node"] == "node-1"
    assert record["attempts"] == 1


@pytest.mark.parametrize("attempts", [-1, 1.5, True, "1"])
def test_store_rejects_invalid_attempt_count(tmp_path, attempts):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    with pytest.raises(ValueError):
        store.save(
            "task-1",
            "add",
            TaskState.PENDING,
            "-",
            attempts,
        )


def test_store_rejects_invalid_state(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    with pytest.raises(TypeError):
        store.save(
            "task-1",
            "add",
            "PENDING",
            "-",
            0,
        )


def test_store_round_trips_task_payload(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")
    original = make_task()

    store.save_task(original)

    restored = store.get_task(original.task_id)

    assert restored is not None
    assert restored.task_id == original.task_id
    assert restored.task_name == original.task_name
    assert restored.args == original.args
    assert restored.kwargs == original.kwargs
    assert restored.max_retries == original.max_retries
    assert restored.timeout_seconds == original.timeout_seconds


def test_store_returns_none_for_missing_payload(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    assert store.get_task("missing-task") is None


def test_store_lists_task_payloads(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    first = make_task(task_id="task-1", args=(1, 2))
    second = make_task(
        task_id="task-2",
        task_name="multiply",
        args=(3, 4),
    )

    store.save_task(first)
    store.save_task(second)

    restored = store.list_task_payloads()

    assert [task.task_id for task in restored] == [
        "task-1",
        "task-2",
    ]


def test_duplicate_task_id_cannot_overwrite_existing_payload(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    original = make_task(
        task_id="duplicate-id",
        task_name="add",
        args=(10, 20),
    )
    replacement = make_task(
        task_id="duplicate-id",
        task_name="multiply",
        args=(100, 200),
    )

    store.save_task(original)

    with pytest.raises(sqlite3.IntegrityError):
        store.save_task(replacement)

    restored = store.get_task("duplicate-id")

    assert restored is not None
    assert restored.task_name == "add"
    assert restored.args == (10, 20)


def test_duplicate_registration_preserves_existing_metadata(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    original = make_task(task_id="duplicate-id")
    store.save_task(original)

    store.save(
        "duplicate-id",
        "add",
        TaskState.RUNNING,
        "worker-1",
        1,
    )

    replacement = make_task(
        task_id="duplicate-id",
        task_name="multiply",
        args=(5, 6),
    )

    with pytest.raises(sqlite3.IntegrityError):
        store.save_task(replacement)

    record = store.get("duplicate-id")
    restored = store.get_task("duplicate-id")

    assert record is not None
    assert record["state"] == TaskState.RUNNING.value
    assert record["node"] == "worker-1"
    assert record["attempts"] == 1

    assert restored is not None
    assert restored.task_name == "add"


def test_delete_task_removes_metadata_and_payload(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    task = make_task()
    store.save_task(task)

    store.delete_task(task.task_id)

    assert store.get(task.task_id) is None
    assert store.get_task(task.task_id) is None


def test_deleting_metadata_cascades_to_payload(tmp_path):
    store = SQLiteTaskStore(tmp_path / "tasks.db")

    task = make_task()
    store.save_task(task)

    store.delete_task_record(task.task_id)

    assert store.get(task.task_id) is None
    assert store.get_task(task.task_id) is None


def test_store_rejects_in_memory_database(tmp_path):
    with pytest.raises(ValueError, match="file-backed"):
        SQLiteTaskStore(":memory:")


def test_registration_transition_failure_rolls_back_sqlite_task(tmp_path):
    from unittest.mock import AsyncMock

    from tasks.lifecycle import TaskLifecycle
    from tasks.recovery import TaskRecoveryCoordinator

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
    task = make_task(task_id="rollback-task")

    with pytest.raises(OSError, match="simulated transition failure"):
        coordinator.register(task, "node-a")

    assert lifecycle.get(task.task_id) is None
    assert store.get(task.task_id) is None
    assert store.get_task(task.task_id) is None
