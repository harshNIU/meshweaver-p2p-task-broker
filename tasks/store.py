"""SQLite-backed persistence for MeshWeaver task records and payloads."""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import cloudpickle

from execution.models import Task
from tasks.lifecycle import TaskState


class SQLiteTaskStore:
    """Persist task lifecycle metadata and serialized task payloads."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)

        if not self.db_path.strip():
            raise ValueError("db_path must be a non-empty path")

        if self.db_path == ":memory:":
            raise ValueError(
                "SQLiteTaskStore requires a file-backed database. "
                "The current connection strategy does not support :memory:."
            )

        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        """Create a configured SQLite connection."""
        connection = sqlite3.connect(self.db_path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")

        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        """Create persistence tables if they do not exist."""
        with self._connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS task_records (
                    task_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    node TEXT NOT NULL,
                    attempts INTEGER NOT NULL CHECK (attempts >= 0)
                )
                """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS task_payloads (
                    task_id TEXT PRIMARY KEY
                        REFERENCES task_records(task_id) ON DELETE CASCADE,
                    payload BLOB NOT NULL
                )
                """)

    @staticmethod
    def _validate(
        task_id: str,
        name: str,
        state: TaskState,
        node: str,
        attempts: int,
    ) -> None:
        """Validate lifecycle metadata before persistence."""
        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("task_id must be a non-empty string")

        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")

        if not isinstance(state, TaskState):
            raise TypeError("state must be a TaskState")

        if not isinstance(node, str):
            raise TypeError("node must be a string")

        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
            raise ValueError("attempts must be a non-negative integer")

    def save(
        self,
        task_id: str,
        name: str,
        state: TaskState,
        node: str,
        attempts: int,
    ) -> None:
        """Insert or update task lifecycle metadata atomically."""
        self._validate(task_id, name, state, node, attempts)

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO task_records
                    (task_id, name, state, node, attempts)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    name = excluded.name,
                    state = excluded.state,
                    node = excluded.node,
                    attempts = excluded.attempts
                """,
                (task_id, name, state.value, node, attempts),
            )

    def save_task(self, task: Task) -> None:
        """Persist an original task and its execution payload."""
        if not isinstance(task, Task):
            raise TypeError("task must be a Task")

        payload = cloudpickle.dumps(
            {
                "task_name": task.task_name,
                "args": task.args,
                "kwargs": task.kwargs,
                "task_id": task.task_id,
                "max_retries": task.max_retries,
                "timeout_seconds": task.timeout_seconds,
            }
        )

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO task_records
                    (task_id, name, state, node, attempts)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO NOTHING
                """,
                (
                    task.task_id,
                    task.task_name,
                    TaskState.PENDING.value,
                    "-",
                    0,
                ),
            )

            connection.execute(
                """
                INSERT INTO task_payloads (task_id, payload)
                VALUES (?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    payload = excluded.payload
                """,
                (task.task_id, payload),
            )

    @staticmethod
    def _deserialize_task(payload: bytes, expected_task_id: str) -> Task:
        """Deserialize and validate a task from trusted local storage.

        WARNING: cloudpickle deserialization can execute arbitrary code.
        Never deserialize payloads from untrusted or externally writable
        database files.
        """
        data: Any = cloudpickle.loads(payload)

        if not isinstance(data, dict):
            raise TypeError("Stored task payload is invalid")

        task = Task(**data)

        if task.task_id != expected_task_id:
            raise ValueError("Stored task ID does not match requested ID")

        return task

    def get_task(self, task_id: str) -> Task | None:
        """Restore one task from trusted local database contents."""
        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("task_id must be a non-empty string")

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload
                FROM task_payloads
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()

        if row is None:
            return None

        return self._deserialize_task(row["payload"], task_id)

    def get(self, task_id: str) -> dict[str, Any] | None:
        """Return one lifecycle record, or None if absent."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT task_id, name, state, node, attempts
                FROM task_records
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()

        return dict(row) if row is not None else None

    def list_tasks(self) -> list[dict[str, Any]]:
        """Return lifecycle records ordered by task ID."""
        with self._connect() as connection:
            rows = connection.execute("""
                SELECT task_id, name, state, node, attempts
                FROM task_records
                ORDER BY task_id
                """).fetchall()

        return [dict(row) for row in rows]

    def list_task_payloads(self) -> list[Task]:
        """Restore all task payloads from trusted local storage."""
        with self._connect() as connection:
            rows = connection.execute("""
                SELECT p.task_id, p.payload
                FROM task_payloads AS p
                JOIN task_records AS r ON r.task_id = p.task_id
                ORDER BY p.task_id
                """).fetchall()

        return [self._deserialize_task(row["payload"], row["task_id"]) for row in rows]

    def delete_task_payload(self, task_id: str) -> None:
        """Delete a task payload without deleting its lifecycle record."""
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM task_payloads WHERE task_id = ?",
                (task_id,),
            )

    def delete_task_record(self, task_id: str) -> None:
        """Delete lifecycle metadata and cascade-delete its payload."""
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM task_records WHERE task_id = ?",
                (task_id,),
            )

    def delete_task(self, task_id: str) -> None:
        """Delete a task and all associated persisted information."""
        self.delete_task_record(task_id)
