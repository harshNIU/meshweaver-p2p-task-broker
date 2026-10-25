"""Thread-safe in-memory registry of actual task lifecycle records."""
from __future__ import annotations

import threading
import time
from typing import Any

from execution.models import Task, TaskResult, TaskStatus


class TaskRegistry:
    """Store lifecycle records produced by a real ExecutionEngine."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: dict[str, dict[str, Any]] = {}
        self._events: list[dict[str, str]] = []

    def mark_pending(self, task: Task) -> None:
        with self._lock:
            self._records[task.task_id] = {
                "task_id": task.task_id,
                "name": task.task_name,
                "assigned_to": None,
                "attempt": 0,
                "status": TaskStatus.PENDING.value.upper(),
                "created_at": time.time(),
                "duration_seconds": None,
                "error": None,
            }
            self._event("INFO", f"Task {task.task_id} registered ({task.task_name})")

    def mark_running(self, task: Task, attempt: int = 1) -> None:
        with self._lock:
            record = self._records.setdefault(
                task.task_id,
                {
                    "task_id": task.task_id,
                    "name": task.task_name,
                    "assigned_to": None,
                    "attempt": 0,
                    "created_at": time.time(),
                },
            )
            record.update(status=TaskStatus.RUNNING.value.upper(), attempt=attempt)
            self._event("INFO", f"Task {task.task_id} started ({task.task_name})")

    def mark_result(self, task: Task, result: TaskResult) -> None:
        with self._lock:
            record = self._records.setdefault(
                task.task_id,
                {
                    "task_id": task.task_id,
                    "name": task.task_name,
                    "assigned_to": None,
                    "attempt": 0,
                    "created_at": time.time(),
                },
            )
            record.update(
                status=result.status.value.upper(),
                attempt=result.attempts,
                duration_seconds=result.duration_seconds,
                error=result.error,
            )
            self._event(
                "ERROR" if result.status == TaskStatus.FAILED else "INFO",
                f"Task {task.task_id} {result.status.value} ({task.task_name})",
            )

    def _event(self, level: str, message: str) -> None:
        self._events.append(
            {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "level": level,
                "message": message,
            }
        )
        self._events = self._events[-100:]

    def snapshot_tasks(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {key: value for key, value in record.items()
                 if key not in {"created_at"}}
                for record in self._records.values()
            ]

    def snapshot_events(self) -> list[dict[str, str]]:
        with self._lock:
            return list(self._events)
