"""JSON serialization helpers for MeshWeaver tasks."""

import json
from typing import Any

from execution.models import Task


class TaskSerializationError(ValueError):
    """Raised when task data cannot be serialized or decoded."""


def task_to_dict(task: Task) -> dict[str, Any]:
    """Convert a Task into a JSON-compatible dictionary."""
    return {
        "task_id": task.task_id,
        "task_name": task.task_name,
        "args": list(task.args),
        "kwargs": task.kwargs,
        "max_retries": task.max_retries,
        "timeout_seconds": task.timeout_seconds,
    }


def serialize_task(task: Task) -> str:
    """Serialize a task into a JSON string."""
    try:
        return json.dumps(task_to_dict(task))
    except (TypeError, ValueError) as exc:
        raise TaskSerializationError(f"Could not serialize task: {exc}") from exc


def deserialize_task(raw: str) -> Task:
    """Deserialize JSON and validate its task fields."""
    try:
        data = json.loads(raw)

        if not isinstance(data, dict):
            raise TaskSerializationError("Task JSON must contain an object.")

        required = {"task_id", "task_name", "args", "kwargs"}
        missing = required - data.keys()
        if missing:
            raise TaskSerializationError(
                f"Missing fields: {', '.join(sorted(missing))}"
            )

        allowed = required | {"max_retries", "timeout_seconds"}
        unexpected = data.keys() - allowed
        if unexpected:
            raise TaskSerializationError(
                f"Unexpected fields: {', '.join(sorted(unexpected))}"
            )

        if not isinstance(data["task_id"], str):
            raise TaskSerializationError("'task_id' must be a string.")
        if not isinstance(data["task_name"], str):
            raise TaskSerializationError("'task_name' must be a string.")
        if not isinstance(data["args"], list):
            raise TaskSerializationError("'args' must be a list.")
        if not isinstance(data["kwargs"], dict):
            raise TaskSerializationError("'kwargs' must be an object.")

        return Task(
            task_id=data["task_id"],
            task_name=data["task_name"],
            args=tuple(data["args"]),
            kwargs=data["kwargs"],
            max_retries=data.get("max_retries", 0),
            timeout_seconds=data.get("timeout_seconds"),
        )

    except TaskSerializationError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise TaskSerializationError(f"Invalid task JSON: {exc}") from exc
