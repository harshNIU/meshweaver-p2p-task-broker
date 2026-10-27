
"""Reliable TCP task-assignment handshake with retry and backoff."""

import asyncio
from typing import Any

from net.correlation import RequestTimeoutError
from net.messages import Message, TASK_ASSIGN, TASK_ACK
from net.tcp_node import send_tcp_message


DEFAULT_TIMEOUT = 3.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF = 0.25


class TaskAssignmentError(RuntimeError):
    """Raised when a task assignment cannot be acknowledged."""


async def assign_task(
    host: str,
    port: int,
    task: dict[str, Any],
    *,
    sender_id: str = "task-client",
    seq: int = 1,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
) -> Message:
    """Send TASK_ASSIGN and retry with exponential backoff until acknowledged.

    max_retries is the number of retries after the initial attempt.
    The task dictionary must contain a non-empty task_id.
    """
    if not isinstance(task, dict):
        raise TypeError("task must be a dictionary")

    task_id = task.get("task_id")
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("task must contain a non-empty task_id")

    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")
    if max_retries < 0:
        raise ValueError("max_retries cannot be negative")
    if backoff < 0:
        raise ValueError("backoff cannot be negative")

    last_error: Exception | None = None

    for attempt in range(max_retries + 1):
        request = Message(
            type=TASK_ASSIGN,
            sender_id=sender_id,
            seq=seq,
            payload={"task": task, "task_id": task_id},
        )

        try:
            response = await send_tcp_message(
                host, port, request, timeout=timeout
            )

            if response.type != TASK_ACK:
                raise TaskAssignmentError(
                    f"Expected {TASK_ACK}, received {response.type}"
                )

            if not isinstance(response.payload, dict):
                raise TaskAssignmentError("TASK_ACK payload must be a dictionary")

            if response.payload.get("task_id") != task_id:
                raise TaskAssignmentError(
                    "TASK_ACK task_id does not match the assignment"
                )

            if response.payload.get("accepted") is not True:
                raise TaskAssignmentError(
                    response.payload.get("error", "Task assignment was rejected")
                )

            return response

        except (OSError, ConnectionError, TimeoutError, TaskAssignmentError) as exc:
            last_error = exc

            # A malformed or mismatched acknowledgement is not a transient
            # network failure, so do not retry it.
            if isinstance(exc, TaskAssignmentError):
                raise

            if attempt < max_retries:
                await asyncio.sleep(backoff * (2 ** attempt))

    raise TaskAssignmentError(
        f"Could not assign task {task_id!r} after "
        f"{max_retries + 1} attempts: {last_error}"
    ) from last_error
