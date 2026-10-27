"""Asynchronous TCP communication and remote task execution for MeshWeaver."""

import asyncio
import json
from typing import Any

from execution.engine import ExecutionEngine
from execution.models import TaskStatus
from execution.serializer import TaskSerializationError, deserialize_task
from net.correlation import CorrelationManager, RequestTimeoutError
from net.messages import TASK_ACK, TASK_ASSIGN, TASK_REQUEST, TASK_RESULT, Message

# Maximum size of a newline-delimited TCP message, including its newline.
MAX_MESSAGE_SIZE = 64 * 1024
DEFAULT_REQUEST_TIMEOUT = 5.0


async def read_message(reader: asyncio.StreamReader) -> Message:
    """Read one newline-delimited JSON message from a TCP stream."""
    try:
        raw = await reader.readline()
    except ValueError as exc:
        raise ValueError(
            f"TCP message exceeds the {MAX_MESSAGE_SIZE}-byte limit."
        ) from exc

    if not raw:
        raise EOFError("TCP connection closed before a message was received")

    if len(raw) > MAX_MESSAGE_SIZE:
        raise ValueError(f"TCP message exceeds the {MAX_MESSAGE_SIZE}-byte limit.")

    try:
        return Message.decode(raw.rstrip(b"\r\n"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError("Received an invalid TCP message") from exc


async def send_message(
    writer: asyncio.StreamWriter,
    message: Message,
) -> None:
    """Send one message over a TCP stream."""
    encoded = message.encode()
    if len(encoded) + 1 > MAX_MESSAGE_SIZE:
        raise ValueError(f"TCP message exceeds the {MAX_MESSAGE_SIZE}-byte limit.")

    writer.write(encoded + b"\n")
    await writer.drain()


def _result_payload(result: Any) -> dict[str, Any]:
    """Convert a TaskResult into a JSON-compatible response payload."""
    payload: dict[str, Any] = {
        "task_id": result.task_id,
        "status": result.status.value,
        "attempts": result.attempts,
        "error": result.error,
        "duration_seconds": result.duration_seconds,
    }

    if result.status == TaskStatus.COMPLETED:
        payload["result"] = result.value

    try:
        json.dumps(payload)
    except (TypeError, ValueError) as exc:
        payload["status"] = TaskStatus.FAILED.value
        payload.pop("result", None)
        payload["error"] = f"Task result is not JSON serializable: {exc}"

    return payload


def _failure_response(
    message: Message,
    error: str,
    task_id: str | None = None,
) -> Message:
    """Build a consistent failure response for a task request."""
    payload: dict[str, Any] = {
        "status": TaskStatus.FAILED.value,
        "error": error,
    }

    if task_id is not None:
        payload["task_id"] = task_id

    return Message(
        type=TASK_RESULT,
        sender_id="tcp-server",
        seq=message.seq,
        payload=payload,
        correlation_id=message.correlation_id,
    )


async def _handle_task_request(
    message: Message,
    execution_engine: ExecutionEngine | None,
) -> Message:
    """Validate and execute a registered task request."""
    if not isinstance(message.payload, dict):
        return _failure_response(
            message,
            "TASK_REQUEST payload must be a dictionary.",
        )

    task_data = message.payload.get("task")

    if execution_engine is None:
        return _failure_response(
            message,
            "Remote task execution is not configured on this server.",
        )

    if not isinstance(task_data, dict):
        return _failure_response(
            message,
            "TASK_REQUEST payload must contain a task object.",
        )

    try:
        task = deserialize_task(json.dumps(task_data))
    except (TaskSerializationError, TypeError, ValueError) as exc:
        return _failure_response(message, f"Invalid task request: {exc}")

    try:
        # Keep synchronous task execution off the asyncio event loop.
        result = await asyncio.to_thread(execution_engine.execute, task)
    except (ValueError, RuntimeError) as exc:
        return _failure_response(
            message,
            f"Task execution rejected: {exc}",
            task_id=task.task_id,
        )

    return Message(
        type=TASK_RESULT,
        sender_id="tcp-server",
        seq=message.seq,
        payload=_result_payload(result),
        correlation_id=message.correlation_id,
    )


def _handle_task_assignment(message: Message) -> Message:
    """Validate a task assignment and acknowledge its receipt."""
    payload = message.payload

    if not isinstance(payload, dict):
        return Message(
            type=TASK_ACK,
            sender_id="tcp-server",
            seq=message.seq,
            payload={
                "accepted": False,
                "error": "TASK_ASSIGN payload must be a dictionary.",
            },
            correlation_id=message.correlation_id,
        )

    task = payload.get("task")
    task_id = payload.get("task_id")

    if not isinstance(task, dict):
        return Message(
            type=TASK_ACK,
            sender_id="tcp-server",
            seq=message.seq,
            payload={
                "accepted": False,
                "task_id": task_id,
                "error": "TASK_ASSIGN payload must contain a task object.",
            },
            correlation_id=message.correlation_id,
        )

    actual_task_id = task.get("task_id")

    if (
        not isinstance(task_id, str)
        or not task_id.strip()
        or actual_task_id != task_id
    ):
        return Message(
            type=TASK_ACK,
            sender_id="tcp-server",
            seq=message.seq,
            payload={
                "accepted": False,
                "task_id": task_id,
                "error": (
                    "Assignment task_id must match the task object's task_id."
                ),
            },
            correlation_id=message.correlation_id,
        )

    return Message(
        type=TASK_ACK,
        sender_id="tcp-server",
        seq=message.seq,
        payload={
            "accepted": True,
            "task_id": task_id,
        },
        correlation_id=message.correlation_id,
    )


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    received_messages: list[Message],
    execution_engine: ExecutionEngine | None = None,
) -> None:
    """Receive messages and respond with ACKs or task execution results."""
    try:
        while True:
            try:
                message = await read_message(reader)
            except EOFError:
                break

            received_messages.append(message)

            if message.type == TASK_REQUEST:
                response = await _handle_task_request(message, execution_engine)
            elif message.type == TASK_ASSIGN:
                response = _handle_task_assignment(message)
            else:
                response = Message(
                    type="ACK",
                    sender_id="tcp-server",
                    seq=message.seq,
                    payload={
                        "received": True,
                        "original_type": message.type,
                    },
                    correlation_id=message.correlation_id,
                )

            await send_message(writer, response)

    except (ValueError, ConnectionError):
        # Reject malformed or oversized messages by closing that connection.
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass


async def start_tcp_server(
    host: str = "127.0.0.1",
    port: int = 0,
    execution_engine: ExecutionEngine | None = None,
) -> tuple[asyncio.AbstractServer, list[Message]]:
    """Start a TCP server with optional registered-task execution."""
    received_messages: list[Message] = []

    async def client_connected(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await handle_client(
            reader,
            writer,
            received_messages,
            execution_engine=execution_engine,
        )

    server = await asyncio.start_server(
        client_connected,
        host,
        port,
        limit=MAX_MESSAGE_SIZE,
    )
    return server, received_messages


async def send_tcp_message(
    host: str,
    port: int,
    message: Message,
    timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> Message:
    """Send a correlated TCP request and return its matching response."""
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")

    manager = CorrelationManager()
    correlation_id, _ = manager.create_request()

    # Use a unique ID for this network request.
    message.correlation_id = correlation_id

    reader = None
    writer = None

    try:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host,
                    port,
                    limit=MAX_MESSAGE_SIZE,
                ),
                timeout=timeout,
            )
            await asyncio.wait_for(
                send_message(writer, message),
                timeout=timeout,
            )
            response = await asyncio.wait_for(
                read_message(reader),
                timeout=timeout,
            )
        except TimeoutError as exc:
            raise RequestTimeoutError(
                f"TCP request timed out after {timeout} seconds"
            ) from exc

        if response.correlation_id != correlation_id:
            raise ValueError("TCP response correlation ID does not match the request")

        if not manager.deliver_response(correlation_id, response):
            raise ValueError("TCP response could not be matched to its request")

        return await manager.wait_for_response(correlation_id, timeout)

    finally:
        manager.cancel_request(correlation_id)

        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
