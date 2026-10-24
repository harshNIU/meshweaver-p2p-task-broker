"""Asynchronous TCP communication and remote task execution for MeshWeaver."""

import asyncio
import json
from typing import Any

from execution.engine import ExecutionEngine
from execution.models import TaskStatus
from execution.serializer import (
    TaskSerializationError,
    deserialize_task,
)
from net.messages import TASK_REQUEST, TASK_RESULT, Message


async def read_message(reader: asyncio.StreamReader) -> Message:
    """Read one newline-delimited JSON message from a TCP stream."""
    raw = await reader.readline()

    if not raw:
        raise EOFError("TCP connection closed before a message was received")

    try:
        return Message.decode(raw.rstrip(b"\r\n"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError("Received an invalid TCP message") from exc


async def send_message(
    writer: asyncio.StreamWriter,
    message: Message,
) -> None:
    """Send one message over a TCP stream."""
    writer.write(message.encode() + b"\n")
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

    # Validate that the result can actually be sent as JSON.
    try:
        json.dumps(payload)
    except (TypeError, ValueError) as exc:
        payload["status"] = TaskStatus.FAILED.value
        payload.pop("result", None)
        payload["error"] = f"Task result is not JSON serializable: {exc}"

    return payload


async def _handle_task_request(
    message: Message,
    execution_engine: ExecutionEngine | None,
) -> Message:
    """Validate and execute a registered task request."""
    task_data = message.payload.get("task")

    if execution_engine is None:
        return Message(
            type=TASK_RESULT,
            sender_id="tcp-server",
            seq=message.seq,
            payload={
                "status": TaskStatus.FAILED.value,
                "error": "Remote task execution is not configured on this server.",
            },
        )

    if not isinstance(task_data, dict):
        return Message(
            type=TASK_RESULT,
            sender_id="tcp-server",
            seq=message.seq,
            payload={
                "status": TaskStatus.FAILED.value,
                "error": "TASK_REQUEST payload must contain a task object.",
            },
        )

    try:
        task = deserialize_task(json.dumps(task_data))
    except (TaskSerializationError, TypeError, ValueError) as exc:
        return Message(
            type=TASK_RESULT,
            sender_id="tcp-server",
            seq=message.seq,
            payload={
                "status": TaskStatus.FAILED.value,
                "error": f"Invalid task request: {exc}",
            },
        )

    # Keep synchronous task execution off the asyncio event loop.
    result = await asyncio.to_thread(execution_engine.execute, task)

    return Message(
        type=TASK_RESULT,
        sender_id="tcp-server",
        seq=message.seq,
        payload=_result_payload(result),
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
            else:
                response = Message(
                    type="ACK",
                    sender_id="tcp-server",
                    seq=message.seq,
                    payload={
                        "received": True,
                        "original_type": message.type,
                    },
                )

            await send_message(writer, response)

    except (ValueError, ConnectionError):
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

    server = await asyncio.start_server(client_connected, host, port)
    return server, received_messages


async def send_tcp_message(
    host: str,
    port: int,
    message: Message,
) -> Message:
    """Connect to a TCP server, send one message, and return its response."""
    reader, writer = await asyncio.open_connection(host, port)

    try:
        await send_message(writer, message)
        return await read_message(reader)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass
