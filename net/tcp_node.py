import asyncio
import base64
import json
from typing import Any

from execution.engine import ExecutionEngine
from execution.function_serializer import (
    FunctionSerializationError,
    deserialize_callable_payload,
    deserialize_result_value,
    serialize_callable_payload,
    serialize_result_value,
)
from execution.models import Task, TaskResult, TaskStatus
from execution.serializer import TaskSerializationError, deserialize_task
from net.correlation import CorrelationManager, RequestTimeoutError
from net.messages import (
    TASK_ACK,
    TASK_ASSIGN,
    TASK_REQUEST,
    TASK_RESULT,
    TASK_SUBMIT,
    Message,
)
from security.message_signing import (
    MessageAuthenticationError,
    load_shared_secret,
    sign_message,
    verify_message,
)

MAX_MESSAGE_SIZE = 64 * 1024
DEFAULT_REQUEST_TIMEOUT = 5.0
AUTHENTICATED_TYPES = {
    TASK_REQUEST,
    TASK_SUBMIT,
    TASK_ASSIGN,
    TASK_RESULT,
    TASK_ACK,
}


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


async def send_message(writer: asyncio.StreamWriter, message: Message) -> None:
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
        json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError) as exc:
        payload["status"] = TaskStatus.FAILED.value
        payload.pop("result", None)
        payload["error"] = f"Task result is not JSON serializable: {exc}"

    return payload


def _failure_response(
    message: Message, error: str, task_id: str | None = None
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
    """Validate and execute an authenticated task request."""
    if not isinstance(message.payload, dict):
        return _failure_response(message, "TASK_REQUEST payload must be a dictionary.")

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



async def _handle_task_submit(
    message: Message,
    execution_engine: ExecutionEngine | None,
) -> Message:
    """Authenticate first, then deserialize and execute a callable task."""
    payload = message.payload

    if not isinstance(payload, dict):
        return _failure_response(message, "TASK_SUBMIT payload must be a dictionary.")

    task_data = payload.get("task")
    encoded_callable = payload.get("callable_payload")

    if execution_engine is None:
        return _failure_response(
            message,
            "Remote callable execution is not configured on this server.",
        )

    if not isinstance(task_data, dict):
        return _failure_response(message, "TASK_SUBMIT must contain task metadata.")

    required = {"task_id", "task_name", "max_retries", "timeout_seconds"}
    if set(task_data) != required:
        return _failure_response(message, "TASK_SUBMIT task metadata has invalid fields.")

    if not isinstance(encoded_callable, str):
        return _failure_response(message, "Callable payload must be a Base64 string.")

    try:
        task_id = task_data["task_id"]
        task_name = task_data["task_name"]

        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("Invalid task_id.")

        if not isinstance(task_name, str) or not task_name.strip():
            raise ValueError("Invalid task_name.")

        raw_callable = base64.b64decode(encoded_callable, validate=True)

        function, args, kwargs = deserialize_callable_payload(raw_callable)

        task = Task(
            task_name=task_name,
            args=args,
            kwargs=kwargs,
            task_id=task_id,
            max_retries=task_data["max_retries"],
            timeout_seconds=task_data["timeout_seconds"],
        )
    except (
        ValueError,
        TypeError,
        KeyError,
        FunctionSerializationError,
    ) as exc:
        return _failure_response(
            message,
            f"Invalid callable task: {exc}",
            task_id=task_data.get("task_id") if isinstance(task_data, dict) else None,
        )

    try:
        result = await asyncio.to_thread(
            execution_engine.execute_callable,
            task,
            function,
        )
    except (ValueError, RuntimeError) as exc:
        return _failure_response(
            message,
            f"Callable task execution failed: {type(exc).__name__}: {exc}",
            task_id=task.task_id,
        )

    result_payload = {
        "task_id": result.task_id,
        "status": result.status.value,
        "attempts": result.attempts,
        "error": result.error,
        "duration_seconds": result.duration_seconds,
    }

    if result.status == TaskStatus.COMPLETED:
        try:
            serialized_result = serialize_result_value(result.value)
            result_payload["result_payload"] = base64.b64encode(
                serialized_result
            ).decode("ascii")
        except FunctionSerializationError as exc:
            result_payload["status"] = TaskStatus.FAILED.value
            result_payload["error"] = str(exc)

    return Message(
        type=TASK_RESULT,
        sender_id="tcp-server",
        seq=message.seq,
        payload=result_payload,
        correlation_id=message.correlation_id,
    )


def _handle_task_assignment(message: Message) -> Message:
    """Validate a task assignment and acknowledge its receipt."""
    payload = message.payload

    if not isinstance(payload, dict):
        accepted = False
        task_id = None
        error = "TASK_ASSIGN payload must be a dictionary."
    else:
        task = payload.get("task")
        task_id = payload.get("task_id")
        actual_task_id = task.get("task_id") if isinstance(task, dict) else None

        accepted = (
            isinstance(task, dict)
            and isinstance(task_id, str)
            and bool(task_id.strip())
            and actual_task_id == task_id
        )

        error = (
            None
            if accepted
            else "Assignment task_id must match the task object's task_id."
        )

    response_payload = {
        "accepted": accepted,
        "task_id": task_id,
    }

    if error:
        response_payload["error"] = error

    return Message(
        type=TASK_ACK,
        sender_id="tcp-server",
        seq=message.seq,
        payload=response_payload,
        correlation_id=message.correlation_id,
    )


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    received_messages: list[Message],
    execution_engine: ExecutionEngine | None = None,
    secret: bytes | None = None,
) -> None:
    """Receive messages and authenticate task traffic before processing it."""
    try:
        while True:
            try:
                message = await read_message(reader)
            except EOFError:
                break

            if message.type in AUTHENTICATED_TYPES:
                key = secret if secret is not None else load_shared_secret()
                verify_message(message, key)

            received_messages.append(message)

            if message.type == TASK_REQUEST:
                response = await _handle_task_request(
                    message,
                    execution_engine,
                )
            elif message.type == TASK_SUBMIT:
                response = await _handle_task_submit(
                    message,
                    execution_engine,
                )
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

            if response.type in AUTHENTICATED_TYPES:
                response_key = secret if secret is not None else load_shared_secret()
                sign_message(response, response_key)

            await send_message(writer, response)

    except (
        ValueError,
        ConnectionError,
        MessageAuthenticationError,
        OSError,
    ):
        # Invalid or unauthenticated task traffic is rejected before execution.
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
    *,
    secret: bytes | None = None,
    ssl_context: Any | None = None,
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
            secret=secret,
        )

    server = await asyncio.start_server(
        client_connected,
        host,
        port,
        ssl=ssl_context,
        limit=MAX_MESSAGE_SIZE,
    )

    return server, received_messages


async def send_tcp_message(
    host: str,
    port: int,
    message: Message,
    timeout: float = DEFAULT_REQUEST_TIMEOUT,
    *,
    secret: bytes | None = None,
    ssl_context: Any | None = None,
) -> Message:
    """Send a correlated TCP request and verify authenticated task responses."""
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")

    manager = CorrelationManager()
    correlation_id, _ = manager.create_request()
    message.correlation_id = correlation_id

    key = secret

    if message.type in AUTHENTICATED_TYPES:
        key = key if key is not None else load_shared_secret()
        sign_message(message, key)

    reader = None
    writer = None

    try:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host,
                    port,
                    ssl=ssl_context,
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

        if response.type in AUTHENTICATED_TYPES:
            if key is None:
                raise MessageAuthenticationError(
                    "No shared secret is available to verify the TCP response."
                )

            verify_message(response, key)

        if not manager.deliver_response(correlation_id, response):
            raise ValueError("TCP response could not be matched to its request")

        return await manager.wait_for_response(
            correlation_id,
            timeout,
        )

    finally:
        manager.cancel_request(correlation_id)

        if writer is not None:
            writer.close()

            try:
                await writer.wait_closed()
            except ConnectionError:
                pass


async def send_callable_task(
    host: str,
    port: int,
    task: Task,
    function: Any,
    timeout: float = DEFAULT_REQUEST_TIMEOUT,
    *,
    secret: bytes | None = None,
    ssl_context: Any | None = None,
) -> TaskResult:
    """Submit a callable task to a trusted, authenticated TCP worker."""
    if not isinstance(task, Task):
        raise TypeError("task must be a Task instance.")

    callable_bytes = serialize_callable_payload(
        function,
        task.args,
        task.kwargs,
    )

    message = Message(
        type=TASK_SUBMIT,
        sender_id="tcp-client",
        seq=0,
        payload={
            "task": {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "max_retries": task.max_retries,
                "timeout_seconds": task.timeout_seconds,
            },
            "callable_payload": base64.b64encode(
                callable_bytes
            ).decode("ascii"),
        },
    )

    response = await send_tcp_message(
        host,
        port,
        message,
        timeout=timeout,
        secret=secret,
        ssl_context=ssl_context,
    )

    if response.type != TASK_RESULT:
        raise ValueError("Remote worker returned an unexpected response type.")

    data = response.payload
    if not isinstance(data, dict):
        raise TypeError("Remote worker returned an invalid task result.")

    if data.get("task_id") != task.task_id:
        raise ValueError("Remote task result ID does not match the submitted task.")

    try:
        status = TaskStatus(data["status"])
        attempts = data["attempts"]
        duration = data.get("duration_seconds", 0.0)
        error = data.get("error")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Remote task result has invalid fields.") from exc

    if status not in {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
    }:
        raise ValueError("Remote task result is not in a terminal state.")

    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
        raise ValueError("Remote task result has an invalid attempt count.")

    if (
        isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or duration < 0
    ):
        raise ValueError("Remote task result has an invalid duration.")

    value = None
    if status == TaskStatus.COMPLETED:
        encoded_result = data.get("result_payload")
        if not isinstance(encoded_result, str):
            raise ValueError("Completed remote task has no serialized result.")

        try:
            result_bytes = base64.b64decode(encoded_result, validate=True)
            value = deserialize_result_value(result_bytes)
        except (ValueError, FunctionSerializationError) as exc:
            raise ValueError("Could not deserialize the remote task result.") from exc

    return TaskResult(
        task_id=task.task_id,
        status=status,
        attempts=attempts,
        value=value,
        error=error,
        duration_seconds=float(duration),
    )
