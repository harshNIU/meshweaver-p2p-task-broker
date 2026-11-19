
"""Serialization helpers for trusted Python functions and task payloads."""

from collections.abc import Callable
from typing import Any

import cloudpickle


class FunctionSerializationError(ValueError):
    """Raised when function or task payload serialization fails."""


def serialize_function(func: Callable[..., Any]) -> bytes:
    """Serialize a callable into trusted bytes."""
    if not callable(func):
        raise FunctionSerializationError("Expected a callable Python object.")

    try:
        return cloudpickle.dumps(func)
    except Exception as exc:
        raise FunctionSerializationError(
            f"Function serialization failed: {exc}"
        ) from exc


def deserialize_function(payload: bytes) -> Callable[..., Any]:
    """Deserialize a callable from trusted bytes only."""
    if not isinstance(payload, bytes):
        raise FunctionSerializationError("Serialized payload must be bytes.")

    try:
        func = cloudpickle.loads(payload)
    except Exception as exc:
        raise FunctionSerializationError(
            f"Function deserialization failed: {exc}"
        ) from exc

    if not callable(func):
        raise FunctionSerializationError("Deserialized object is not callable.")

    return func


def serialize_callable_payload(
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> bytes:
    """Serialize a callable together with its positional and keyword arguments."""
    if not callable(function):
        raise FunctionSerializationError("Expected a callable Python object.")

    if not isinstance(args, tuple):
        raise FunctionSerializationError("Task arguments must be a tuple.")

    if not isinstance(kwargs, dict) or not all(
        isinstance(key, str) for key in kwargs
    ):
        raise FunctionSerializationError(
            "Task keyword arguments must be a dictionary with string keys."
        )

    try:
        return cloudpickle.dumps(
            {
                "function": function,
                "args": args,
                "kwargs": kwargs,
            }
        )
    except Exception as exc:
        raise FunctionSerializationError(
            f"Callable task serialization failed: {exc}"
        ) from exc


def deserialize_callable_payload(
    payload: bytes,
) -> tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]:
    """Deserialize a trusted callable task payload."""
    if not isinstance(payload, bytes):
        raise FunctionSerializationError("Serialized payload must be bytes.")

    try:
        data = cloudpickle.loads(payload)
    except Exception as exc:
        raise FunctionSerializationError(
            f"Callable task deserialization failed: {exc}"
        ) from exc

    if not isinstance(data, dict):
        raise FunctionSerializationError("Callable task payload must be a dictionary.")

    if set(data) != {"function", "args", "kwargs"}:
        raise FunctionSerializationError("Callable task payload has invalid fields.")

    function = data["function"]
    args = data["args"]
    kwargs = data["kwargs"]

    if not callable(function):
        raise FunctionSerializationError("Deserialized function is not callable.")

    if not isinstance(args, tuple):
        raise FunctionSerializationError("Deserialized task arguments must be a tuple.")

    if not isinstance(kwargs, dict) or not all(
        isinstance(key, str) for key in kwargs
    ):
        raise FunctionSerializationError(
            "Deserialized task keyword arguments are invalid."
        )

    return function, args, kwargs


def serialize_result_value(value: Any) -> bytes:
    """Serialize an arbitrary task result value."""
    try:
        return cloudpickle.dumps(value)
    except Exception as exc:
        raise FunctionSerializationError(
            f"Result serialization failed: {exc}"
        ) from exc


def deserialize_result_value(payload: bytes) -> Any:
    """Deserialize a result value from trusted bytes only."""
    if not isinstance(payload, bytes):
        raise FunctionSerializationError("Serialized result must be bytes.")

    try:
        return cloudpickle.loads(payload)
    except Exception as exc:
        raise FunctionSerializationError(
            f"Result deserialization failed: {exc}"
        ) from exc
