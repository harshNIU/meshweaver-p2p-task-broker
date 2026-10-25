"""Serialization helpers for trusted Python functions."""

from collections.abc import Callable
from typing import Any

import cloudpickle


class FunctionSerializationError(ValueError):
    """Raised when function serialization or decoding fails."""


def serialize_function(func: Callable[..., Any]) -> bytes:
    """Serialize a callable into bytes."""
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
