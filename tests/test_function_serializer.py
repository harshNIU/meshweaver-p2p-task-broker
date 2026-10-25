"""Tests for trusted Python function serialization."""

import pytest

from execution.function_serializer import (
    FunctionSerializationError,
    deserialize_function,
    serialize_function,
)


def test_regular_function():
    def add(a, b):
        return a + b

    restored = deserialize_function(serialize_function(add))
    assert restored(10, 20) == 30


def test_lambda_function():
    multiply = lambda x, y: x * y

    restored = deserialize_function(serialize_function(multiply))
    assert restored(4, 5) == 20


def test_closure():
    def multiplier(factor):
        def multiply(value):
            return factor * value

        return multiply

    restored = deserialize_function(serialize_function(multiplier(5)))
    assert restored(6) == 30


def test_invalid_callable():
    with pytest.raises(FunctionSerializationError):
        serialize_function(123)


def test_invalid_payload():
    with pytest.raises(FunctionSerializationError):
        deserialize_function(b"invalid-payload")
