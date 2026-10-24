
"""Explicitly allowlisted demonstration tasks for MeshWeaver."""


def add(a, b):
    """Return the sum of two numbers."""
    return a + b


def multiply(a, b):
    """Return the product of two numbers."""
    return a * b


def register_builtin_tasks(engine):
    """Register the approved demonstration tasks with an execution engine."""
    engine.register("add", add)
    engine.register("multiply", multiply)
