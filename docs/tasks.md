# MeshWeaver Task Serialization and Execution

## Purpose

This document describes the current task models, serialization helpers, and execution engine in the team repository.

## Main Components

- `execution/models.py`: defines `Task`, `TaskResult`, and `TaskStatus`.
- `execution/serializer.py`: serializes registered-task envelopes to JSON and validates incoming task data.
- `execution/function_serializer.py`: serializes and deserializes trusted Python callables using cloudpickle.
- `execution/engine.py`: registers task functions and manages execution, retries, timeouts, cancellation requests, and results.
- `net/`: contains the existing networking and peer communication modules.

## Task Execution Flow

1. Create a task with a registered task name and arguments.
2. Serialize the task envelope to JSON when task transfer is required.
3. Register the corresponding callable with `ExecutionEngine`.
4. Execute the task through the engine.
5. Track task status and execution attempts.
6. Return a structured `TaskResult`.

Function serialization is a separate capability and is not the same as JSON task-envelope serialization.

## Verification

The focused tests for execution-engine behavior, execution models, and function serialization reported 48 passing tests.

Ruff and Black checks for the function serializer and its tests passed.

The latest full test run reported 142 passed, 2 failed, and 18 subtests passed. The two failures were in DHT networking tests:

- `tests/test_dht.py::DHTNetwork::test_refresh_evicts_dead_peer_and_rejoin`
- `tests/test_dht.py::DHTNetwork::test_ten_nodes_one_seed_and_remote_store`

These failures remain unresolved.

## Security and Reliability Limitations

- Cloudpickle deserialization must only be performed on trusted payloads.
- Function serialization does not provide network authentication or encryption.
- A timed-out synchronous function may continue running.
- Cancellation requires task cooperation.
- Full distributed integration and production security are not established by these focused tests.
