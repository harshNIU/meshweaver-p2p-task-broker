# Serialization Check

## Purpose

Verify that MeshWeaver can serialize and deserialize registered tasks and
that trusted Python functions can be serialized and restored.

## Components

- `execution/models.py`: task and task-result data models.
- `execution/serializer.py`: JSON serialization and validation for registered tasks.
- `execution/function_serializer.py`: serialization and deserialization of trusted Python callables.
- `tests/test_function_serializer.py`: tests for regular functions, lambdas, closures, and invalid inputs.
- `net/tcp_node.py`: existing TCP task execution transport.

## Verification

Run the focused tests:

```powershell
python -m pytest -q tests/test_function_serializer.py
python -m pytest -q tests/test_execution_engine.py tests/test_execution_models.py tests/test_function_serializer.py
python -m ruff check execution/function_serializer.py tests/test_function_serializer.py
python -m black --check execution/function_serializer.py tests/test_function_serializer.py
```

## Security Note

Function deserialization uses `cloudpickle` and must only be performed on
trusted payloads. Do not deserialize arbitrary network input.

## Known Baseline Test Failures

The full test suite currently has two reproducible DHT networking failures:
`test_refresh_evicts_dead_peer_and_rejoin` and
`test_ten_nodes_one_seed_and_remote_store`. These are separate from the
function-serialization tests and should be investigated by the DHT owner.
