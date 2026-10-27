<<<<<<< HEAD
﻿# Member 4 — Execution Engine Design
=======
# Member 4 � Execution Engine Design
>>>>>>> origin/main

## Public Interface

- `Task`: describes a registered task name, positional arguments, keyword arguments, task ID, retry limit, and optional timeout.
- `TaskResult`: records the task ID, status, attempts, result value, error, and duration.
- `TaskStatus`: defines `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, and `CANCELLED`.
- `ExecutionEngine.register(name, function)`: registers a callable under a task name.
- `ExecutionEngine.execute(task)`: executes a task and returns a `TaskResult`.
- `ExecutionEngine.cancel_task(task_id)`: requests cooperative cancellation of a running task.
- `ExecutionEngine.get_task_status(task_id)`: returns the latest known status.
- `ExecutionEngine.get_task_result(task_id)`: returns the stored result, if available.

## Lifecycle

The engine uses these statuses:

`PENDING -> RUNNING -> COMPLETED`

Execution failures can lead to retries before the task reaches `COMPLETED` or `FAILED`. Cancellation can result in `CANCELLED`.

The current enum does not define a separate `RETRYING` status.

## Retry and Timeout Behavior

- The engine retries failures according to the task's `max_retries` setting.
- A timeout stops the engine from waiting for the result, but does not forcibly terminate the underlying Python thread.
- The engine does not retry after a timeout because the timed-out function may still be running and could repeat side effects.
- Cancellation is cooperative. A callable must check its supplied cancellation event to stop promptly.

## Serialization and Security

- `execution/serializer.py` serializes registered-task envelopes to JSON.
- `execution/function_serializer.py` uses cloudpickle for trusted Python callables.
- Cloudpickle deserialization must never be performed on untrusted payloads.
- Function serialization does not provide network authentication or encryption.

## Integration Boundaries

The engine is part of the existing MeshWeaver team architecture. Integration with networking, routing, and other peer-to-peer components must be verified separately.

## Reliability Considerations

- Retries can repeat side effects.
- Timed-out functions may continue running.
- Cancellation depends on task cooperation.
- Full distributed reliability and production security are not established by the focused tests.
<<<<<<< HEAD

=======
>>>>>>> origin/main
