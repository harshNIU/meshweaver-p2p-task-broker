# Member 4 Viva Notes — Task Serializer and Execution Lead

## 1. What is MeshWeaver?

MeshWeaver is a peer-to-peer task-broker project. My contribution focuses on task models, task serialization, trusted function serialization, and task execution behavior within the existing team codebase.

## 2. Why do we need task serialization?

A task object exists in memory. Serialization converts its supported data into a transferable representation so another component can reconstruct the task envelope. In this implementation, registered-task envelopes use JSON serialization.

## 3. What does the task serializer do?

`execution/serializer.py` converts a `Task` into a JSON-compatible dictionary and then a JSON string. Deserialization validates the required fields and reconstructs the `Task` object. Unsupported or malformed task data raises `TaskSerializationError`.

## 4. Why use cloudpickle?

`execution/function_serializer.py` uses cloudpickle to serialize Python callables. The tests cover a regular function, a lambda, and a closure. This is separate from the JSON serializer for registered-task envelopes.

## 5. Is cloudpickle safe for arbitrary input?

No. Deserialization can execute code. Only deserialize trusted payloads. Function serialization alone does not secure a network connection.

## 6. What is the role of ExecutionEngine?

`execution/engine.py` manages registered task functions. It validates registrations, executes tasks, tracks statuses and results, supports retry limits, and handles timeout and cancellation requests.

## 7. Which task statuses are defined?

The current `TaskStatus` enum defines `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, and `CANCELLED`. There is no separate `RETRYING` status in the current enum.

## 8. How does retry behavior work?

The engine can retry failures up to the task's configured `max_retries`. However, retries can repeat side effects. The engine stops retrying after a timeout because the timed-out function may still be running.

## 9. What happens when a task times out?

The engine waits for a worker thread for the configured timeout. If the thread is still running, the engine reports a timeout. Python cannot forcibly terminate that underlying thread, so the function may continue running.

## 10. How does cancellation work?

The engine can set a cancellation event for a running task when cancellation is accepted. A task that supports cancellation must check the event and cooperate by stopping its work. Cancellation is not a guarantee that arbitrary synchronous code stops immediately.

## 11. What does TaskResult contain?

`TaskResult` records the task ID, status, number of attempts, returned value, error information, and execution duration.

## 12. What do the current tests prove?

The focused tests for execution-engine behavior, execution models, and function serialization reported 48 passing tests. Ruff and Black checks for the new function serializer and its tests also passed.

The full suite reported 142 passed, 2 failed, and 18 subtests passed. Two DHT networking tests remain failing, so I would not claim that the entire suite passes.

## 13. Is the complete distributed system finished?

I cannot claim that from these focused checks alone. The networking, routing, peer discovery, and other team components must be validated as an integrated system.

## 14. What are the main security limitations?

Cloudpickle must only deserialize trusted payloads. Function serialization does not provide network authentication or encryption. Those protections must be implemented and verified separately.

## Short Closing Statement

My contribution includes trusted Python function serialization and tests, alongside the existing task models and execution engine. The focused validation passes, while two DHT networking failures remain to be investigated before claiming that the full test suite is clean.
