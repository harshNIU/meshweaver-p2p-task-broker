# MeshWeaver Web Dashboard Contract

## Purpose

This document defines the shared interface between the MeshWeaver dashboard,
task lifecycle, and task submission components.

The dashboard must use the existing task submission path. It must never accept
arbitrary Python code, function bodies, or serialized pickle payloads from a browser.

## 1. Endpoints

| Endpoint | Method | Purpose | Authentication |
|---|---|---|---|
| `/` | GET | Serve the dashboard page | None |
| `/api/state` | GET | Return the current mesh snapshot as JSON | None; read-only |
| `/api/events` | GET | Stream snapshots using Server-Sent Events | None; read-only |
| `/api/submit` | POST | Submit an approved demo task | `X-Token` header required |

## 2. Snapshot Contract

The snapshot is a JSON object containing:

- `ts`: snapshot timestamp as a Unix timestamp.
- `self`: identifier of the local node.
- `nodes`: list of known mesh nodes.
- `tasks`: list of tracked tasks.
- `events`: recent human-readable mesh events.

Each node entry contains `id`, `host`, `port`, `cpu`, `ram`, `state`, and `last_seen`.

Node states are `ALIVE`, `SUSPECT`, and `DEAD`.

Each task entry contains:

- `task_id`: unique task identifier.
- `name`: registered task name.
- `state`: `PENDING`, `ASSIGNED`, `RUNNING`, `DONE`, or `FAILED`.
- `node`: assigned or executing node identifier, or `-`.
- `attempts`: number of execution attempts started.

## 3. Task State Contract

The dashboard lifecycle is:

`PENDING -> ASSIGNED -> RUNNING -> DONE`

A task may transition to `FAILED` when execution fails.

The existing `execution.models.TaskStatus` enum uses `COMPLETED` and
`CANCELLED`, while `tasks.lifecycle.TaskState` uses `DONE`.
Integration code must explicitly map these representations.

## 4. Demo Submission Contract

The request body accepts a registered task name and JSON-compatible arguments.

Example:

```json
{
  "task": "monte_carlo_pi",
  "args": [1000000]
}
```

Only explicitly allowlisted demo tasks may be submitted. A successful submission
returns HTTP `202 Accepted` with a task identifier.

## 5. Security Requirements

- Bind to `127.0.0.1` by default.
- Require an explicit option and warning before listening on a LAN interface.
- Generate a fresh, unpredictable token at startup.
- Require the token in the `X-Token` header for task submission.
- Compare tokens using a constant-time comparison.
- Limit request-body size and header-read time.
- Do not expose directory listings.
- Render untrusted text safely.
- Route demo submissions through the existing task submission and security path.
- Never deserialize untrusted cloudpickle payloads.
- Plain HTTP on a LAN does not provide encryption; TLS is needed for protected
  network communication.

## 6. Integration Status

This document defines the interface; it does not claim the web dashboard is
implemented. The server, snapshot adapter, authentication checks, submission
adapter, and endpoint tests must be implemented and verified separately.
