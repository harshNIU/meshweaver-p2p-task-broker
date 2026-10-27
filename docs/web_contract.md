<<<<<<< HEAD
﻿# MeshWeaver Web Dashboard Contract

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
=======
# MeshWeaver Web Dashboard Data Contract

## 1. Purpose

This document defines the mock data contract used by the optional
MeshWeaver local web dashboard.

The first version uses sample data. It does not connect to live peers
or execute tasks.

## 2. State endpoint

- Method: `GET`
- Path: `/api/state`
- Success status: `200 OK`
- Content type: `application/json`

The response contains a snapshot with node, task, and event information.

## 3. Snapshot fields

| Field | Type | Description |
|---|---|---|
| `snapshot_at` | string | Time represented by the snapshot |
| `mode` | string | Data source, currently `mock` |
| `nodes` | array | Known node records |
| `tasks` | array | Sample task records |
| `events` | array | Recent sample events |

## 4. Node fields

| Field | Type | Description |
|---|---|---|
| `node_id` | string | Node identifier |
| `host` | string | Hostname or IP address |
| `port` | integer | Node communication port |
| `cpu` | number | Sample CPU usage percentage |
| `ram` | number | Sample RAM usage percentage |
| `last_seen` | number | Sample Unix timestamp |
| `state` | string | `ALIVE`, `SUSPECT`, or `DEAD` |

The node field names align with the existing `NodeInfo` contract in
`gossip/state.py`.

## 5. Task fields

| Field | Type | Description |
|---|---|---|
| `task_id` | string | Task identifier |
| `name` | string | Human-readable task name |
| `status` | string | Sample task status |
| `assigned_to` | string or null | Assigned node identifier |
| `attempt` | integer | Sample attempt number |

These are dashboard mock fields, not a claim that the existing task
execution module already exposes this exact API.

## 6. Event fields

| Field | Type | Description |
|---|---|---|
| `timestamp` | string | Human-readable event time |
| `level` | string | Event severity |
| `message` | string | Event description |

## 7. Compatibility and safety

1. The first version must use mock data only.
2. Existing networking and execution modules must not be changed for
   this milestone.
3. The HTTP server binds to `127.0.0.1` by default.
4. Only the documented page and `/api/state` endpoint are served.
5. The server must not expose arbitrary filesystem paths.
6. This dashboard is an optional addition and does not replace the
   required terminal CLI or Rich dashboard.

## 8. Future integration

A later milestone may replace the mock snapshot with real project state.
That change must be coordinated with the relevant team members and must
preserve the agreed field names or document an approved contract change.
>>>>>>> origin/main
