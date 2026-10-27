# MeshWeaver Web Dashboard Contract

## M2 decision record — 27 October 2026

**Decision: the optional web dashboard is approved by the mentor, as reported
by Harsh (M2) on 27 October 2026.** This records the team lead's confirmation;
no approval date, mentor name or verbatim statement was supplied. The web view
supplements the terminal interface and does not replace the core project work.

M2 has reviewed the current implementation at `aa76697`. Mentor permission for
the feature is separate from technical acceptance: the integration gaps below
must be resolved before claiming compliance with this contract.

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
  "name": "add",
  "args": [2, 3],
  "kwargs": {}
}
```

The current allowlist is `add` and `multiply`. Only explicitly allowlisted demo tasks may be submitted. A successful submission
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

## 6. Shared snapshot ownership and proposed integration

M5 owns the shared dashboard entry point, proposed as
`cli/snapshot.py:snapshot(...)`. Both Rich and web views should consume this
same function and must not maintain separate runtime state or select workers.
This is M2's integration proposal, not a claim that all owners have signed off.

The current live implementation is `web/snapshot.py:build_snapshot(...)`,
called by `cli/main.py`. Neither `cli/snapshot.py` nor `cli/dashboard.py` exists
at the reviewed revision. M5 should reuse/move the current live builder behind
the shared entry point, retain a compatibility wrapper during migration, and
update both consumers together. Do not replace working live data with mock data.
Mocks should remain explicit test/demo fixtures.

The proposed input is the existing builder's keyword-only runtime context:
`protocol`, `transport`, `node_id`, `host`, `port`, and `task_registry`.
The return value is a detached, finite-JSON-compatible dictionary with the
canonical fields in section 2. The function is read-only: no task submission,
state transitions or routing decisions. Capture runtime state on its owning
event loop or under appropriate locks before serving it from HTTP threads.

Canonical units and adapters:

- `ts` and `nodes[].last_seen`: Unix seconds; unknown peer time is `null`.
- `nodes[].id`: the shared NodeInfo `node_id`; CPU/RAM are measured percentages
  in [0, 100], or `null` when unavailable. Unknown load must not mean idle.
- `tasks[].node`: actual assigned node ID, or `"-"` if unassigned.
- `tasks[].attempts`: non-negative execution-attempt count.
- Map executor `COMPLETED` to dashboard `DONE`; preserve cancellation as
  `state: "FAILED", reason: "CANCELLED"` under the five-state dashboard schema.
  A cancelled task must never appear as successfully completed.
- `events`: bounded objects with `timestamp` (ISO-8601 UTC), `level`, `message`;
  M3 supplies actual SUSPECT/DEAD transitions. Do not invent transitions.

These adapters apply only at the dashboard boundary; do not change the shared
NodeInfo or executor enums. M3 and M4 must confirm their field semantics with M5.

## 7. Current implementation versus required contract

| Area | Observed at aa76697 | Required follow-up |
|---|---|---|
| Web endpoints | Server, HTML, state, SSE and submit endpoints exist | Keep endpoint tests in CI |
| Snapshot entry point | `web.snapshot.build_snapshot` | M5 provides one shared entry point for both views |
| Snapshot fields | `snapshot_at` ISO string and `nodes[].node_id` | Add canonical `ts` and `id`; migrate UI/tests together, retain aliases temporarily |
| Peer timestamp | ISO string or null | Canonical Unix seconds or null; explicit adapter |
| Task completion | Uppercased executor status can remain `COMPLETED` | Map to `DONE`; explicitly represent cancellation |
| Browser submission | CLI callback invokes `engine.submit_task(task)` locally | M4/M1 wire the existing signed submission channel; no local shortcut |
| Web authentication | Startup token, constant-time comparison, allowlist | Token is HTTP access control, not a node signature or TLS |
| Binding | CLI passes node `--host` to HTTP server | M5 separates web bind settings; explicit LAN opt-in and warning |
| Request limits | 4096-byte body limit exists | M5 adds bounded header/body reads and tests |

Until the signed submission adapter exists, use read-only web mode
(`task_submitter=None`) when demonstrating contract compliance. The current CLI
still installs the local callback: this document does not silently disable or
certify that behavior.

## 8. Ownership and acceptance checklist

- M2: mentor decision recorded, architecture documented, web review recorded.
- M3: confirm peer fields/units and supply real health-transition events.
- M4: confirm task fields/state mapping and route demo submission through the
  shared signed path without accepting browser-supplied code or pickle.
- M5: shared snapshot, Rich consumer, web migration, bind controls, request
  timeouts, integration tests and user guide.
- M1: transport signatures/verification and TLS integration.

Acceptance requires tests that compare both views' snapshot data, cover task
state adapters, reject invalid tokens/oversized or slow requests, and prove the
web callback reaches the signed submission boundary. A green unit-test suite
alone does not prove those missing integration checks have been implemented.

Detailed findings and evidence: [M2 web review](m2_web_review.md).
