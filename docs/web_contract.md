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