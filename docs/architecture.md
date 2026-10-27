# MeshWeaver Architecture

## 1. Overview

MeshWeaver is a peer-to-peer task broker with networking, task routing, and task execution components. The optional web dashboard provides a browser-based view of runtime state.

## 2. Main Components

- **CLI (`cli/`)** — Starts the application and accepts runtime options.
- **Networking (`net/`, `dht/`)** — Supports peer communication and discovery.
- **Task routing** — Coordinates task requests and routing between nodes.
- **Execution (`execution/`)** — Handles task execution and lifecycle tracking.
- **Task registry** — Tracks registered tasks and their associated state.
- **Web dashboard (`web/`)** — Serves the browser interface and HTTP endpoints.
- **Tests (`tests/`)** — Verifies application components and integration behavior.

## 3. Web Dashboard Data Flow

1. The CLI starts the runtime and optionally enables the web dashboard with `--web`.
2. The dashboard server listens on the configured web host and port.
3. `GET /api/state` obtains a snapshot from the configured snapshot provider.
4. `GET /api/events` streams snapshots using Server-Sent Events.
5. `POST /api/submit` validates an authenticated request and passes an approved task to the configured submission callback.
6. The execution engine and task registry provide runtime task state to the dashboard integration.

## 4. Security Boundaries

- The web server binds to `127.0.0.1` by default.
- Task submission requires the `X-Token` header.
- Only the approved demo tasks `add` and `multiply` are accepted by the web submission endpoint.
- Request bodies are limited to 4096 bytes.
- Unknown routes are rejected.
- Request payloads are validated before task submission.

## 5. Design Considerations

The web dashboard is an optional interface and does not replace the CLI or Rich dashboard. Dashboard task submission and local execution do not automatically prove successful remote task distribution. Distributed execution should be verified through the networking and routing tests.

## 6. Verification

Run the complete test suite:

```powershell id="mx54g8"
python -m pytest -q
```

Run the web-specific tests:

```powershell id="4o0vck"
python -m pytest tests/test_web.py -q
```
