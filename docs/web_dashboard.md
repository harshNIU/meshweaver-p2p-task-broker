# MeshWeaver Web Dashboard

## 1. Purpose

The optional web dashboard provides a browser-based view of MeshWeaver runtime state. It complements the CLI and does not replace the terminal interface or Rich dashboard.

## 2. Start the Dashboard

Activate the project virtual environment and run this command from the repository root:

```powershell
python -m cli.main start --host 127.0.0.1 --port 8000 --web --web-port 8765
```

Open the dashboard URL printed by the CLI, normally:

`http://127.0.0.1:8765/`

Press `Ctrl+C` in the terminal to stop the application.

## 3. HTTP Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/` | Serves the dashboard page |
| GET | `/api/state` | Returns the current runtime snapshot as JSON |
| GET | `/api/events` | Streams runtime snapshots using Server-Sent Events |
| POST | `/api/submit` | Accepts an approved demo task submission |

Unknown paths return HTTP `404`.

## 4. Task Submission Security

Task submission requires the `X-Token` HTTP header. The CLI displays a submission token when starting the web dashboard. Keep this token private.

Only the approved demo tasks `add` and `multiply` are accepted. The server validates JSON payloads and task arguments before passing them to the configured submission callback.

Request bodies must not exceed 4096 bytes. Oversized requests return HTTP `413`. Missing or invalid tokens return HTTP `403`, and invalid task submissions return HTTP `400`.

The server binds to `127.0.0.1` by default. Do not expose the dashboard to an untrusted network without an appropriate security review.

## 5. Error Responses

| Status | Meaning |
|---|---|
| 400 | Invalid request or task payload |
| 403 | Missing or invalid submission token |
| 404 | Unknown endpoint |
| 413 | Request body exceeds the size limit |
| 415 | Unsupported content type |
| 500 | Runtime snapshot or submission failure |
| 503 | Task submission is disabled |

## 6. Run the Tests

Run the web-specific tests:

```powershell
python -m pytest tests/test_web.py -q
```

Run the complete test suite:

```powershell
python -m pytest -q
```

Run lint and formatting checks:

```powershell
python -m ruff check .
python -m black --check .
```

## 7. Limitations

The dashboard is an optional interface. Successful submission to the local execution engine does not by itself prove that a task was distributed to a remote peer. Verify distributed execution separately using the project's networking and task-routing tests.
