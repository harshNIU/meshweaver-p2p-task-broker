# MeshWeaver User Guide

## 1. Overview

MeshWeaver provides a command-line interface for starting a node and an optional web dashboard for viewing runtime state and submitting approved demo tasks.

## 2. Prerequisites

- Python installed
- Project dependencies installed
- Virtual environment activated
- Repository opened in the terminal

Install dependencies from the repository root:

```powershell
python -m pip install -r requirements.txt
```

## 3. Start a Node

Start the application with the default settings:

```powershell
python -m cli.main start
```

Specify a different node port when needed:

```powershell
python -m cli.main start --port 8123
```

Use the CLI help to view available commands and options:

```powershell
python -m cli.main --help
python -m cli.main start --help
```

## 4. Start the Web Dashboard

Start the node and enable the optional web dashboard:

```powershell
python -m cli.main start --host 127.0.0.1 --port 8000 --web --web-port 8765
```

Open the dashboard URL printed in the terminal. The default example is:

`http://127.0.0.1:8765/`

Keep the terminal running while using the dashboard. Press `Ctrl+C` to stop the application.

## 5. Dashboard Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/state` | Retrieves the runtime snapshot |
| GET | `/api/events` | Receives snapshot updates through Server-Sent Events |
| POST | `/api/submit` | Submits an approved demo task |

Task submission requires the `X-Token` header. Use the token printed by the CLI and do not share it publicly.

Only `add` and `multiply` are approved demo task names. Request bodies are limited to 4096 bytes.

## 6. Run Tests

Run all tests:

```powershell
python -m pytest -q
```

Run only the web dashboard tests:

```powershell
python -m pytest tests/test_web.py -q
```

Check lint and formatting:

```powershell
python -m ruff check .
python -m black --check .
```

## 7. Security and Limitations

The dashboard binds to the loopback address by default and is intended as an optional local interface. Keep the submission token private.

A successful web submission does not necessarily mean a task was executed on a remote peer. Use the networking and routing tests to verify distributed behavior separately.

The web dashboard complements the CLI and does not replace it.
