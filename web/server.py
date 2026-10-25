"""Local HTTP server for the live MeshWeaver dashboard."""

from __future__ import annotations

import hmac
import json
import math
import secrets
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

WEB_DIR = Path(__file__).resolve().parent
INDEX_FILE = WEB_DIR / "static" / "index.html"
HOST = "127.0.0.1"
PORT = 8765
SSE_INTERVAL_SECONDS = 2
MAX_REQUEST_BYTES = 4096
ALLOWED_TASKS = frozenset({"add", "multiply"})


def _reject_json_constant(value: str) -> None:
    """Reject non-standard JSON constants such as NaN and Infinity."""
    raise ValueError(f"Invalid JSON constant: {value}")


def _validate_json_value(value: Any) -> bool:
    """Allow only finite JSON-compatible values."""
    if value is None or isinstance(value, (str, bool, int)):
        return True

    if isinstance(value, float):
        return math.isfinite(value)

    if isinstance(value, list):
        return all(_validate_json_value(item) for item in value)

    if isinstance(value, dict):
        return all(
            isinstance(key, str) and _validate_json_value(item)
            for key, item in value.items()
        )

    return False


def create_dashboard_server(
    snapshot_provider: Callable[[], dict[str, Any]],
    host: str = HOST,
    port: int = PORT,
    task_submitter: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    *,
    submit_token: str | None = None,
) -> ThreadingHTTPServer:
    """Create a dashboard with optional token-protected demo task submission."""
    token = submit_token if submit_token is not None else secrets.token_urlsafe(32)

    if not isinstance(token, str) or not token:
        raise ValueError("submit_token must be a non-empty string")

    class DashboardHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def send_body(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, allow_nan=False).encode("utf-8")
            self.send_body(status, "application/json; charset=utf-8", body)

        def send_json_error(self, status: int, message: str) -> None:
            self.send_json(status, {"error": message})

        def do_GET(self) -> None:
            path = urlsplit(self.path).path

            if path in ("/", "/index.html", "/static/index.html"):
                try:
                    body = INDEX_FILE.read_bytes()
                except OSError:
                    self.send_body(
                        500,
                        "text/plain; charset=utf-8",
                        b"Dashboard page could not be loaded",
                    )
                    return

                self.send_body(200, "text/html; charset=utf-8", body)
                return

            if path == "/api/state":
                try:
                    self.send_json(200, snapshot_provider())
                except Exception as exc:  # noqa: BLE001
                    self.send_json_error(
                        500, f"Live snapshot unavailable: {type(exc).__name__}"
                    )
                return

            if path == "/api/events":
                self.handle_event_stream()
                return

            self.send_body(404, "text/plain; charset=utf-8", b"404 Not Found")

        def do_POST(self) -> None:
            if urlsplit(self.path).path != "/api/submit":
                self.send_json_error(404, "Unknown endpoint.")
                return

            if task_submitter is None:
                self.send_json_error(
                    503, "Task submission is not enabled for this runtime."
                )
                return

            supplied_token = self.headers.get("X-Token", "")
            if not hmac.compare_digest(supplied_token, token):
                self.send_json_error(403, "Invalid or missing submission token.")
                return

            content_lengths = self.headers.get_all("Content-Length", [])
            if len(content_lengths) != 1:
                self.send_json_error(400, "Exactly one Content-Length is required.")
                return

            raw_length = content_lengths[0]
            if not raw_length.isascii() or not raw_length.isdecimal():
                self.send_json_error(400, "Invalid Content-Length.")
                return

            length = int(raw_length)
            if length <= 0:
                self.send_json_error(400, "Request body must not be empty.")
                return

            if length > MAX_REQUEST_BYTES:
                self.send_json_error(413, "Request body must not exceed 4096 bytes.")
                return

            content_type = self.headers.get("Content-Type", "")
            media_type = content_type.split(";", 1)[0].strip().lower()
            if media_type != "application/json":
                self.send_json_error(415, "Content-Type must be application/json.")
                return

            try:
                raw_body = self.rfile.read(length)
                if len(raw_body) != length:
                    self.send_json_error(400, "Incomplete request body.")
                    return

                payload = json.loads(
                    raw_body.decode("utf-8"),
                    parse_constant=_reject_json_constant,
                )
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                self.send_json_error(400, "Request body must contain valid JSON.")
                return

            if not isinstance(payload, dict):
                self.send_json_error(400, "JSON body must be an object.")
                return

            task_name = payload.get("name")
            if not isinstance(task_name, str) or task_name not in ALLOWED_TASKS:
                self.send_json_error(
                    400, "Task name must be one of the approved demo tasks."
                )
                return

            args = payload.get("args", [])
            if not isinstance(args, list):
                self.send_json_error(400, "'args' must be a JSON array.")
                return

            kwargs = payload.get("kwargs", {})
            if not isinstance(kwargs, dict):
                self.send_json_error(400, "'kwargs' must be a JSON object.")
                return

            if not all(isinstance(key, str) for key in kwargs):
                self.send_json_error(400, "'kwargs' keys must be strings.")
                return

            if not _validate_json_value(args) or not _validate_json_value(kwargs):
                self.send_json_error(
                    400, "Task arguments must contain finite JSON values only."
                )
                return

            safe_payload = {
                "name": task_name,
                "args": args,
                "kwargs": kwargs,
            }

            try:
                result = task_submitter(safe_payload)
            except (ValueError, TypeError) as exc:
                self.send_json_error(400, str(exc))
                return
            except Exception as exc:  # noqa: BLE001
                self.send_json_error(
                    500, f"Task submission failed: {type(exc).__name__}"
                )
                return

            self.send_json(202, result)

        def handle_event_stream(self) -> None:
            try:
                snapshot_provider()
            except Exception as exc:  # noqa: BLE001
                self.send_json_error(
                    500, f"Live snapshot unavailable: {type(exc).__name__}"
                )
                return

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            try:
                while True:
                    state = snapshot_provider()
                    payload = json.dumps(state, separators=(",", ":"), allow_nan=False)
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    time.sleep(SSE_INTERVAL_SECONDS)
            except (
                BrokenPipeError,
                ConnectionResetError,
                ConnectionAbortedError,
                OSError,
            ):
                pass

        def log_message(self, format_string: str, *args: object) -> None:
            super().log_message(format_string, *args)

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    server.daemon_threads = True
    server.submit_token = token

    thread = threading.Thread(
        target=server.serve_forever,
        name="meshweaver-dashboard",
        daemon=True,
    )
    thread.start()
    return server


def main() -> None:
    """Standalone server entry point is intentionally disabled without runtime."""
    raise SystemExit("Start the live dashboard with: python -m cli.main start --web")


if __name__ == "__main__":
    main()
