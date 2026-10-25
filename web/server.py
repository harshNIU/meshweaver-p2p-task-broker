"""Local HTTP server for the live MeshWeaver dashboard."""
from __future__ import annotations

import json
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
MAX_REQUEST_BYTES = 64 * 1024


def create_dashboard_server(
    snapshot_provider: Callable[[], dict[str, Any]],
    host: str = HOST,
    port: int = PORT,
    task_submitter: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> ThreadingHTTPServer:
    """Create a dashboard backed by live runtime state and optional task submission."""

    class DashboardHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def send_body(
            self, status: int, content_type: str, body: bytes
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode("utf-8")
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
            if urlsplit(self.path).path != "/api/tasks":
                self.send_json_error(404, "Unknown endpoint.")
                return

            if task_submitter is None:
                self.send_json_error(
                    503, "Task submission is not enabled for this runtime."
                )
                return

            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self.send_json_error(400, "Invalid Content-Length.")
                return

            if length <= 0 or length > MAX_REQUEST_BYTES:
                self.send_json_error(413, "Request body must be 1–65536 bytes.")
                return

            if "application/json" not in self.headers.get("Content-Type", ""):
                self.send_json_error(415, "Content-Type must be application/json.")
                return

            try:
                payload = json.loads(self.rfile.read(length))
            except (json.JSONDecodeError, UnicodeDecodeError):
                self.send_json_error(400, "Request body must contain valid JSON.")
                return

            if not isinstance(payload, dict):
                self.send_json_error(400, "JSON body must be an object.")
                return

            try:
                result = task_submitter(payload)
            except (ValueError, TypeError) as exc:
                self.send_json_error(400, str(exc))
                return
            except Exception as exc:  # noqa: BLE001
                self.send_json_error(
                    500, f"Task submission failed: {type(exc).__name__}"
                )
                return

            self.send_json(200, result)

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
                    payload = json.dumps(state, separators=(",", ":"))
                    self.wfile.write(f"data: {payload}\n\n".encode())
                    self.wfile.flush()
                    time.sleep(SSE_INTERVAL_SECONDS)
            except (
                BrokenPipeError,
                ConnectionResetError,
                ConnectionAbortedError,
            ):
                pass
            except OSError:
                pass

        def log_message(self, format_string: str, *args: object) -> None:
            super().log_message(format_string, *args)

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    server.daemon_threads = True
    thread = threading.Thread(
        target=server.serve_forever,
        name="meshweaver-dashboard",
        daemon=True,
    )
    thread.start()
    return server


def main() -> None:
    """Standalone server entry point is intentionally disabled without runtime."""
    raise SystemExit(
        "Start the live dashboard with: python -m cli.main start --web"
    )


if __name__ == "__main__":
    main()
