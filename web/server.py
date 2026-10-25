"""Local HTTP server for the MeshWeaver mock web dashboard.

This first version serves a simple page and a read-only mock-state API.
It uses only Python's standard library.
"""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

WEB_DIR = Path(__file__).resolve().parent
STATE_FILE = WEB_DIR / "mock_state.json"
HOST = "127.0.0.1"
PORT = 8765

STARTER_PAGE = """<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>MeshWeaver Dashboard</title>
    <style>
        body {
            max-width: 900px;
            margin: 40px auto;
            padding: 0 20px;
            font-family: Arial, sans-serif;
            background: #f4f7fb;
            color: #172b4d;
        }
        main {
            padding: 24px;
            background: white;
            border-radius: 12px;
        }
        h1 { margin-top: 0; }
        .notice { color: #775600; }
        button {
            padding: 10px 16px;
            border: 0;
            border-radius: 6px;
            background: #175cd3;
            color: white;
            cursor: pointer;
        }
        pre {
            padding: 16px;
            overflow-x: auto;
            background: #edf2f7;
            border-radius: 8px;
        }
    </style>
</head>
<body>
    <main>
        <h1>MeshWeaver Dashboard</h1>
        <p class="notice">
            WEB 1 starter page — the information below is mock data,
            not live peer information.
        </p>
        <p>State API: <code>/api/state</code></p>
        <button id="refresh">Refresh mock state</button>
        <p id="status">Loading state...</p>
        <pre id="state">Waiting for data...</pre>
    </main>
    <script>
        const statusElement = document.getElementById("status");
        const stateElement = document.getElementById("state");
        const refreshButton = document.getElementById("refresh");

        async function loadState() {
            statusElement.textContent = "Loading mock state...";

            try {
                const response = await fetch("/api/state", {
                    cache: "no-store"
                });

                if (!response.ok) {
                    throw new Error("HTTP status " + response.status);
                }

                const state = await response.json();
                stateElement.textContent = JSON.stringify(state, null, 2);
                statusElement.textContent =
                    "Loaded successfully. Data mode: " + state.mode;
            } catch (error) {
                statusElement.textContent =
                    "Could not load state: " + error.message;
            }
        }

        refreshButton.addEventListener("click", loadState);
        loadState();
    </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    """Handle the dashboard page and read-only state endpoint."""

    def send_body(self, status, content_type, body):
        """Send a complete HTTP response."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        """Serve only the dashboard page and mock-state endpoint."""
        path = urlsplit(self.path).path

        if path in ("/", "/index.html"):
            body = STARTER_PAGE.encode("utf-8")
            self.send_body(
                200,
                "text/html; charset=utf-8",
                body,
            )
            return

        if path == "/api/state":
            try:
                with STATE_FILE.open("r", encoding="utf-8") as state_file:
                    state = json.load(state_file)

                body = json.dumps(state, indent=2).encode("utf-8")
                self.send_body(
                    200,
                    "application/json; charset=utf-8",
                    body,
                )
            except (OSError, json.JSONDecodeError):
                error_body = json.dumps(
                    {"error": "Mock state could not be loaded"}
                ).encode("utf-8")
                self.send_body(
                    500,
                    "application/json; charset=utf-8",
                    error_body,
                )
            return

        body = b"404 Not Found"
        self.send_body(404, "text/plain; charset=utf-8", body)

    def log_message(self, format_string, *args):
        """Keep standard HTTP request logging enabled."""
        super().log_message(format_string, *args)


def main():
    """Start the local dashboard server."""
    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)

    print(f"MeshWeaver dashboard: http://{HOST}:{PORT}/")
    print(f"Mock state API:       http://{HOST}:{PORT}/api/state")
    print("Data source: mock_state.json")
    print("Press Ctrl+C to stop the server.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping MeshWeaver dashboard...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
