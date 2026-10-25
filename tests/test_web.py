"""Tests for the live MeshWeaver dashboard endpoints."""

import json
import urllib.error
import urllib.request

from web.server import create_dashboard_server


def fetch(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        return response.status, response.headers, response.read()


def test_live_state_endpoint_returns_snapshot():
    snapshot = {
        "mode": "live",
        "nodes": [{"node_id": "real-node", "state": "ALIVE"}],
        "tasks": [],
        "events": [],
    }
    server = create_dashboard_server(lambda: snapshot, port=0)
    try:
        host, port = server.server_address
        status, headers, body = fetch(f"http://{host}:{port}/api/state")
        assert status == 200
        assert headers["Content-Type"].startswith("application/json")
        assert json.loads(body)["nodes"][0]["node_id"] == "real-node"
        assert json.loads(body)["mode"] == "live"
    finally:
        server.shutdown()
        server.server_close()


def test_unknown_path_returns_404():
    server = create_dashboard_server(dict, port=0)
    try:
        host, port = server.server_address
        try:
            fetch(f"http://{host}:{port}/not-a-route")
        except urllib.error.HTTPError as error:
            assert error.code == 404
        else:
            raise AssertionError("Expected HTTP 404")
    finally:
        server.shutdown()
        server.server_close()


def test_snapshot_provider_failure_returns_500():
    def broken_provider():
        raise RuntimeError("runtime not available")

    server = create_dashboard_server(broken_provider, port=0)
    try:
        host, port = server.server_address
        try:
            fetch(f"http://{host}:{port}/api/state")
        except urllib.error.HTTPError as error:
            assert error.code == 500
            payload = json.loads(error.read())
            assert "Live snapshot unavailable" in payload["error"]
        else:
            raise AssertionError("Expected HTTP 500")
    finally:
        server.shutdown()
        server.server_close()
