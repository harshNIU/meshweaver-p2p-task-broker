"""Tests for the live MeshWeaver dashboard endpoints."""

import json
import threading
import urllib.error
import urllib.request

from web.server import create_dashboard_server


def fetch(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        return response.status, response.headers, response.read()


def post_json(url, payload, token=None):
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["X-Token"] = token

    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def run_server(snapshot=None, submitter=None, token="test-token"):
    return create_dashboard_server(
        snapshot_provider=lambda: snapshot
        or {
            "mode": "live",
            "nodes": [],
            "tasks": [],
            "events": [],
        },
        port=0,
        task_submitter=submitter,
        submit_token=token,
    )


def test_live_state_endpoint_returns_snapshot():
    snapshot = {
        "mode": "live",
        "nodes": [{"node_id": "real-node", "state": "ALIVE"}],
        "tasks": [],
        "events": [],
    }
    server = run_server(snapshot=snapshot)
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
    server = run_server()
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


def test_submit_without_token_returns_403():
    server = run_server(submitter=lambda payload: {"task_id": "task-1"})
    try:
        host, port = server.server_address
        status, payload = post_json(
            f"http://{host}:{port}/api/submit",
            {"name": "add", "args": [2, 3]},
        )
        assert status == 403
        assert "token" in payload["error"].lower()
    finally:
        server.shutdown()
        server.server_close()


def test_submit_with_invalid_token_returns_403():
    server = run_server(submitter=lambda payload: {"task_id": "task-1"})
    try:
        host, port = server.server_address
        status, _ = post_json(
            f"http://{host}:{port}/api/submit",
            {"name": "add", "args": [2, 3]},
            token="wrong-token",
        )
        assert status == 403
    finally:
        server.shutdown()
        server.server_close()


def test_submit_rejects_task_outside_allowlist():
    called = threading.Event()

    def submitter(payload):
        called.set()
        return {"task_id": "task-1"}

    server = run_server(submitter=submitter)
    try:
        host, port = server.server_address
        status, _ = post_json(
            f"http://{host}:{port}/api/submit",
            {"name": "arbitrary_code", "args": []},
            token="test-token",
        )
        assert status == 400
        assert not called.is_set()
    finally:
        server.shutdown()
        server.server_close()


def test_submit_accepts_allowlisted_task():
    received = []

    def submitter(payload):
        received.append(payload)
        return {"task_id": "task-123", "state": "PENDING"}

    server = run_server(submitter=submitter)
    try:
        host, port = server.server_address
        status, payload = post_json(
            f"http://{host}:{port}/api/submit",
            {"name": "add", "args": [2, 3]},
            token="test-token",
        )
        assert status == 202
        assert payload["task_id"] == "task-123"
        assert received == [{"name": "add", "args": [2, 3], "kwargs": {}}]
    finally:
        server.shutdown()
        server.server_close()
