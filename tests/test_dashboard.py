"""Tests for the live Rich dashboard."""

from io import StringIO

from rich.console import Console

from cli.dashboard import _percentage, create_dashboard


def render_dashboard(snapshot):
    """Render a dashboard to text for assertions."""
    output = StringIO()
    test_console = Console(file=output, width=240)
    test_console.print(create_dashboard(snapshot))
    return output.getvalue()


def test_dashboard_displays_empty_state():
    output = render_dashboard({})

    assert "Waiting for live node" in output
    assert "No live nodes" in output
    assert "No tasks" in output


def test_dashboard_displays_live_node_topology_and_metrics():
    snapshot = {
        "self": "node-1",
        "nodes": [
            {
                "node_id": "node-1",
                "host": "127.0.0.1",
                "port": 8000,
                "state": "ALIVE",
                "cpu": 25.5,
                "ram": 63.2,
            },
            {
                "node_id": "node-2",
                "host": "127.0.0.2",
                "port": 8001,
                "state": "SUSPECT",
                "cpu": 40.0,
                "ram": 72.5,
            },
        ],
        "tasks": [
            {
                "task_id": "task-1",
                "name": "add",
                "node": "node-1",
                "state": "COMPLETED",
            }
        ],
    }

    output = render_dashboard(snapshot)

    assert "node-1" in output
    assert "node-2" in output
    assert "127.0.0.1:8000" in output
    assert "127.0.0.2:8001" in output
    assert "ALIVE" in output
    assert "SUSPECT" in output
    assert "25.5%" in output
    assert "63.2%" in output
    assert "40.0%" in output
    assert "72.5%" in output
    assert "task-1" in output
    assert "COMPLETED" in output


def test_percentage_handles_invalid_values():
    assert _percentage(25.5) == "25.5%"
    assert _percentage(None) == "N/A"
    assert _percentage("invalid") == "N/A"


def test_dashboard_displays_task_state_and_retry_count():
    snapshot = {
        "self": "node-1",
        "nodes": [],
        "tasks": [
            {
                "task_id": "task-retried",
                "name": "add",
                "node": "node-1",
                "state": "RUNNING",
                "attempts": 3,
            },
            {
                "task_id": "task-new",
                "name": "multiply",
                "node": "node-2",
                "state": "PENDING",
                "attempts": 1,
            },
        ],
    }

    output = render_dashboard(snapshot)

    assert "State" in output
    assert "Retries" in output
    assert "RUNNING" in output
    assert "PENDING" in output
    assert "task-retried" in output
    assert "task-new" in output
    assert "2" in output
    assert "0" in output
