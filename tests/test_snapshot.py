"""Regression tests for dashboard task snapshot normalization."""

import pytest

from web.snapshot import _normalise_task


def test_normalise_task_uses_dashboard_field_names():
    result = _normalise_task(
        {
            "task_id": "task-123",
            "name": "calculate",
            "status": "RUNNING",
            "assigned_to": "node-1",
            "attempt": 2,
        }
    )

    assert result == {
        "task_id": "task-123",
        "name": "calculate",
        "state": "RUNNING",
        "node": "node-1",
        "attempts": 2,
    }


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("COMPLETED", "DONE"),
        ("CANCELLED", "FAILED"),
        ("OUTCOME_UNCERTAIN", "OUTCOME_UNCERTAIN"),
        ("FAILED", "FAILED"),
        ("RUNNING", "RUNNING"),
    ],
)
def test_normalise_task_preserves_expected_status(status, expected):
    result = _normalise_task({"status": status})

    assert result["state"] == expected


def test_normalise_task_supports_existing_normalized_fields():
    result = _normalise_task(
        {
            "task_id": "task-456",
            "name": "multiply",
            "state": "ASSIGNED",
            "node": "node-2",
            "attempts": 3,
        }
    )

    assert result["node"] == "node-2"
    assert result["attempts"] == 3
    assert result["state"] == "ASSIGNED"
