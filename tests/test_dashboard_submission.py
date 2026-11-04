"""Tests for authenticated dashboard task submission."""

import asyncio
import concurrent.futures
import time
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from execution.models import TaskResult, TaskStatus
from execution.task_registry import TaskRegistry
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.recovery import TaskRecoveryCoordinator
from web.submission import create_dashboard_submitter


class FakeFuture:
    def __init__(self):
        self.cancelled = False

    def result(self, timeout=None):
        raise concurrent.futures.TimeoutError()

    def cancel(self):
        self.cancelled = True
        return True


class CompletedFuture:
    """Future-like wrapper for a synchronously completed coroutine."""

    def __init__(self, value):
        self.value = value

    def result(self, timeout=None):
        return self.value

    def cancel(self):
        return False


class FakePeerManager:
    def is_peer_alive(self, peer_id):
        return True

    def get_peer_tcp_address(self, peer_id):
        return ("127.0.0.1", 9001)


@dataclass(frozen=True)
class FakePeer:
    node_id: str
    host: str
    port: int
    state: str
    cpu: float = 10.0
    ram: float = 10.0
    last_seen: float = 0.0


def make_protocol(include_worker=True):
    peers = {}

    if include_worker:
        peers["worker-1"] = FakePeer(
            node_id="worker-1",
            host="127.0.0.1",
            port=9001,
            state="ALIVE",
            cpu=10.0,
            ram=10.0,
            last_seen=time.time(),
        )

    return SimpleNamespace(
        node_registry=peers,
        peer_manager=FakePeerManager(),
    )


def make_recovery_coordinator():
    lifecycle = TaskLifecycle()

    coordinator = TaskRecoveryCoordinator(
        lifecycle=lifecycle,
        snapshot=list,
        submit=lambda peer, task: None,
    )

    return coordinator, lifecycle


def run_submission(coroutine, loop):
    """Run a submitted coroutine and return a Future-like result."""
    return CompletedFuture(asyncio.run(coroutine))


def test_dashboard_timeout_after_worker_contact_is_uncertain():
    registry = TaskRegistry()
    coordinator, lifecycle = make_recovery_coordinator()
    protocol = make_protocol()
    fake_future = FakeFuture()

    async def fake_send_task(peer, task, **kwargs):
        return TaskResult(
            task.task_id,
            TaskStatus.COMPLETED,
            value=5,
        )

    def schedule(coroutine, loop):
        asyncio.run(coroutine)
        return fake_future

    with (
        patch(
            "web.submission.send_task",
            new=AsyncMock(side_effect=fake_send_task),
        ),
        patch(
            "web.submission.asyncio.run_coroutine_threadsafe",
            side_effect=schedule,
        ),
    ):
        submitter = create_dashboard_submitter(
            loop=asyncio.new_event_loop(),
            protocol=protocol,
            task_registry=registry,
            local_node_id="local-node",
            secret=b"test-secret",
            recovery_coordinator=coordinator,
        )

        result = submitter({"name": "add", "args": [2, 3]})

    assert result["state"] == "OUTCOME_UNCERTAIN"
    assert result["node"] == "worker-1"
    assert fake_future.cancelled is True

    dashboard_record = registry.snapshot_tasks()[0]
    assert dashboard_record["status"] == "OUTCOME_UNCERTAIN"

    persistent_record = lifecycle.get(result["task_id"])
    assert persistent_record is not None
    assert persistent_record.state == TaskState.RUNNING


def test_dashboard_timeout_without_worker_contact_is_failed():
    registry = TaskRegistry()
    coordinator, lifecycle = make_recovery_coordinator()
    fake_future = FakeFuture()

    def schedule(coroutine, loop):
        coroutine.close()
        return fake_future

    with patch(
        "web.submission.asyncio.run_coroutine_threadsafe",
        side_effect=schedule,
    ):
        submitter = create_dashboard_submitter(
            loop=asyncio.new_event_loop(),
            protocol=make_protocol(include_worker=False),
            task_registry=registry,
            local_node_id="local-node",
            secret=b"test-secret",
            recovery_coordinator=coordinator,
        )

        with pytest.raises(
            RuntimeError,
            match="before contacting a worker",
        ):
            submitter({"name": "add", "args": [2, 3]})

    record = registry.snapshot_tasks()[0]

    assert record["status"] == "FAILED"
    assert lifecycle.get(record["task_id"]) is None
    assert fake_future.cancelled is True


def test_dashboard_success_updates_dashboard_and_lifecycle():
    registry = TaskRegistry()
    coordinator, lifecycle = make_recovery_coordinator()

    async def fake_send_task(peer, task, **kwargs):
        return TaskResult(
            task.task_id,
            TaskStatus.COMPLETED,
            value=5,
            attempts=1,
        )

    with (
        patch(
            "web.submission.send_task",
            new=AsyncMock(side_effect=fake_send_task),
        ),
        patch(
            "web.submission.asyncio.run_coroutine_threadsafe",
            side_effect=run_submission,
        ),
    ):
        submitter = create_dashboard_submitter(
            loop=asyncio.new_event_loop(),
            protocol=make_protocol(),
            task_registry=registry,
            local_node_id="local-node",
            secret=b"test-secret",
            recovery_coordinator=coordinator,
        )

        result = submitter({"name": "add", "args": [2, 3]})

    assert result["state"] == "COMPLETED"
    assert result["node"] == "worker-1"
    assert result["attempts"] == 1

    dashboard_record = registry.snapshot_tasks()[0]
    assert dashboard_record["status"] == "COMPLETED"

    persistent_record = lifecycle.get(result["task_id"])
    assert persistent_record is not None
    assert persistent_record.state == TaskState.DONE


def test_dashboard_missing_valid_result_is_uncertain():
    registry = TaskRegistry()
    coordinator, lifecycle = make_recovery_coordinator()

    async def failed_send_task(peer, task, **kwargs):
        raise OSError("Connection lost before receiving a result")

    with (
        patch(
            "web.submission.send_task",
            new=AsyncMock(side_effect=failed_send_task),
        ),
        patch(
            "web.submission.asyncio.run_coroutine_threadsafe",
            side_effect=run_submission,
        ),
    ):
        submitter = create_dashboard_submitter(
            loop=asyncio.new_event_loop(),
            protocol=make_protocol(),
            task_registry=registry,
            local_node_id="local-node",
            secret=b"test-secret",
            recovery_coordinator=coordinator,
        )

        result = submitter({"name": "add", "args": [2, 3]})

    assert result["state"] == "OUTCOME_UNCERTAIN"
    assert result["node"] == "worker-1"
    assert result["attempts"] >= 1

    dashboard_record = registry.snapshot_tasks()[0]
    assert dashboard_record["status"] == "OUTCOME_UNCERTAIN"

    persistent_record = lifecycle.get(result["task_id"])
    assert persistent_record is not None
    assert persistent_record.state == TaskState.RUNNING