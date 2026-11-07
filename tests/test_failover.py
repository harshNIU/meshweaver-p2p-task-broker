"""Tests for task failure handling when a node dies."""

import asyncio
import multiprocessing
import time

import pytest

from execution.engine import ExecutionEngine
from tasks.failover import mark_running_tasks_failed
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.transport import TaskServer, send_task

_FAILOVER_STARTED_EVENT = None


def _blocking_failover_task(a, b):
    """Signal that execution started, then keep running until process death."""
    _FAILOVER_STARTED_EVENT.set()
    while True:
        time.sleep(0.05)


def _failover_worker_process(ready, started):
    """Start an independent worker process for the failover test."""
    global _FAILOVER_STARTED_EVENT
    _FAILOVER_STARTED_EVENT = started
    asyncio.run(_serve_failover_worker(ready))


async def _serve_failover_worker(ready):
    engine = ExecutionEngine(max_workers=1)
    engine.register("calculate", _blocking_failover_task)
    server = await TaskServer(engine, workers=1).start()
    try:
        ready.send(server.address)
        await asyncio.Event().wait()
    finally:
        await server.close()
        engine.shutdown()
        ready.close()


def make_running_task(
    lifecycle: TaskLifecycle,
    task_id: str,
    node_id: str,
) -> None:
    """Create a task and move it to RUNNING on a node."""
    lifecycle.create(task_id, "demo_task")
    lifecycle.transition(task_id, TaskState.ASSIGNED, node=node_id)
    lifecycle.transition(task_id, TaskState.RUNNING, node=node_id)


def test_dead_node_tasks_are_marked_failed():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-1", "node-dead")
    make_running_task(lifecycle, "task-2", "node-dead")

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == ["task-1", "task-2"]
    assert lifecycle.get("task-1").state == TaskState.FAILED
    assert lifecycle.get("task-2").state == TaskState.FAILED


def test_tasks_on_other_nodes_remain_running():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-dead", "node-dead")
    make_running_task(lifecycle, "task-alive", "node-alive")

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == ["task-dead"]
    assert lifecycle.get("task-alive").state == TaskState.RUNNING


def test_assigned_tasks_on_dead_node_are_not_changed():
    lifecycle = TaskLifecycle()
    lifecycle.create("task-assigned", "demo_task")
    lifecycle.transition(
        "task-assigned",
        TaskState.ASSIGNED,
        node="node-dead",
    )

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == []
    assert lifecycle.get("task-assigned").state == TaskState.ASSIGNED


def test_completed_tasks_on_dead_node_remain_done():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-done", "node-dead")
    lifecycle.transition("task-done", TaskState.DONE)

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == []
    assert lifecycle.get("task-done").state == TaskState.DONE


def test_repeated_dead_node_notification_is_safe():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-1", "node-dead")

    first = mark_running_tasks_failed(lifecycle, "node-dead")
    second = mark_running_tasks_failed(lifecycle, "node-dead")

    assert first == ["task-1"]
    assert second == []
    assert lifecycle.get("task-1").state == TaskState.FAILED


def test_unknown_node_returns_no_failed_tasks():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "task-1", "node-alive")

    failed = mark_running_tasks_failed(lifecycle, "unknown-node")

    assert failed == []
    assert lifecycle.get("task-1").state == TaskState.RUNNING


@pytest.mark.parametrize("node_id", ["", "   "])
def test_empty_dead_node_id_is_rejected(node_id):
    lifecycle = TaskLifecycle()

    with pytest.raises(ValueError, match="dead_node_id"):
        mark_running_tasks_failed(lifecycle, node_id)


def test_dead_node_recovery_candidates_preserve_task_identity():
    lifecycle = TaskLifecycle()
    make_running_task(lifecycle, "recover-1", "node-dead")
    make_running_task(lifecycle, "keep-1", "node-alive")

    failed = mark_running_tasks_failed(lifecycle, "node-dead")

    assert failed == ["recover-1"]
    assert lifecycle.get("recover-1").task_id == "recover-1"
    assert lifecycle.get("recover-1").state == TaskState.FAILED
    assert lifecycle.get("keep-1").state == TaskState.RUNNING


def test_running_task_completes_after_dead_worker_is_replaced():
    from execution.models import Task, TaskStatus
    from gossip.state import NodeInfo
    from tasks.recovery import TaskRecoveryCoordinator

    async def scenario():
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe(duplex=False)
        started = context.Event()

        dead_process = context.Process(
            target=_failover_worker_process,
            args=(child, started),
            name="meshweaver-failover-worker",
        )

        replacement_engine = ExecutionEngine(max_workers=1)
        replacement_engine.register("calculate", lambda a, b: a + b)

        replacement_server = None

        try:
            dead_process.start()
            child.close()

            if not parent.poll(15):
                raise TimeoutError("Original worker did not start")

            dead_host, dead_port = parent.recv()

            replacement_server = await TaskServer(
                replacement_engine,
                workers=1,
            ).start()

            replacement_host, replacement_port = replacement_server.address

            dead_node = NodeInfo(
                "worker-dead",
                dead_host,
                dead_port,
                cpu=10,
                ram=10,
                last_seen=time.time(),
            )

            replacement_node = NodeInfo(
                "worker-replacement",
                replacement_host,
                replacement_port,
                cpu=20,
                ram=20,
                last_seen=time.time(),
            )

            lifecycle = TaskLifecycle()
            coordinator = TaskRecoveryCoordinator(
                lifecycle,
                snapshot=lambda: [dead_node, replacement_node],
                submit=send_task,
            )

            task = Task(
                task_name="calculate",
                args=(20, 22),
                task_id="failover-killed-worker-001",
            )

            coordinator.register(task, dead_node.node_id)

            original_submission = asyncio.create_task(send_task(dead_node, task))

            task_started = await asyncio.to_thread(started.wait, 15)
            assert task_started, "Original worker never started the task"

            dead_process.terminate()
            await asyncio.to_thread(dead_process.join, 5)

            assert not dead_process.is_alive()

            await asyncio.gather(
                original_submission,
                return_exceptions=True,
            )

            results = await coordinator.recover_node(dead_node.node_id)

            assert len(results) == 1
            assert results[0].task_id == task.task_id
            assert results[0].status == TaskStatus.COMPLETED
            assert results[0].value == 42

            record = lifecycle.get(task.task_id)
            assert record.state == TaskState.DONE
            assert record.node == replacement_node.node_id

        finally:
            if dead_process.is_alive():
                dead_process.terminate()

            await asyncio.to_thread(dead_process.join, 5)
            parent.close()

            if replacement_server is not None:
                await replacement_server.close()

            replacement_engine.shutdown()

    asyncio.run(scenario())
