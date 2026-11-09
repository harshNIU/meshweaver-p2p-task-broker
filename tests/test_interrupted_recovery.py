"""Regression tests for recovery after coordinator interruption."""

import time
import unittest
from unittest.mock import AsyncMock

from execution.models import Task, TaskResult, TaskStatus
from gossip.state import NodeInfo
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.recovery import TaskRecoveryCoordinator


def make_node(node_id: str, cpu: float = 10) -> NodeInfo:
    return NodeInfo(
        node_id,
        "127.0.0.1",
        9000,
        cpu=cpu,
        ram=10,
        last_seen=time.time(),
    )


class InterruptedRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_node = make_node("node-old", cpu=5)
        self.new_node = make_node("node-new", cpu=10)
        self.lifecycle = TaskLifecycle()
        self.submit = AsyncMock()
        self.task = Task("calculate", args=(2, 3), task_id="interrupted-1")

        self.coordinator = TaskRecoveryCoordinator(
            self.lifecycle,
            snapshot=lambda: [self.old_node, self.new_node],
            submit=self.submit,
        )

        self.coordinator.register(self.task, "node-original")

        # Simulate a crash after a task was durably reassigned but
        # before the replacement submission completed.
        self.lifecycle.reassign_running(
            self.task.task_id,
            "node-original",
            "node-old",
        )

    async def test_interrupted_assigned_task_is_recovered(self):
        self.submit.return_value = TaskResult(
            self.task.task_id,
            TaskStatus.COMPLETED,
            value=5,
            attempts=2,
        )

        results = await self.coordinator.recover_interrupted()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, TaskStatus.COMPLETED)
        self.submit.assert_awaited_once_with(self.new_node, self.task)

        record = self.lifecycle.get(self.task.task_id)
        self.assertEqual(record.state, TaskState.DONE)
        self.assertEqual(record.node, "node-new")

    async def test_no_replacement_keeps_task_assigned(self):
        self.coordinator.snapshot = lambda: [self.old_node]

        results = await self.coordinator.recover_interrupted()

        self.assertEqual(results, [])
        self.submit.assert_not_awaited()

        record = self.lifecycle.get(self.task.task_id)
        self.assertEqual(record.state, TaskState.ASSIGNED)
        self.assertEqual(record.node, "node-old")

    async def test_unexpected_submission_exception_marks_task_failed(self):
        self.submit.side_effect = RuntimeError("unexpected worker failure")

        results = await self.coordinator.recover_interrupted()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, TaskStatus.FAILED)
        self.assertIn("RuntimeError", results[0].error)

        record = self.lifecycle.get(self.task.task_id)
        self.assertEqual(record.state, TaskState.FAILED)
