"""Tests for task recovery coordination."""

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


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dead = make_node("node-dead", cpu=1)
        self.replacement = make_node("node-replacement", cpu=20)
        self.lifecycle = TaskLifecycle()
        self.submit = AsyncMock()
        self.coordinator = TaskRecoveryCoordinator(
            self.lifecycle,
            snapshot=lambda: [self.dead, self.replacement],
            submit=self.submit,
        )

    def register_task(self, task_id="recover-1"):
        task = Task("calculate", args=(2, 3), task_id=task_id)
        self.coordinator.register(task, "node-dead")
        return task

    async def test_recovery_submits_original_task_to_replacement(self):
        task = self.register_task()
        self.submit.return_value = TaskResult(
            task.task_id,
            TaskStatus.COMPLETED,
            value=5,
            attempts=1,
        )

        results = await self.coordinator.recover_node("node-dead")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, TaskStatus.COMPLETED)
        self.submit.assert_awaited_once_with(self.replacement, task)

        record = self.lifecycle.get(task.task_id)
        self.assertEqual(record.node, "node-replacement")
        self.assertEqual(record.state, TaskState.DONE)
        self.assertEqual(record.attempts, 2)

    async def test_no_replacement_leaves_task_running(self):
        task = self.register_task()
        self.coordinator.snapshot = lambda: [self.dead]

        results = await self.coordinator.recover_node("node-dead")

        self.assertEqual(results, [])
        self.submit.assert_not_awaited()

        record = self.lifecycle.get(task.task_id)
        self.assertEqual(record.node, "node-dead")
        self.assertEqual(record.state, TaskState.RUNNING)

    async def test_submission_exception_marks_reserved_task_failed(self):
        task = self.register_task()
        self.submit.side_effect = ConnectionError("replacement offline")

        results = await self.coordinator.recover_node("node-dead")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, TaskStatus.FAILED)
        self.assertIn("outcome uncertain", results[0].error)

        record = self.lifecycle.get(task.task_id)
        self.assertEqual(record.state, TaskState.FAILED)
        self.assertEqual(record.node, "node-replacement")

    async def test_mismatched_task_result_is_rejected(self):
        task = self.register_task()
        self.submit.return_value = TaskResult(
            "different-task",
            TaskStatus.COMPLETED,
        )

        results = await self.coordinator.recover_node("node-dead")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, TaskStatus.FAILED)
        self.assertEqual(
            self.lifecycle.get(task.task_id).state,
            TaskState.FAILED,
        )

    async def test_application_failure_is_terminal(self):
        task = self.register_task()
        self.submit.return_value = TaskResult(
            task.task_id,
            TaskStatus.FAILED,
            error="calculation failed",
        )

        results = await self.coordinator.recover_node("node-dead")

        self.assertEqual(results[0].status, TaskStatus.FAILED)

        record = self.lifecycle.get(task.task_id)
        self.assertEqual(record.state, TaskState.FAILED)
        self.assertEqual(record.node, "node-replacement")

    async def test_repeated_recovery_does_not_submit_twice(self):
        task = self.register_task()
        self.submit.return_value = TaskResult(
            task.task_id,
            TaskStatus.COMPLETED,
            value=5,
        )

        first = await self.coordinator.recover_node("node-dead")
        second = await self.coordinator.recover_node("node-dead")

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.submit.assert_awaited_once()

    async def test_empty_dead_node_id_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "dead_node_id"):
            await self.coordinator.recover_node("")


if __name__ == "__main__":
    unittest.main()
