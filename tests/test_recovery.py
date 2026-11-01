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


class RecoveryPersistenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import tempfile
        from pathlib import Path

        from tasks.store import SQLiteTaskStore

        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "meshweaver.db"
        self.store = SQLiteTaskStore(self.db_path)
        self.dead = make_node("node-dead", cpu=1)
        self.replacement = make_node("node-replacement", cpu=20)

    def tearDown(self):
        self.temp_dir.cleanup()

    async def test_registration_persists_task_payload(self):
        lifecycle = TaskLifecycle(store=self.store)
        submit = AsyncMock()
        coordinator = TaskRecoveryCoordinator(
            lifecycle,
            snapshot=lambda: [self.dead, self.replacement],
            submit=submit,
            store=self.store,
        )

        task = Task(
            "calculate",
            args=(2, 3),
            kwargs={"operation": "add"},
            task_id="persistent-task-1",
        )
        coordinator.register(task, "node-dead")

        restored = self.store.get_task(task.task_id)
        self.assertEqual(restored, task)

        record = self.store.get(task.task_id)
        self.assertEqual(record["state"], TaskState.RUNNING.value)
        self.assertEqual(record["node"], "node-dead")

    async def test_new_coordinator_restores_saved_task_payloads(self):
        lifecycle = TaskLifecycle(store=self.store)
        first = TaskRecoveryCoordinator(
            lifecycle,
            snapshot=lambda: [self.dead, self.replacement],
            submit=AsyncMock(),
            store=self.store,
        )
        task = Task(
            "calculate",
            args=(7, 8),
            task_id="persistent-task-2",
        )
        first.register(task, "node-dead")

        restarted_lifecycle = TaskLifecycle(store=self.store)
        restarted_submit = AsyncMock(
            return_value=TaskResult(
                task.task_id,
                TaskStatus.COMPLETED,
                value=15,
                attempts=1,
            )
        )
        restarted = TaskRecoveryCoordinator(
            restarted_lifecycle,
            snapshot=lambda: [self.dead, self.replacement],
            submit=restarted_submit,
            store=self.store,
        )

        results = await restarted.recover_node("node-dead")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].task_id, task.task_id)
        self.assertEqual(results[0].status, TaskStatus.COMPLETED)
        restarted_submit.assert_awaited_once_with(self.replacement, task)

    async def test_registration_payload_failure_does_not_start_task(self):
        from unittest.mock import Mock

        lifecycle = TaskLifecycle()
        store = Mock()
        store.get.return_value = None
        store.list_task_payloads.return_value = []
        store.save_task.side_effect = OSError("database unavailable")

        coordinator = TaskRecoveryCoordinator(
            lifecycle,
            snapshot=lambda: [self.dead, self.replacement],
            submit=AsyncMock(),
            store=store,
        )

        task = Task("calculate", args=(2, 3), task_id="save-failure-task")

        with self.assertRaisesRegex(OSError, "database unavailable"):
            coordinator.register(task, "node-dead")

        record = lifecycle.get(task.task_id)
        self.assertIsNone(record)
