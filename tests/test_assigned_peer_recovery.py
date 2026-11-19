import time
import unittest
from unittest.mock import AsyncMock

from execution.models import Task, TaskResult, TaskStatus
from gossip.state import NodeInfo
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.recovery import TaskRecoveryCoordinator


def make_node(node_id):
    return NodeInfo(
        node_id,
        "127.0.0.1",
        9000,
        cpu=10,
        ram=10,
        last_seen=time.time(),
    )


class AssignedPeerRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dead = make_node("node-dead")
        self.replacement = make_node("node-replacement")
        self.lifecycle = TaskLifecycle()
        self.submit = AsyncMock()

        self.coordinator = TaskRecoveryCoordinator(
            self.lifecycle,
            snapshot=lambda: [self.dead, self.replacement],
            submit=self.submit,
        )

    def create_assigned_task(self):
        task = Task(
            "calculate",
            args=(2, 3),
            task_id="assigned-peer-recovery-1",
        )

        self.lifecycle.create(task.task_id, task.task_name)
        self.lifecycle.transition(
            task.task_id,
            TaskState.ASSIGNED,
            node=self.dead.node_id,
        )

        self.coordinator._tasks[task.task_id] = task
        return task

    async def test_assigned_task_recovers_after_worker_failure(self):
        task = self.create_assigned_task()

        self.submit.return_value = TaskResult(
            task.task_id,
            TaskStatus.COMPLETED,
            value=5,
            attempts=1,
        )

        results = await self.coordinator.recover_node(self.dead.node_id)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, TaskStatus.COMPLETED)
        self.submit.assert_awaited_once_with(self.replacement, task)

        record = self.lifecycle.get(task.task_id)
        self.assertEqual(record.state, TaskState.DONE)
        self.assertEqual(record.node, self.replacement.node_id)

    async def test_assigned_task_remains_assigned_without_replacement(self):
        task = self.create_assigned_task()
        self.coordinator.snapshot = lambda: [self.dead]

        results = await self.coordinator.recover_node(self.dead.node_id)

        self.assertEqual(results, [])
        self.submit.assert_not_awaited()

        record = self.lifecycle.get(task.task_id)
        self.assertEqual(record.state, TaskState.ASSIGNED)
        self.assertEqual(record.node, self.dead.node_id)


if __name__ == "__main__":
    unittest.main()
