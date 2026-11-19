import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from execution.models import Task, TaskResult, TaskStatus
from gossip.state import NodeInfo
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.recovery import TaskRecoveryCoordinator
from tasks.store import SQLiteTaskStore


def make_node(node_id: str) -> NodeInfo:
    return NodeInfo(
        node_id,
        "127.0.0.1",
        9000,
        cpu=10,
        ram=10,
        last_seen=time.time(),
    )


class PendingRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_pending_task_is_recovered_after_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SQLiteTaskStore(Path(temp_dir) / "meshweaver.db")

            task = Task(
                "calculate",
                args=(2, 3),
                task_id="pending-recovery-1",
            )

            store.save_task(task)

            lifecycle = TaskLifecycle(store=store)
            replacement = make_node("node-replacement")

            submit = AsyncMock(
                return_value=TaskResult(
                    task.task_id,
                    TaskStatus.COMPLETED,
                    value=5,
                    attempts=1,
                )
            )

            coordinator = TaskRecoveryCoordinator(
                lifecycle,
                snapshot=lambda: [replacement],
                submit=submit,
                store=store,
            )

            results = await coordinator.recover_interrupted()

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].status, TaskStatus.COMPLETED)
            submit.assert_awaited_once_with(replacement, task)

            record = lifecycle.get(task.task_id)
            self.assertEqual(record.state, TaskState.DONE)
            self.assertEqual(record.node, replacement.node_id)

    async def test_pending_task_stays_pending_without_available_worker(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SQLiteTaskStore(Path(temp_dir) / "meshweaver.db")

            task = Task(
                "calculate",
                args=(4, 6),
                task_id="pending-no-worker-1",
            )

            store.save_task(task)

            lifecycle = TaskLifecycle(store=store)
            submit = AsyncMock()

            coordinator = TaskRecoveryCoordinator(
                lifecycle,
                snapshot=list,
                submit=submit,
                store=store,
            )

            results = await coordinator.recover_interrupted()

            self.assertEqual(results, [])
            submit.assert_not_awaited()

            record = lifecycle.get(task.task_id)
            self.assertEqual(record.state, TaskState.PENDING)
            self.assertEqual(record.node, "-")


if __name__ == "__main__":
    unittest.main()
