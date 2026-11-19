import asyncio
import time
import unittest

from execution.models import Task, TaskResult, TaskStatus
from gossip.state import NodeInfo
from tasks.lifecycle import TaskLifecycle
from tasks.recovery import TaskRecoveryCoordinator
from tasks.store import SQLiteTaskStore


def make_node(node_id, cpu):
    return NodeInfo(
        node_id,
        "127.0.0.1",
        9000,
        cpu=cpu,
        ram=10,
        last_seen=time.time(),
    )


class MultiCoordinatorRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_coordinators_do_not_submit_same_recovery(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "shared-recovery.db"

            store_one = SQLiteTaskStore(db_path)
            store_two = SQLiteTaskStore(db_path)

            dead = make_node("node-dead", 1)
            replacement = make_node("node-replacement", 20)

            lifecycle_one = TaskLifecycle(store=store_one)

            first_started = asyncio.Event()
            release_first = asyncio.Event()
            submissions = []

            task = Task(
                "calculate",
                args=(2, 3),
                task_id="multi-coordinator-task",
            )

            async def submit_first(node, submitted_task):
                submissions.append("coordinator-one")
                first_started.set()
                await release_first.wait()

                return TaskResult(
                    submitted_task.task_id,
                    TaskStatus.COMPLETED,
                    value=5,
                    attempts=1,
                )

            async def submit_second(node, submitted_task):
                submissions.append("coordinator-two")

                return TaskResult(
                    submitted_task.task_id,
                    TaskStatus.COMPLETED,
                    value=5,
                    attempts=1,
                )

            coordinator_one = TaskRecoveryCoordinator(
                lifecycle_one,
                snapshot=lambda: [dead, replacement],
                submit=submit_first,
                store=store_one,
            )

            coordinator_one.register(task, dead.node_id)

            # The second coordinator loads the task before recovery starts.
            lifecycle_two = TaskLifecycle(store=store_two)

            coordinator_two = TaskRecoveryCoordinator(
                lifecycle_two,
                snapshot=lambda: [dead, replacement],
                submit=submit_second,
                store=store_two,
            )

            first_recovery = asyncio.create_task(
                coordinator_one.recover_node(dead.node_id)
            )

            try:
                await asyncio.wait_for(first_started.wait(), timeout=2)

                await coordinator_two.recover_interrupted()

            finally:
                release_first.set()

            await first_recovery

            self.assertEqual(
                submissions,
                ["coordinator-one"],
                "Two independent coordinators submitted the same task",
            )


if __name__ == "__main__":
    unittest.main()
