import asyncio
import time
import unittest

from execution.models import Task, TaskResult, TaskStatus
from gossip.state import NodeInfo
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.recovery import TaskRecoveryCoordinator


def make_node(node_id, cpu):
    return NodeInfo(
        node_id,
        "127.0.0.1",
        9000,
        cpu=cpu,
        ram=10,
        last_seen=time.time(),
    )


class RecoveryConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_interrupted_recovery_does_not_duplicate_active_node_recovery(self):
        dead = make_node("node-dead", 1)
        replacement = make_node("node-replacement", 20)

        lifecycle = TaskLifecycle()
        first_submission_started = asyncio.Event()
        release_first_submission = asyncio.Event()
        submitted_nodes = []

        task = Task(
            "calculate",
            args=(2, 3),
            task_id="concurrent-recovery-1",
        )

        async def submit(node, submitted_task):
            submitted_nodes.append(node.node_id)

            if len(submitted_nodes) == 1:
                first_submission_started.set()
                await release_first_submission.wait()

            return TaskResult(
                submitted_task.task_id,
                TaskStatus.COMPLETED,
                value=5,
                attempts=1,
            )

        coordinator = TaskRecoveryCoordinator(
            lifecycle,
            snapshot=lambda: [dead, replacement],
            submit=submit,
        )

        coordinator.register(task, dead.node_id)

        first_recovery = asyncio.create_task(
            coordinator.recover_node(dead.node_id)
        )

        try:
            await asyncio.wait_for(first_submission_started.wait(), timeout=2)

            # A second recovery path starts while the first submission
            # is still waiting for its result.
            await coordinator.recover_interrupted()

        finally:
            release_first_submission.set()

        await first_recovery

        self.assertEqual(
            submitted_nodes,
            [replacement.node_id],
            "The same task must not be submitted by two recovery paths",
        )

        record = lifecycle.get(task.task_id)
        self.assertEqual(record.state, TaskState.DONE)
        self.assertEqual(record.node, replacement.node_id)


if __name__ == "__main__":
    unittest.main()
