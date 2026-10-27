import unittest

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from tasks.transport import TaskServer, send_task


class TestTaskTransport(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = ExecutionEngine()
        self.server = await TaskServer(self.engine).start()
        host, port = self.server.address

        class Peer:
            pass

        self.peer = Peer()
        self.peer.host = host
        self.peer.port = port

    async def asyncTearDown(self):
        await self.server.close()

    async def test_remote_task_returns_successful_result(self):
        self.engine.register("add", lambda a, b: a + b)
        task = Task(task_name="add", args=(2, 3))

        result = await send_task(self.peer, task)

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.status, TaskStatus.COMPLETED)
        self.assertEqual(result.value, 5)
        self.assertIsNone(result.error)

    async def test_remote_task_returns_exception_as_failed_result(self):
        def fail_task():
            raise RuntimeError("intentional test failure")

        self.engine.register("fail", fail_task)
        task = Task(task_name="fail")

        result = await send_task(self.peer, task)

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertIsNone(result.value)
        self.assertIn("RuntimeError", result.error)
        self.assertIn("intentional test failure", result.error)


if __name__ == "__main__":
    unittest.main()
