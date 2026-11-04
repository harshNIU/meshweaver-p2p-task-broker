import unittest
from unittest.mock import AsyncMock, Mock, patch

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from net.messages import TASK_RESULT, Message
from tasks.transport import TaskServer, send_task

TEST_SECRET = b"meshweaver-test-secret-32-bytes!!"


class TestTaskTransport(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = ExecutionEngine()

        self.server = await TaskServer(
            self.engine,
            secret=TEST_SECRET,
        ).start()

        host, port = self.server.address

        class Peer:
            pass

        self.peer = Peer()
        self.peer.host = host
        self.peer.port = port

    async def asyncTearDown(self):
        await self.server.close()

    async def _send_mock_response(self, payload):
        task = Task(task_name="mock_task")
        payload = {"task_id": task.task_id, **payload}

        response = Message(
            TASK_RESULT,
            "mock-worker",
            1,
            payload,
        )

        writer = Mock()
        writer.wait_closed = AsyncMock()

        with (
            patch(
                "tasks.transport.asyncio.open_connection",
                new=AsyncMock(return_value=(object(), writer)),
            ),
            patch("tasks.transport.send_message", new=AsyncMock()),
            patch(
                "tasks.transport.read_message",
                new=AsyncMock(return_value=response),
            ),
        ):
            result = await send_task(self.peer, task)

        return result

    async def test_remote_task_returns_successful_result(self):
        self.engine.register("add", lambda a, b: a + b)

        task = Task(
            task_name="add",
            args=(2, 3),
        )

        result = await send_task(
            self.peer,
            task,
            secret=TEST_SECRET,
        )

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.status, TaskStatus.COMPLETED)
        self.assertEqual(result.value, 5)
        self.assertIsNone(result.error)

    async def test_remote_task_returns_exception_as_failed_result(self):
        def fail_task():
            raise RuntimeError("intentional test failure")

        self.engine.register("fail", fail_task)

        task = Task(task_name="fail")

        result = await send_task(
            self.peer,
            task,
            secret=TEST_SECRET,
        )

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertIsNone(result.value)
        self.assertIn("RuntimeError", result.error)
        self.assertIn("intentional test failure", result.error)

    async def test_remote_task_accepts_result_field(self):
        result = await self._send_mock_response(
            {
                "status": "completed",
                "result": 5,
            }
        )

        self.assertEqual(result.status, TaskStatus.COMPLETED)
        self.assertEqual(result.value, 5)

    async def test_explicit_none_result_does_not_fall_back_to_value(self):
        result = await self._send_mock_response(
            {
                "status": "completed",
                "result": None,
                "value": 99,
            }
        )

        self.assertIsNone(result.value)


if __name__ == "__main__":
    unittest.main()