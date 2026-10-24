"""Tests for remote task execution over TCP."""

import unittest

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from execution.serializer import task_to_dict
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.tcp_node import send_tcp_message, start_tcp_server


class TestTCPTaskExecution(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = ExecutionEngine(max_workers=2)
        self.server, self.received_messages = await start_tcp_server(
            execution_engine=self.engine
        )
        self.host, self.port = self.server.sockets[0].getsockname()[:2]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        self.engine.shutdown()

    async def test_registered_task_executes_and_returns_result(self):
        self.engine.register("add", lambda a, b: a + b)
        task = Task(task_name="add", args=(10, 15))

        message = Message(
            type=TASK_REQUEST,
            sender_id="node-client",
            seq=1,
            payload={"task": task_to_dict(task)},
        )

        response = await send_tcp_message(self.host, self.port, message)

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.seq, 1)
        self.assertEqual(response.payload["task_id"], task.task_id)
        self.assertEqual(response.payload["status"], TaskStatus.COMPLETED.value)
        self.assertEqual(response.payload["result"], 25)
        self.assertEqual(response.payload["attempts"], 1)
        self.assertEqual(len(self.received_messages), 1)

    async def test_unknown_task_returns_failure_result(self):
        task = Task(task_name="not_registered")

        message = Message(
            type=TASK_REQUEST,
            sender_id="node-client",
            seq=2,
            payload={"task": task_to_dict(task)},
        )

        response = await send_tcp_message(self.host, self.port, message)

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.seq, 2)
        self.assertEqual(response.payload["status"], TaskStatus.FAILED.value)
        self.assertIn("Unknown task", response.payload["error"])

    async def test_invalid_task_payload_returns_failure_result(self):
        message = Message(
            type=TASK_REQUEST,
            sender_id="node-client",
            seq=3,
            payload={"task": {"task_name": "add"}},
        )

        response = await send_tcp_message(self.host, self.port, message)

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.seq, 3)
        self.assertEqual(response.payload["status"], TaskStatus.FAILED.value)
        self.assertIn("Invalid task request", response.payload["error"])

    async def test_missing_task_object_returns_failure_result(self):
        message = Message(
            type=TASK_REQUEST,
            sender_id="node-client",
            seq=4,
            payload={},
        )

        response = await send_tcp_message(self.host, self.port, message)

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.seq, 4)
        self.assertEqual(response.payload["status"], TaskStatus.FAILED.value)
        self.assertIn("task object", response.payload["error"])

    async def test_task_function_exception_returns_failure_result(self):
        def failing_task():
            raise RuntimeError("intentional failure")

        self.engine.register("fail", failing_task)
        task = Task(task_name="fail")

        message = Message(
            type=TASK_REQUEST,
            sender_id="node-client",
            seq=5,
            payload={"task": task_to_dict(task)},
        )

        response = await send_tcp_message(self.host, self.port, message)

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.payload["status"], TaskStatus.FAILED.value)
        self.assertIn("intentional failure", response.payload["error"])


if __name__ == "__main__":
    unittest.main()
