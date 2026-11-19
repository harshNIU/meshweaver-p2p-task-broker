
"""Tests for remote task execution over TCP."""

import unittest

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from execution.serializer import task_to_dict
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.tcp_node import (
    send_callable_task,
    send_tcp_message,
    start_tcp_server,
)
from security.message_signing import MessageAuthenticationError

TEST_SECRET = b"meshweaver-test-secret-32-bytes!!"
WRONG_SECRET = b"another-test-secret-32-bytes!!!"


class TestTCPTaskExecution(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = ExecutionEngine(max_workers=2)
        self.server, self.received_messages = await start_tcp_server(
            execution_engine=self.engine,
            secret=TEST_SECRET,
        )
        self.host, self.port = self.server.sockets[0].getsockname()[:2]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        self.engine.shutdown()

    def make_request(self, task, seq):
        return Message(
            type=TASK_REQUEST,
            sender_id="node-client",
            seq=seq,
            payload={"task": task_to_dict(task)},
        )

    async def test_registered_task_executes_and_returns_result(self):
        self.engine.register("add", lambda a, b: a + b)
        task = Task(task_name="add", args=(10, 15))

        response = await send_tcp_message(
            self.host,
            self.port,
            self.make_request(task, 1),
            secret=TEST_SECRET,
        )

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.seq, 1)
        self.assertEqual(response.payload["task_id"], task.task_id)
        self.assertEqual(response.payload["status"], TaskStatus.COMPLETED.value)
        self.assertEqual(response.payload["result"], 25)
        self.assertEqual(response.payload["attempts"], 1)
        self.assertEqual(len(self.received_messages), 1)

    async def test_unknown_task_returns_failure_result(self):
        task = Task(task_name="not_registered")

        response = await send_tcp_message(
            self.host,
            self.port,
            self.make_request(task, 2),
            secret=TEST_SECRET,
        )

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

        response = await send_tcp_message(
            self.host, self.port, message, secret=TEST_SECRET
        )

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

        response = await send_tcp_message(
            self.host, self.port, message, secret=TEST_SECRET
        )

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.seq, 4)
        self.assertEqual(response.payload["status"], TaskStatus.FAILED.value)
        self.assertIn("task object", response.payload["error"])

    async def test_task_function_exception_returns_failure_result(self):
        def failing_task():
            raise RuntimeError("intentional failure")

        self.engine.register("fail", failing_task)
        task = Task(task_name="fail")

        response = await send_tcp_message(
            self.host,
            self.port,
            self.make_request(task, 5),
            secret=TEST_SECRET,
        )

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.payload["status"], TaskStatus.FAILED.value)
        self.assertIn("intentional failure", response.payload["error"])

    async def test_duplicate_task_id_returns_failure_without_closing_response(self):
        executions = []

        def count_execution():
            executions.append("executed")
            return len(executions)

        self.engine.register("count", count_execution)
        task = Task(task_name="count", task_id="duplicate-network-task")

        first_response = await send_tcp_message(
            self.host,
            self.port,
            self.make_request(task, 6),
            secret=TEST_SECRET,
        )
        second_response = await send_tcp_message(
            self.host,
            self.port,
            self.make_request(task, 7),
            secret=TEST_SECRET,
        )

        self.assertEqual(first_response.type, TASK_RESULT)
        self.assertEqual(first_response.payload["status"], TaskStatus.COMPLETED.value)
        self.assertEqual(first_response.payload["result"], 1)

        self.assertEqual(second_response.type, TASK_RESULT)
        self.assertEqual(second_response.seq, 7)
        self.assertEqual(second_response.payload["task_id"], task.task_id)
        self.assertEqual(second_response.payload["status"], TaskStatus.FAILED.value)
        self.assertIn("already exists", second_response.payload["error"])

        self.assertEqual(executions, ["executed"])

    async def test_authenticated_remote_callable_executes_and_returns_result(self):
        def multiply_and_add(a, b, bonus=0):
            return a * b + bonus

        task = Task(
            task_name="multiply_and_add",
            args=(10, 20),
            kwargs={"bonus": 5},
        )

        result = await send_callable_task(
            self.host,
            self.port,
            task,
            multiply_and_add,
            secret=TEST_SECRET,
        )

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.status, TaskStatus.COMPLETED)
        self.assertEqual(result.value, 205)
        self.assertEqual(result.attempts, 1)

        self.assertTrue(
            any(message.type == "TASK_SUBMIT" for message in self.received_messages)
        )

    async def test_remote_callable_exception_returns_failed_result(self):
        def failing_callable():
            raise RuntimeError("remote callable failed")

        task = Task(task_name="failing_callable")

        result = await send_callable_task(
            self.host,
            self.port,
            task,
            failing_callable,
            secret=TEST_SECRET,
        )

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertIn("remote callable failed", result.error)

    async def test_remote_callable_rejects_wrong_authentication_secret(self):
        executions = []

        def should_not_execute():
            executions.append(True)
            return "executed"

        task = Task(task_name="must_not_execute")

        with self.assertRaises(MessageAuthenticationError):
            await send_callable_task(
                self.host,
                self.port,
                task,
                should_not_execute,
                secret=WRONG_SECRET,
            )

        self.assertEqual(executions, [])

    async def test_malformed_callable_payload_returns_failure_result(self):
        message = Message(
            type="TASK_SUBMIT",
            sender_id="node-client",
            seq=10,
            payload={
                "task": {
                    "task_id": "invalid-callable-task",
                    "task_name": "invalid_callable",
                    "max_retries": 0,
                    "timeout_seconds": None,
                },
                "callable_payload": "not-valid-base64!!!",
            },
        )

        response = await send_tcp_message(
            self.host,
            self.port,
            message,
            secret=TEST_SECRET,
        )

        self.assertEqual(response.type, TASK_RESULT)
        self.assertEqual(response.payload["status"], TaskStatus.FAILED.value)


if __name__ == "__main__":
    unittest.main()
