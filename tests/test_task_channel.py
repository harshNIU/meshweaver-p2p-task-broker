
"""Tests for TCP task assignment, acknowledgement, and retry behavior."""

import unittest
from unittest.mock import AsyncMock, patch

from net.messages import Message, TASK_ACK, TASK_ASSIGN
from net.task_channel import (
    TaskAssignmentError,
    assign_task,
)
from net.tcp_node import start_tcp_server, _handle_task_assignment


class TestTaskChannel(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server, self.received_messages = await start_tcp_server(
            host="127.0.0.1",
            port=0,
        )
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    def make_task(self):
        return {
            "task_id": "day16-task-001",
            "task_name": "add",
            "args": [2, 3],
            "kwargs": {},
        }

    async def test_assignment_receives_acknowledgement(self):
        task = self.make_task()

        response = await assign_task(
            "127.0.0.1",
            self.port,
            task,
            timeout=1.0,
            max_retries=0,
            backoff=0,
        )

        self.assertEqual(response.type, TASK_ACK)
        self.assertTrue(response.payload["accepted"])
        self.assertEqual(response.payload["task_id"], task["task_id"])

        self.assertEqual(len(self.received_messages), 1)
        self.assertEqual(self.received_messages[0].type, TASK_ASSIGN)

    async def test_invalid_task_id_is_rejected(self):
        task = {
            "task_id": "task-correct",
            "task_name": "add",
        }

        message = Message(
            type=TASK_ASSIGN,
            sender_id="test-client",
            seq=2,
            payload={
                "task_id": "task-different",
                "task": task,
            },
        )

        response = _handle_task_assignment(message)

        self.assertEqual(response.type, TASK_ACK)
        self.assertFalse(response.payload["accepted"])

    async def test_retries_after_temporary_timeout(self):
        task = self.make_task()
        acknowledgement = Message(
            type=TASK_ACK,
            sender_id="tcp-server",
            seq=1,
            payload={
                "accepted": True,
                "task_id": task["task_id"],
            },
        )

        with (
            patch(
                "net.task_channel.send_tcp_message",
                new_callable=AsyncMock,
            ) as mock_send,
            patch(
                "net.task_channel.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            mock_send.side_effect = [
                TimeoutError("temporary timeout"),
                TimeoutError("temporary timeout"),
                acknowledgement,
            ]

            response = await assign_task(
                "127.0.0.1",
                self.port,
                task,
                timeout=0.5,
                max_retries=2,
                backoff=0.1,
            )

        self.assertEqual(response.type, TASK_ACK)
        self.assertTrue(response.payload["accepted"])
        self.assertEqual(mock_send.await_count, 3)

        # Exponential backoff: 0.1 seconds, then 0.2 seconds.
        self.assertEqual(
            [call.args[0] for call in mock_sleep.await_args_list],
            [0.1, 0.2],
        )

    async def test_stops_after_retry_limit(self):
        task = self.make_task()

        with (
            patch(
                "net.task_channel.send_tcp_message",
                new_callable=AsyncMock,
            ) as mock_send,
            patch(
                "net.task_channel.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            mock_send.side_effect = TimeoutError("peer unavailable")

            with self.assertRaises(TaskAssignmentError):
                await assign_task(
                    "127.0.0.1",
                    self.port,
                    task,
                    timeout=0.1,
                    max_retries=2,
                    backoff=0.1,
                )

        # Initial attempt plus two retries.
        self.assertEqual(mock_send.await_count, 3)
        self.assertEqual(mock_sleep.await_count, 2)


if __name__ == "__main__":
    unittest.main()
