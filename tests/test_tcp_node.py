import asyncio
import unittest

from net.messages import Message
from net.tcp_node import (
    read_message,
    send_tcp_message,
    start_tcp_server,
)


class TestTCPNode(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server, self.received_messages = await start_tcp_server()
        self.host, self.port = self.server.sockets[0].getsockname()[:2]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    async def test_tcp_message_round_trip(self):
        message = Message(
            type="TASK",
            sender_id="node-1",
            seq=1,
            payload={"task": "calculate", "value": 10},
        )

        response = await send_tcp_message(self.host, self.port, message)

        self.assertEqual(len(self.received_messages), 1)
        self.assertEqual(self.received_messages[0].type, "TASK")
        self.assertEqual(self.received_messages[0].sender_id, "node-1")
        self.assertEqual(self.received_messages[0].payload["value"], 10)

        self.assertEqual(response.type, "ACK")
        self.assertEqual(response.seq, 1)
        self.assertTrue(response.payload["received"])

    async def test_tcp_message_preserves_payload(self):
        message = Message(
            type="RESULT",
            sender_id="node-2",
            seq=7,
            payload={"status": "success", "result": 42},
        )

        await send_tcp_message(self.host, self.port, message)

        self.assertEqual(
            self.received_messages[0].payload,
            {"status": "success", "result": 42},
        )

    async def test_multiple_messages_on_one_connection(self):
        reader, writer = await asyncio.open_connection(self.host, self.port)

        messages = [
            Message(
                type="TASK",
                sender_id="node-1",
                seq=10,
                payload={"task": "first"},
            ),
            Message(
                type="RESULT",
                sender_id="node-1",
                seq=11,
                payload={"result": "second"},
            ),
        ]

        try:
            # Send both messages over the same TCP connection.
            for message in messages:
                writer.write(message.encode() + b"\n")

            await writer.drain()

            # Read the ACK for each message.
            responses = [
                await read_message(reader),
                await read_message(reader),
            ]

            self.assertEqual(
                [response.seq for response in responses],
                [10, 11],
            )

            self.assertTrue(all(response.payload["received"] for response in responses))

            self.assertEqual(
                [message.seq for message in self.received_messages],
                [10, 11],
            )
        finally:
            writer.close()
            await writer.wait_closed()


if __name__ == "__main__":
    unittest.main()
