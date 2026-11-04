import asyncio
import unittest

from execution.engine import ExecutionEngine
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.tcp_node import start_tcp_server
from net.udp_node import MAX_UDP_MESSAGE_SIZE, start_udp_node

# Must contain at least 32 bytes.
TEST_SECRET = b"meshweaver-test-shared-secret-32-bytes-minimum"


class TestUDPTCPFallback(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = ExecutionEngine()
        self.engine.register("add", lambda a, b: a + b)

        # Start an authenticated TCP server and retain its received messages.
        self.tcp_server, self.tcp_received_messages = await start_tcp_server(
            host="127.0.0.1",
            port=0,
            execution_engine=self.engine,
            secret=TEST_SECRET,
        )

        self.tcp_port = self.tcp_server.sockets[0].getsockname()[1]

        # Start both UDP nodes with the same shared secret.
        self.transport_a, self.protocol_a = await start_udp_node(
            node_id="node-a",
            secret=TEST_SECRET,
        )

        self.transport_b, self.protocol_b = await start_udp_node(
            node_id="node-b",
            secret=TEST_SECRET,
        )

        self.udp_address_b = self.transport_b.get_extra_info("sockname")

        # Register the receiver's UDP and TCP addresses with the sender.
        self.protocol_a.peer_manager.record_activity(
            "node-b",
            address=self.udp_address_b,
        )

        self.protocol_a.peer_manager.record_tcp_address(
            "node-b",
            ("127.0.0.1", self.tcp_port),
        )

    async def asyncTearDown(self):
        self.transport_a.close()
        self.transport_b.close()

        self.tcp_server.close()
        await self.tcp_server.wait_closed()

        await asyncio.sleep(0)

    async def wait_for_condition(self, condition, timeout=3.0):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while not condition():
            if loop.time() >= deadline:
                self.fail("Timed out waiting for TCP fallback")

            await asyncio.sleep(0.02)

    async def test_small_message_uses_udp(self):
        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=1,
            payload={"text": "small message"},
        )

        self.protocol_a.send_message(message, self.udp_address_b)

        await self.wait_for_condition(
            lambda: any(
                received.type == "TEST"
                for received, _ in self.protocol_b.received_envelopes
            )
        )

    async def test_large_message_uses_tcp(self):
        message = Message(
            type=TASK_REQUEST,
            sender_id="node-a",
            seq=2,
            payload={
                "task": {
                    "task_id": "large-message-task",
                    "task_name": "add",
                    "args": [2, 3],
                    "kwargs": {},
                },
                "padding": "x" * (MAX_UDP_MESSAGE_SIZE + 100),
            },
        )

        self.protocol_a.send_message(message, self.udp_address_b)

        try:
            await self.wait_for_condition(
                lambda: any(
                    received.type == TASK_RESULT and received.seq == 2
                    for received, _ in self.protocol_a.received_envelopes
                )
            )
        except AssertionError:
            print("\n--- TCP FALLBACK DIAGNOSTICS ---")
            print("TCP send errors:", self.protocol_a.tcp_send_errors)
            print("TCP server received:", self.tcp_received_messages)
            print(
                "UDP A received envelopes:",
                self.protocol_a.received_envelopes,
            )
            print(
                "UDP B received messages:",
                self.protocol_b.received_messages,
            )
            print(
                "UDP B received envelopes:",
                self.protocol_b.received_envelopes,
            )
            print("--- END DIAGNOSTICS ---\n")
            raise

        result = next(
            received
            for received, _ in self.protocol_a.received_envelopes
            if received.type == TASK_RESULT and received.seq == 2
        )

        self.assertEqual(result.payload["status"], "completed")
        self.assertEqual(result.payload["result"], 5)

    async def test_large_message_without_tcp_endpoint_fails(self):
        self.protocol_a.peer_manager.record_tcp_address("node-b", None)

        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=3,
            payload={
                "padding": "x" * (MAX_UDP_MESSAGE_SIZE + 100),
            },
        )

        with self.assertRaises((RuntimeError, ValueError)):
            self.protocol_a.send_message(message, self.udp_address_b)


if __name__ == "__main__":
    unittest.main()
