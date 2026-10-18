import asyncio
import unittest

from net.messages import PING, PONG, Message
from net.udp_node import start_udp_node


class TestUDPNode(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.transport_a, self.protocol_a = await start_udp_node(node_id="node-a")
        self.transport_b, self.protocol_b = await start_udp_node(node_id="node-b")

    async def asyncTearDown(self):
        self.transport_a.close()
        self.transport_b.close()
        await asyncio.sleep(0)

    async def wait_for_condition(self, condition, timeout=2.0):
        """Wait until a condition becomes true or the timeout expires."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while not condition():
            if loop.time() >= deadline:
                self.fail("Timed out waiting for UDP communication")

            await asyncio.sleep(0.02)

    async def test_two_nodes_exchange_plain_text_messages(self):
        address_b = self.transport_b.get_extra_info("sockname")

        self.protocol_a.send_message("Hello MeshWeaver", address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_messages) == 1
        )

        message, sender = self.protocol_b.received_messages[0]

        self.assertEqual(message, "Hello MeshWeaver")
        self.assertEqual(sender[0], "127.0.0.1")

    async def test_structured_message_is_received(self):
        address_b = self.transport_b.get_extra_info("sockname")

        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=1,
            payload={"text": "Hello MeshWeaver"},
        )

        self.protocol_a.send_message(message, address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_envelopes) == 1
        )

        received, sender = self.protocol_b.received_envelopes[0]

        self.assertEqual(received.type, "TEST")
        self.assertEqual(received.sender_id, "node-a")
        self.assertEqual(received.seq, 1)
        self.assertEqual(received.payload, {"text": "Hello MeshWeaver"})
        self.assertEqual(sender[0], "127.0.0.1")

    async def test_ping_receives_pong_response(self):
        address_b = self.transport_b.get_extra_info("sockname")

        ping = Message(
            type=PING,
            sender_id="node-a",
            seq=42,
            payload={},
        )

        self.protocol_a.send_message(ping, address_b)

        await self.wait_for_condition(
            lambda: any(
                received.type == PONG
                for received, _ in self.protocol_a.received_envelopes
            )
        )

        pong, sender = next(
            (received, sender)
            for received, sender in self.protocol_a.received_envelopes
            if received.type == PONG
        )

        self.assertEqual(pong.sender_id, "node-b")
        self.assertEqual(pong.seq, 42)
        self.assertEqual(pong.payload, {"reply_to": "node-a"})
        self.assertEqual(sender[0], "127.0.0.1")

    async def test_unknown_structured_message_does_not_reply(self):
        address_b = self.transport_b.get_extra_info("sockname")

        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=2,
            payload={},
        )

        self.protocol_a.send_message(message, address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_envelopes) == 1
        )

        await asyncio.sleep(0.1)

        self.assertEqual(len(self.protocol_a.received_envelopes), 0)

    async def test_structured_message_records_peer_activity(self):
        address_b = self.transport_b.get_extra_info("sockname")

        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=10,
            payload={},
        )

        self.protocol_a.send_message(message, address_b)

        await self.wait_for_condition(
            lambda: "node-a" in self.protocol_b.peer_manager.peers
        )

        self.assertTrue(self.protocol_b.peer_manager.is_peer_alive("node-a"))

    async def test_plain_text_does_not_register_structured_peer(self):
        address_b = self.transport_b.get_extra_info("sockname")

        self.protocol_a.send_message("Hello MeshWeaver", address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_messages) == 1
        )

        self.assertEqual(self.protocol_b.peer_manager.peers, {})


if __name__ == "__main__":
    unittest.main()
