import asyncio
import unittest

from net.messages import PING, PONG, Message
from net.udp_node import start_udp_node


class TestPingPong(unittest.IsolatedAsyncioTestCase):
    async def test_ping_receives_pong(self):
        transport_a, protocol_a = await start_udp_node(node_id="node-a")
        transport_b, protocol_b = await start_udp_node(node_id="node-b")

        try:
            address_b = transport_b.get_extra_info("sockname")

            ping = Message(
                type=PING,
                sender_id="node-a",
                seq=1,
                payload={"target": "node-b"},
            )

            protocol_a.send_message(ping, address_b)

            # Wait for Node A to receive the PONG response.
            for _ in range(50):
                if any(
                    message.type == PONG for message, _ in protocol_a.received_envelopes
                ):
                    break

                await asyncio.sleep(0.02)

            # Confirm Node B received PING.
            self.assertTrue(
                any(
                    message.type == PING for message, _ in protocol_b.received_envelopes
                )
            )

            # Confirm Node A received PONG.
            pong_messages = [
                message
                for message, _ in protocol_a.received_envelopes
                if message.type == PONG
            ]

            self.assertEqual(len(pong_messages), 1)
            self.assertEqual(pong_messages[0].sender_id, "node-b")
            self.assertEqual(pong_messages[0].seq, 1)

        finally:
            transport_a.close()
            transport_b.close()


if __name__ == "__main__":
    unittest.main()
