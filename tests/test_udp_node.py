
import asyncio
import unittest

from net.udp_node import start_udp_node


class TestUDPNode(unittest.IsolatedAsyncioTestCase):
    async def test_two_nodes_exchange_messages(self):
        transport_a, protocol_a = await start_udp_node()
        transport_b, protocol_b = await start_udp_node()

        try:
            address_b = transport_b.get_extra_info("sockname")

            protocol_a.send_message("Hello MeshWeaver", address_b)

            for _ in range(50):
                if protocol_b.received_messages:
                    break
                await asyncio.sleep(0.02)

            self.assertEqual(len(protocol_b.received_messages), 1)
            message, sender = protocol_b.received_messages[0]

            self.assertEqual(message, "Hello MeshWeaver")
            self.assertEqual(sender[0], "127.0.0.1")

        finally:
            transport_a.close()
            transport_b.close()


if __name__ == "__main__":
    unittest.main()