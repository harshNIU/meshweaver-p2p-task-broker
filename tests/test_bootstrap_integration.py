import unittest

from net.bootstrap import SeedAddress, join_via_seed
from net.udp_node import start_udp_node


class TestBootstrapIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_join_via_seed_receives_pong(self):
        seed_transport, seed_protocol = await start_udp_node(node_id="seed-node")

        try:
            host, port = seed_transport.get_extra_info("sockname")
            seed = SeedAddress(host, port)

            joined = await join_via_seed(
                node_id="joining-node",
                seed=seed,
                timeout=1.0,
            )

            self.assertTrue(joined)
            self.assertTrue(
                any(
                    message.type == "PING" and message.sender_id == "joining-node"
                    for message, _ in seed_protocol.received_envelopes
                )
            )
        finally:
            seed_transport.close()


if __name__ == "__main__":
    unittest.main()
