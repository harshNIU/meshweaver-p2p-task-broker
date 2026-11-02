"""Integration tests for three-node mesh bootstrap discovery."""

import unittest

from net.bootstrap import SeedAddress, request_peer_list
from net.udp_node import start_udp_node


class TestThreeNodeMesh(unittest.IsolatedAsyncioTestCase):
    async def test_three_nodes_join_using_one_bootstrap(self):
        transports = []

        try:
            # Start the bootstrap node.
            seed_transport, seed_protocol = await start_udp_node(
                node_id="seed-node",
            )
            transports.append(seed_transport)

            seed_host, seed_port = seed_transport.get_extra_info("sockname")
            seed = SeedAddress(seed_host, seed_port)

            # Start the second node and let it discover the bootstrap.
            node_b_transport, node_b_protocol = await start_udp_node(
                node_id="node-b",
            )
            transports.append(node_b_transport)

            node_b_joined = await request_peer_list(
                protocol=node_b_protocol,
                node_id="node-b",
                seed=seed,
                timeout=1.0,
            )
            self.assertTrue(
                node_b_joined,
                "Node B failed to join through the bootstrap node",
            )

            # Start the third node and use the same bootstrap address.
            node_c_transport, node_c_protocol = await start_udp_node(
                node_id="node-c",
            )
            transports.append(node_c_transport)

            node_c_joined = await request_peer_list(
                protocol=node_c_protocol,
                node_id="node-c",
                seed=seed,
                timeout=1.0,
            )
            self.assertTrue(
                node_c_joined,
                "Node C failed to join through the same bootstrap node",
            )

            # Both joining nodes should know the bootstrap node.
            self.assertIn("seed-node", node_b_protocol.node_registry)
            self.assertIn("seed-node", node_c_protocol.node_registry)

            # The bootstrap node should have observed both joining nodes.
            self.assertIn("node-b", seed_protocol.node_registry)
            self.assertIn("node-c", seed_protocol.node_registry)

            # Each joining node should have recorded the seed's address.
            self.assertEqual(
                node_b_protocol.peer_manager.get_peer_address("seed-node"),
                (seed_host, seed_port),
            )
            self.assertEqual(
                node_c_protocol.peer_manager.get_peer_address("seed-node"),
                (seed_host, seed_port),
            )

        finally:
            for transport in reversed(transports):
                transport.close()


if __name__ == "__main__":
    unittest.main()
