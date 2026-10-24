"""Integration tests for automatic stale-peer cleanup."""

import asyncio
import unittest

from net.messages import PING, Message
from net.peer_manager import PeerConnectionManager
from net.udp_node import start_udp_node


class TestAutomaticPeerCleanup(unittest.IsolatedAsyncioTestCase):
    async def test_inactive_peer_is_removed_automatically(self):
        target_transport = None
        sender_transport = None

        try:
            manager = PeerConnectionManager(timeout_seconds=0.12)

            target_transport, target_protocol = await start_udp_node(
                node_id="target-node",
                peer_manager=manager,
            )
            sender_transport, _sender_protocol = await start_udp_node(
                node_id="silent-peer",
            )

            target_address = target_transport.get_extra_info("sockname")

            ping = Message(
                type=PING,
                sender_id="silent-peer",
                seq=1,
                payload={},
            )
            sender_transport.sendto(ping.encode(), target_address)

            # Wait until the target records the incoming peer.
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 1.0

            while (
                "silent-peer" not in target_protocol.node_registry
                and loop.time() < deadline
            ):
                await asyncio.sleep(0.01)

            self.assertIn("silent-peer", target_protocol.node_registry)
            self.assertTrue(manager.is_peer_alive("silent-peer"))

            # No further messages arrive from this peer. Automatic cleanup
            # should remove its registry entry and saved address.
            deadline = loop.time() + 1.0

            while (
                "silent-peer" in target_protocol.node_registry
                and loop.time() < deadline
            ):
                await asyncio.sleep(0.01)

            self.assertNotIn("silent-peer", target_protocol.node_registry)
            self.assertFalse(manager.is_peer_alive("silent-peer"))
            self.assertIsNone(manager.get_peer_address("silent-peer"))

            # Cleanup must never remove the local node itself.
            self.assertIn("target-node", target_protocol.node_registry)

        finally:
            if sender_transport is not None:
                sender_transport.close()
            if target_transport is not None:
                target_transport.close()
            await asyncio.sleep(0)


if __name__ == "__main__":
    unittest.main()
