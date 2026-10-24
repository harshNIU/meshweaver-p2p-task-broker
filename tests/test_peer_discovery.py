
import asyncio
import time
import unittest

from gossip.state import NodeInfo
from net.messages import PEER_LIST, PING, Message
from net.udp_node import start_udp_node


class TestPeerDiscovery(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.transport_a, self.protocol_a = await start_udp_node(
            node_id="node-a"
        )
        self.transport_b, self.protocol_b = await start_udp_node(
            node_id="node-b"
        )

    async def asyncTearDown(self):
        self.transport_a.close()
        self.transport_b.close()
        await asyncio.sleep(0)

    async def wait_for_condition(self, condition, timeout=2.0):
        """Wait for asynchronous UDP communication to complete."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while not condition():
            if loop.time() >= deadline:
                self.fail("Timed out waiting for peer discovery")

            await asyncio.sleep(0.02)

    async def test_ping_exchanges_peer_list(self):
        """A ping causes the receiver to advertise its known peers."""
        self.protocol_b.node_registry["node-c"] = NodeInfo(
            node_id="node-c",
            host="127.0.0.3",
            port=8003,
            last_seen=time.time(),
        )

        address_b = self.transport_b.get_extra_info("sockname")
        self.protocol_a.send_message(
            Message(
                type=PING,
                sender_id="node-a",
                seq=800,
                payload={},
            ),
            address_b,
        )

        await self.wait_for_condition(
            lambda: "node-c" in self.protocol_a.node_registry
        )

        discovered = self.protocol_a.node_registry["node-c"]
        self.assertEqual(discovered.host, "127.0.0.3")
        self.assertEqual(discovered.port, 8003)

        self.assertIn("node-b", self.protocol_a.node_registry)
        self.assertEqual(
            self.protocol_a.peer_manager.get_peer_address("node-c"),
            ("127.0.0.3", 8003),
        )

    async def test_peer_list_message_merges_valid_records(self):
        """A valid peer-list message adds nodes to the registry."""
        address_a = self.transport_a.get_extra_info("sockname")
        message = Message(
            type=PEER_LIST,
            sender_id="node-b",
            seq=801,
            payload={
                "nodes": [
                    {
                        "node_id": "node-c",
                        "host": "127.0.0.3",
                        "port": 8003,
                        "cpu": 0.25,
                        "ram": 0.5,
                        "last_seen": 100.0,
                        "state": "ALIVE",
                    }
                ]
            },
        )

        self.protocol_b.send_message(message, address_a)

        await self.wait_for_condition(
            lambda: "node-c" in self.protocol_a.node_registry
        )

        node = self.protocol_a.node_registry["node-c"]
        self.assertEqual(node.host, "127.0.0.3")
        self.assertEqual(node.port, 8003)
        self.assertEqual(node.cpu, 0.25)
        self.assertEqual(node.ram, 0.5)
        self.assertEqual(node.last_seen, 100.0)

    async def test_invalid_peer_records_are_ignored(self):
        """Invalid records must not enter the peer registry."""
        address_a = self.transport_a.get_extra_info("sockname")
        message = Message(
            type=PEER_LIST,
            sender_id="node-b",
            seq=802,
            payload={
                "nodes": [
                    {
                        "node_id": "invalid-port",
                        "host": "127.0.0.3",
                        "port": 70000,
                    },
                    {
                        "node_id": "invalid-host",
                        "host": "",
                        "port": 8003,
                    },
                    "not-a-record",
                    {
                        "node_id": "invalid-state",
                        "host": "127.0.0.4",
                        "port": 8004,
                        "state": "UNKNOWN",
                    },
                ]
            },
        )

        self.protocol_b.send_message(message, address_a)

        await self.wait_for_condition(
            lambda: any(
                received.type == PEER_LIST and received.seq == 802
                for received, _address in self.protocol_a.received_envelopes
            )
        )

        self.assertNotIn("invalid-port", self.protocol_a.node_registry)
        self.assertNotIn("invalid-host", self.protocol_a.node_registry)
        self.assertNotIn("invalid-state", self.protocol_a.node_registry)

    async def test_peer_list_ignores_self_record(self):
        """A peer list cannot overwrite the local node's own record."""
        self_info = self.protocol_a.node_registry["node-a"]
        address_a = self.transport_a.get_extra_info("sockname")

        message = Message(
            type=PEER_LIST,
            sender_id="node-b",
            seq=803,
            payload={
                "nodes": [
                    {
                        "node_id": "node-a",
                        "host": "192.0.2.99",
                        "port": 9999,
                        "last_seen": time.time() + 1000,
                        "state": "ALIVE",
                    }
                ]
            },
        )

        self.protocol_b.send_message(message, address_a)

        await self.wait_for_condition(
            lambda: any(
                received.type == PEER_LIST and received.seq == 803
                for received, _address in self.protocol_a.received_envelopes
            )
        )

        current = self.protocol_a.node_registry["node-a"]
        self.assertEqual(current.host, self_info.host)
        self.assertEqual(current.port, self_info.port)

    async def test_peer_list_with_invalid_payload_is_ignored(self):
        """Malformed peer-list payloads do not crash the UDP protocol."""
        address_a = self.transport_a.get_extra_info("sockname")

        message = Message(
            type=PEER_LIST,
            sender_id="node-b",
            seq=804,
            payload={"nodes": "not-a-list"},
        )
        self.protocol_b.send_message(message, address_a)

        await self.wait_for_condition(
            lambda: any(
                received.type == PEER_LIST and received.seq == 804
                for received, _address in self.protocol_a.received_envelopes
            )
        )

        self.assertNotIn("not-a-node", self.protocol_a.node_registry)

    async def test_non_string_peer_state_is_ignored(self):
        """A non-string state must not crash peer-list validation."""
        address_a = self.transport_a.get_extra_info("sockname")

        message = Message(
            type=PEER_LIST,
            sender_id="node-b",
            seq=805,
            payload={
                "nodes": [
                    {
                        "node_id": "invalid-state",
                        "host": "127.0.0.4",
                        "port": 8004,
                        "state": ["ALIVE"],
                    }
                ]
            },
        )

        self.protocol_b.send_message(message, address_a)

        await self.wait_for_condition(
            lambda: any(
                received.type == PEER_LIST and received.seq == 805
                for received, _address in self.protocol_a.received_envelopes
            )
        )

        self.assertNotIn("invalid-state", self.protocol_a.node_registry)


if __name__ == "__main__":
    unittest.main()
