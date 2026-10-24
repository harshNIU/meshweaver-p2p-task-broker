"""Unit and real-socket coverage for Member 2's Kademlia implementation."""

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, patch

from dht import Contact, DHTNode, RoutingTable, bucket_index, distance, make_node_id
from dht.ids import normalize_id
from dht.rpc import FIND_NODE, RESPONSE
from dht.store import ValueStore
from net.messages import Message


def contact(number, port=9000):
    return Contact(normalize_id(number), "127.0.0.1", port)


class IdentityAndBuckets(unittest.TestCase):
    def test_hash_and_distance(self):
        self.assertEqual(
            make_node_id("abc"), "a9993e364706816aba3e25717850c26c9cd0d89d"
        )
        self.assertEqual(distance(3, 5), 6)
        self.assertEqual(bucket_index(0, 0), -1)
        self.assertEqual(bucket_index(0, 1 << 159), 159)

    def test_invalid_ids(self):
        for value in (-1, 1 << 160, True, "hello", "z" * 40):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_id(value)

    def test_self_is_not_added(self):
        table = RoutingTable(0)
        table.add(contact(0))
        self.assertEqual(len(table), 0)

    def test_lru_and_full_bucket(self):
        table = RoutingTable(0, k=2)
        table.add(contact(4))
        table.add(contact(5))
        table.add(contact(4, 9001))
        self.assertEqual(table.add(contact(6)).node_id, normalize_id(5))
        self.assertEqual(len(table), 2)
        self.assertEqual(table.closest(4)[0].port, 9001)
        table.remove(5)
        table.add(contact(6))
        self.assertEqual(
            [c.node_id for c in table.closest(7)], [normalize_id(6), normalize_id(4)]
        )

    def test_store_ttl_null_capacity_and_copy(self):
        store = ValueStore(capacity=2)
        with patch("dht.store.time.monotonic", return_value=1):
            store.put(normalize_id(1), None, ttl=2)
        with patch("dht.store.time.monotonic", return_value=2):
            self.assertEqual(store.get(normalize_id(1)), (True, None))
        with patch("dht.store.time.monotonic", return_value=4):
            self.assertEqual(store.get(normalize_id(1)), (False, None))
        value = {"x": []}
        store.put(normalize_id(2), value)
        value["x"].append(1)
        self.assertEqual(store.get(normalize_id(2))[1], {"x": []})
        store.put(normalize_id(3), 3)
        store.put(normalize_id(4), 4)
        self.assertFalse(store.get(normalize_id(2))[0])

    def test_store_rejects_invalid_values(self):
        store = ValueStore(max_value_bytes=10)
        for value, ttl in [
            ("x" * 20, 1),
            (float("nan"), 1),
            (1, -1),
            (1, float("inf")),
        ]:
            with self.subTest(value=value, ttl=ttl), self.assertRaises(ValueError):
                store.put(normalize_id(1), value, ttl)


class DHTNetwork(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.nodes = []

    async def asyncTearDown(self):
        await asyncio.gather(*(node.close() for node in self.nodes))

    async def node(self, number=None, **kwargs):
        node = await DHTNode.start(
            node_id=normalize_id(number) if number is not None else None,
            timeout=0.1,
            **kwargs
        )
        self.nodes.append(node)
        return node

    async def test_ten_nodes_one_seed_and_remote_store(self):
        seed = await self.node()
        for _ in range(9):
            node = await self.node()
            await node.join(seed.contact.addr)
        await asyncio.gather(*(node.lookup(node.node_id) for node in self.nodes))
        self.assertEqual([len(node.routing) for node in self.nodes], [9] * 10)
        self.assertEqual(await seed.put("key", None), 10)
        reader = await self.node()
        await reader.join(seed.contact.addr)
        self.assertIsNone(await reader.get("key"))
        with self.assertRaises(KeyError):
            await reader.get("absent")

    async def test_multihop_lookup(self):
        a, b, c, d = [await self.node(i) for i in (1, 2, 3, 4)]
        a.routing.add(b.contact)
        b.routing.add(c.contact)
        c.routing.add(d.contact)
        found = await a.lookup(d.node_id)
        self.assertEqual(found[0].node_id, d.node_id)
        self.assertIn(d.node_id, [peer.node_id for peer in a.routing.contacts()])

    async def test_dead_seed_timeout_cleans_pending(self):
        seed, other = await self.node(), await self.node()
        address = seed.contact.addr
        await seed.close()
        with self.assertRaises(TimeoutError):
            await other.join(address)
        self.assertFalse(other._pending)

    async def test_refresh_evicts_dead_peer_and_rejoin(self):
        a, b = await self.node(), await self.node()
        await b.join(a.contact.addr)
        old_id = b.node_id
        await b.close()
        await a.refresh()
        self.assertNotIn(old_id, [c.node_id for c in a.routing.contacts()])
        replacement = await DHTNode.start(node_id=old_id, timeout=0.1)
        self.nodes.append(replacement)
        await replacement.join(a.contact.addr)
        self.assertEqual(a.routing.closest(old_id)[0].addr, replacement.contact.addr)

    async def test_bucket_probes_keep_live_and_replace_dead(self):
        a = await self.node(0, k=1, alpha=1)
        b, c = await self.node(4), await self.node(5)
        a._learn(b.contact)
        a._learn(c.contact)
        await asyncio.gather(*list(a._probes.values()))
        await asyncio.sleep(0)
        self.assertEqual(a.routing.closest(4)[0].node_id, b.node_id)
        await b.close()
        a._learn(c.contact)
        await asyncio.gather(*list(a._probes.values()))
        self.assertEqual(a.routing.closest(4)[0].node_id, c.node_id)

    async def test_malformed_packets_do_not_break_node(self):
        a, b = await self.node(), await self.node()
        for raw in [
            b"bad json",
            b"{}",
            b"[]",
            b"x" * 20000,
            Message(
                FIND_NODE, b.node_id, 1, {"request_id": "a" * 32, "target_id": []}
            ).encode(),
        ]:
            a.datagram_received(raw, b.contact.addr)
        self.assertTrue(await b.ping(a.contact))

    async def test_unmatched_response_cannot_complete_request(self):
        a, b, rogue = await self.node(), await self.node(), await self.node()
        future = asyncio.get_running_loop().create_future()
        request_id = "a" * 32
        a._pending[request_id] = (future, b.contact.addr, b.node_id, FIND_NODE)
        payload = {"request_id": request_id, "rpc": FIND_NODE, "nodes": []}
        a.datagram_received(
            Message(RESPONSE, b.node_id, 1, payload).encode(), rogue.contact.addr
        )
        self.assertFalse(future.done())
        a.datagram_received(
            Message(RESPONSE, rogue.node_id, 1, payload).encode(), b.contact.addr
        )
        self.assertFalse(future.done())
        a.datagram_received(
            Message(RESPONSE, b.node_id, 1, payload).encode(), b.contact.addr
        )
        self.assertTrue(future.done())
        a._pending.clear()

    async def test_close_cancels_request_and_is_idempotent(self):
        a, b = await self.node(), await self.node()
        await b.close()
        task = asyncio.create_task(a.ping(b.contact))
        for _ in range(100):
            if a._pending:
                break
            await asyncio.sleep(0.001)
        await a.close()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(a._pending)
        await a.close()

    async def test_stale_bucket_refresh_queries_target_in_bucket(self):
        a, b = await self.node(0), await self.node(4)
        a.routing.add(b.contact)
        a.routing.updated[2] = time.monotonic() - a.refresh_interval - 1
        with patch.object(a, "lookup", new_callable=AsyncMock) as lookup:
            await a.refresh()
            targets = [call.args[0] for call in lookup.call_args_list]
            self.assertEqual(bucket_index(a.node_id, targets[0]), 2)
            self.assertEqual(targets[-1], a.node_id)

    async def test_lookup_parallelism_is_bounded_by_alpha(self):
        a = await self.node(0, alpha=3)
        for number in range(1, 9):
            a.routing.add(contact(number, 9000 + number))
        active = peak = 0

        async def query(address, kind, payload, expected_id):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            return {"nodes": []}

        with patch.object(a, "request", side_effect=query):
            found = await a.lookup(normalize_id(9))
        self.assertEqual(len(found), 8)
        self.assertEqual(peak, 3)
