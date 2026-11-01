"""Kademlia discovery over the existing asyncio UDP protocol.

Only the DHT message family is intercepted; the team's PING and task handling
remain available. Membership is unauthenticated until security is integrated.
"""

import asyncio
import ipaddress
import math
import secrets
import socket
import time
from uuid import uuid4

from dht.ids import BITS, as_int, make_node_id, normalize_id
from dht.lookup import iterative_lookup
from dht.routing_table import Contact, RoutingTable
from dht.rpc import (
    FIND_NODE,
    FIND_VALUE,
    MAX_PACKET_BYTES,
    PING,
    REQUESTS,
    RESPONSE,
    STORE,
)
from dht.store import ValueStore
from net.messages import Message
from net.udp_node import UDPProtocol


class DHTNode(UDPProtocol):
    def __init__(
        self, node_id=None, *, k=20, alpha=3, timeout=0.5, refresh_interval=60
    ):
        super().__init__(
            normalize_id(node_id) if node_id is not None else make_node_id(uuid4().hex)
        )
        if type(alpha) is not int or not 1 <= alpha <= k:
            raise ValueError("alpha must be between 1 and k")
        if not all(math.isfinite(x) and x > 0 for x in (timeout, refresh_interval)):
            raise ValueError("timeouts must be positive")
        self.routing = RoutingTable(self.node_id, k)
        self.k, self.alpha, self.timeout = k, alpha, timeout
        self.refresh_interval = refresh_interval
        self.store = ValueStore()
        self.contact = None
        self._pending = {}
        self._probes = {}
        self._maintenance = None
        self._seq = 0
        self.closed = False

    @classmethod
    async def start(cls, host="127.0.0.1", port=0, *, advertise_host=None, **kwargs):
        node = cls(**kwargs)
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: node, local_addr=(host, port), family=socket.AF_INET
        )
        address = transport.get_extra_info("sockname")
        try:
            advertised = advertise_host or address[0]
            if ipaddress.IPv4Address(advertised).is_unspecified:
                raise ValueError(
                    "wildcard bind requires advertise_host with a reachable IPv4 address"
                )
            node.contact = Contact(node.node_id, advertised, address[1])
        except Exception:
            transport.close()
            raise
        node._maintenance = asyncio.create_task(node._maintain())
        return node

    def _send(self, kind, payload, address):
        if self.closed or self.transport is None:
            raise ConnectionError("DHT node is closed")
        self._seq += 1
        raw = Message(kind, self.node_id, self._seq, payload).encode()
        if len(raw) > MAX_PACKET_BYTES:
            raise ValueError("DHT packet exceeds size limit")
        self.transport.sendto(raw, address)

    async def request(self, address, kind, payload, expected_id=None):
        if self.closed:
            raise ConnectionError("DHT node is closed")
        loop = asyncio.get_running_loop()
        resolved = await loop.getaddrinfo(
            address[0], address[1], family=socket.AF_INET, type=socket.SOCK_DGRAM
        )
        address = resolved[0][4]
        request_id = uuid4().hex
        future = loop.create_future()
        self._pending[request_id] = (future, address, expected_id, kind)
        try:
            self._send(kind, {**payload, "request_id": request_id}, address)
            return await asyncio.wait_for(future, self.timeout)
        finally:
            self._pending.pop(request_id, None)

    def datagram_received(self, data, addr):
        if self.closed or len(data) > MAX_PACKET_BYTES:
            return
        try:
            message = Message.decode(data)
            if not isinstance(message.type, str):
                return
            if message.type not in REQUESTS | {RESPONSE}:
                # Retain compatibility with the team's existing network protocol.
                if isinstance(message.sender_id, str) and type(message.seq) is int:
                    super().datagram_received(data, addr)
                return
            sender = Contact(message.sender_id, addr[0], addr[1])
            if not isinstance(message.payload, dict):
                return
            payload = message.payload
            request_id = payload.get("request_id")
            if not isinstance(request_id, str) or len(request_id) != 32:
                return
            if message.type == RESPONSE:
                pending = self._pending.get(request_id)
                if pending is None:
                    return
                future, address, expected_id, kind = pending
                if addr != address or (
                    expected_id is not None and sender.node_id != expected_id
                ):
                    return
                if payload.get("rpc") != kind or future.done():
                    return
                if "error" in payload:
                    future.set_exception(ValueError(str(payload["error"])))
                    return
                if kind in (FIND_NODE, FIND_VALUE) and not isinstance(
                    payload.get("nodes", []), list
                ):
                    return
                self._learn(sender)
                future.set_result({**payload, "sender": sender.to_dict()})
                return
            result = self._handle_request(message.type, payload)
            self._learn(sender)
            self._send(
                RESPONSE,
                {**result, "request_id": request_id, "rpc": message.type},
                addr,
            )
        except (
            ValueError,
            TypeError,
            KeyError,
            UnicodeError,
            OverflowError,
            RecursionError,
        ):
            # Bad control packets must not terminate the datagram callback.
            return

    def _handle_request(self, kind, payload):
        if kind == PING:
            return {"ok": True}
        if kind == FIND_NODE:
            target = normalize_id(payload["target_id"])
            return {"nodes": [c.to_dict() for c in self.routing.closest(target)]}
        key = normalize_id(payload["key"])
        if kind == STORE:
            self.store.put(key, payload["value"], payload.get("ttl", 300))
            return {"stored": True}
        found, value = self.store.get(key)
        return {
            "found": found,
            "value": value,
            "nodes": [c.to_dict() for c in self.routing.closest(key)],
        }

    def _learn(self, contact):
        oldest = self.routing.add(contact)
        if oldest is not None:
            index = (as_int(self.node_id) ^ as_int(contact.node_id)).bit_length() - 1
            if index not in self._probes:
                task = asyncio.create_task(self._replace_if_dead(oldest, contact))
                self._probes[index] = task
                task.add_done_callback(lambda done, i=index: self._probes.pop(i, None))

    async def _replace_if_dead(self, oldest, newcomer):
        if not await self.ping(oldest):
            self.routing.remove(oldest.node_id)
            self.routing.add(newcomer)

    async def ping(self, contact):
        try:
            await self.request(contact.addr, PING, {}, contact.node_id)
            return True
        except (TimeoutError, OSError, ValueError):
            self.routing.remove(contact.node_id)
            return False

    async def join(self, bootstrap):
        """Resolve one seed, then discover peers by looking up our own identity."""
        response = await self.request(bootstrap, PING, {})
        self._learn(Contact.from_dict(response["sender"]))
        return await self.lookup(self.node_id)

    async def lookup(self, target_id):
        return await iterative_lookup(self, target_id)

    def nearest_neighbours(self, n=3):
        return self.routing.closest(self.node_id, n)

    async def put(self, key, value, ttl=300):
        """Store locally and replicate to reachable nearest peers; return replica count."""
        key_id = make_node_id(key)
        self.store.put(key_id, value, ttl)
        peers = await self.lookup(key_id)
        results = await asyncio.gather(
            *(
                self.request(
                    c.addr,
                    STORE,
                    {"key": key_id, "value": value, "ttl": ttl},
                    c.node_id,
                )
                for c in peers
            ),
            return_exceptions=True
        )
        return 1 + sum(isinstance(r, dict) and r.get("stored") is True for r in results)

    async def get(self, key):
        key_id = make_node_id(key)
        found, value = self.store.get(key_id)
        if found:
            return value
        for peer in await self.lookup(key_id):
            try:
                result = await self.request(
                    peer.addr, FIND_VALUE, {"key": key_id}, peer.node_id
                )
            except (TimeoutError, OSError, ValueError):
                continue
            if result.get("found") is True:
                return result["value"]
        raise KeyError(key)

    async def refresh(self):
        """Probe known peers and refresh occupied buckets with random targets."""
        now = time.monotonic()
        stale_buckets = [
            i
            for i in range(BITS)
            if self.routing.buckets[i]
            and now - self.routing.updated[i] >= self.refresh_interval
        ]
        peers = self.routing.contacts()
        for offset in range(0, len(peers), self.alpha):
            await asyncio.gather(
                *(self.ping(c) for c in peers[offset : offset + self.alpha])
            )
        for index in stale_buckets:
            target = as_int(self.node_id) ^ ((1 << index) | secrets.randbits(index))
            await self.lookup(target)
            self.routing.updated[index] = time.monotonic()
        # Refresh self as well, so later joins propagate even in small meshes.
        await self.lookup(self.node_id)

    async def _maintain(self):
        while True:
            await asyncio.sleep(self.refresh_interval)
            await self.refresh()

    async def close(self):
        if self.closed:
            return
        self.closed = True
        tasks = list(self._probes.values())
        if self._maintenance is not None:
            tasks.append(self._maintenance)
        for task in tasks:
            task.cancel()
        for future, *_ in self._pending.values():
            if not future.done():
                future.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.transport.close()
        await asyncio.sleep(0)
