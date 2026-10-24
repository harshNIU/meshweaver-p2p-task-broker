
"""Asynchronous UDP networking for MeshWeaver."""

import asyncio
import json
import math
import time
from collections import OrderedDict
from dataclasses import asdict

from execution.engine import ExecutionEngine
from execution.models import TaskStatus
from execution.serializer import TaskSerializationError, deserialize_task
from gossip.protocol import merge_node_info
from gossip.state import NodeInfo
from net.messages import (
    PEER_LIST,
    PING,
    PONG,
    TASK_REQUEST,
    TASK_RESULT,
    Message,
)
from net.peer_manager import PeerConnectionManager

MAX_CACHED_TASK_RESULTS = 1000
VALID_NODE_STATES = {"ALIVE", "SUSPECT", "DEAD"}


class UDPProtocol(asyncio.DatagramProtocol):
    """Receive UDP messages, track peers, and process task requests."""

    def __init__(self, node_id="node", peer_manager=None, execution_engine=None):
        self.transport = None
        self.node_id = node_id
        self.execution_engine = (
            execution_engine if execution_engine is not None else ExecutionEngine()
        )

        self.received_messages = []
        self.received_envelopes = []
        self.seen_messages = set()
        self._task_results = OrderedDict()
        self._peer_list_seq = int(time.time_ns())

        self.peer_manager = (
            peer_manager if peer_manager is not None else PeerConnectionManager()
        )
        self.node_registry = {}

    def connection_made(self, transport):
        """Store the UDP transport when the node starts."""
        self.transport = transport
        self._refresh_self_node()
        print(f"UDP node {self.node_id} started")

    def _refresh_self_node(self):
        """Update this node's advertised address in the registry."""
        if self.transport is None:
            return

        address = self.transport.get_extra_info("sockname")
        if not address or len(address) < 2:
            return

        now = time.time()
        existing = self.node_registry.get(self.node_id)
        self.node_registry[self.node_id] = NodeInfo(
            node_id=self.node_id,
            host=address[0],
            port=address[1],
            cpu=existing.cpu if existing else 0.0,
            ram=existing.ram if existing else 0.0,
            last_seen=now,
            state="ALIVE",
        )

    def _record_direct_peer(self, peer_id, address):
        """Record a directly observed peer in the activity tracker and registry."""
        if not isinstance(peer_id, str) or not peer_id or peer_id == self.node_id:
            return

        host, port = address[0], address[1]
        self.peer_manager.record_activity(peer_id, address=address)

        existing = self.node_registry.get(peer_id)
        node = NodeInfo(
            node_id=peer_id,
            host=host,
            port=port,
            cpu=existing.cpu if existing else 0.0,
            ram=existing.ram if existing else 0.0,
            last_seen=time.time(),
            state="ALIVE",
        )
        merge_node_info(self.node_registry, [node])

    def datagram_received(self, data, addr):
        """Handle incoming plain-text or structured UDP messages."""
        try:
            message = Message.decode(data)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            text_message = data.decode("utf-8", errors="replace")
            self.received_messages.append((text_message, addr))
            print(f"Received {text_message!r} from {addr}")
            return

        message_key = (message.sender_id, message.seq)

        if message_key in self.seen_messages:
            if message.type == TASK_REQUEST:
                self._resend_cached_task_result(message, addr)
            elif message.type == TASK_RESULT:
                self.received_envelopes.append((message, addr))
                print(
                    f"Accepting repeated task result from "
                    f"{message.sender_id}, seq={message.seq}"
                )
            else:
                print(
                    f"Ignoring duplicate message from "
                    f"{message.sender_id}, seq={message.seq}"
                )
            return

        self.seen_messages.add(message_key)
        self.received_envelopes.append((message, addr))
        print(f"Received {message.type} from {message.sender_id}")

        self._record_direct_peer(message.sender_id, addr)

        if message.type == PING:
            reply = Message(
                type=PONG,
                sender_id=self.node_id,
                seq=message.seq,
                payload={"reply_to": message.sender_id},
            )
            self.send_message(reply, addr)
            self.send_peer_list(addr)

        elif message.type == PEER_LIST:
            self._merge_peer_list(message.payload)

        elif message.type == TASK_REQUEST:
            self._handle_task_request(message, addr)

    def _next_peer_list_seq(self):
        """Return a sequence number for an outgoing peer-list message."""
        self._peer_list_seq += 1
        return self._peer_list_seq

    def send_peer_list(self, address):
        """Send known node information to a directly connected peer."""
        self._refresh_self_node()
        nodes = [
            asdict(node)
            for node_id, node in self.node_registry.items()
            if node_id != self.node_id
        ]

        self_info = self.node_registry.get(self.node_id)
        if self_info is not None:
            nodes.insert(0, asdict(self_info))

        message = Message(
            type=PEER_LIST,
            sender_id=self.node_id,
            seq=self._next_peer_list_seq(),
            payload={"nodes": nodes},
        )
        self.send_message(message, address)

    @staticmethod
    def _node_info_from_payload(record):
        """Validate and convert a peer-list record into NodeInfo."""
        if not isinstance(record, dict):
            return None

        node_id = record.get("node_id")
        host = record.get("host")
        port = record.get("port")
        cpu = record.get("cpu", 0.0)
        ram = record.get("ram", 0.0)
        last_seen = record.get("last_seen", time.time())
        state = record.get("state", "ALIVE")

        if not isinstance(node_id, str) or not node_id.strip():
            return None
        if not isinstance(host, str) or not host.strip():
            return None
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            return None

        numeric_values = (cpu, ram, last_seen)
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for value in numeric_values
        ):
            return None

        if cpu < 0 or ram < 0 or last_seen < 0:
            return None
        if state not in VALID_NODE_STATES:
            return None

        return NodeInfo(
            node_id=node_id,
            host=host,
            port=port,
            cpu=float(cpu),
            ram=float(ram),
            last_seen=float(last_seen),
            state=state,
        )

    def _merge_peer_list(self, payload):
        """Validate and merge peer records received from another node."""
        if not isinstance(payload, dict):
            return

        records = payload.get("nodes")
        if not isinstance(records, list):
            return

        incoming = []
        for record in records:
            node = self._node_info_from_payload(record)
            if node is None or node.node_id == self.node_id:
                continue
            incoming.append(node)

        merge_node_info(self.node_registry, incoming)

        for node in incoming:
            if node.node_id == self.node_id:
                continue
            self.peer_manager.record_activity(
                node.node_id,
                address=(node.host, node.port),
            )

    def _cache_task_result(self, sender_id, task_id, payload):
        """Cache a task result with a bounded number of entries."""
        key = (sender_id, task_id)
        self._task_results[key] = payload
        self._task_results.move_to_end(key)

        while len(self._task_results) > MAX_CACHED_TASK_RESULTS:
            self._task_results.popitem(last=False)

    def _resend_cached_task_result(self, message, addr):
        """Resend a cached result when a duplicate task request arrives."""
        payload = message.payload
        if not isinstance(payload, dict):
            return

        task_data = payload.get("task")
        if not isinstance(task_data, dict):
            return

        task_id = task_data.get("task_id")
        if not isinstance(task_id, str):
            return

        cached = self._task_results.get((message.sender_id, task_id))
        if cached is None:
            return

        reply = Message(
            type=TASK_RESULT,
            sender_id=self.node_id,
            seq=message.seq,
            payload=cached,
        )
        self.send_message(reply, addr)

    def _handle_task_request(self, message, addr):
        """Execute a valid task request and return its result."""
        payload = message.payload
        task_data = payload.get("task") if isinstance(payload, dict) else None

        if not isinstance(task_data, dict):
            result_payload = self._failure_payload(
                None, "Task request payload must contain a task object."
            )
            self._send_task_result(message, addr, result_payload)
            return

        task_id = task_data.get("task_id")

        if isinstance(task_id, str):
            cached = self._task_results.get((message.sender_id, task_id))
            if cached is not None:
                self._send_task_result(message, addr, cached)
                return

        try:
            task = deserialize_task(json.dumps(task_data))
            result = self.execution_engine.execute(task)

            result_payload = {
                "task_id": result.task_id,
                "status": result.status.value,
                "attempts": result.attempts,
                "value": result.value,
                "error": result.error,
                "duration_seconds": result.duration_seconds,
            }

            json.dumps(result_payload)

        except (TaskSerializationError, TypeError, ValueError) as exc:
            result_payload = self._failure_payload(
                task_id if isinstance(task_id, str) else None,
                f"Invalid task request or result: {exc}",
            )

        if isinstance(task_id, str):
            self._cache_task_result(message.sender_id, task_id, result_payload)

        self._send_task_result(message, addr, result_payload)

    @staticmethod
    def _failure_payload(task_id, error):
        """Create a consistent task failure response."""
        return {
            "task_id": task_id,
            "status": TaskStatus.FAILED.value,
            "attempts": 0,
            "value": None,
            "error": error,
            "duration_seconds": 0.0,
        }

    def _send_task_result(self, request, addr, payload):
        """Send a task result to the requester."""
        reply = Message(
            type=TASK_RESULT,
            sender_id=self.node_id,
            seq=request.seq,
            payload=payload,
        )
        self.send_message(reply, addr)

    def error_received(self, exc):
        """Report UDP transport errors."""
        print(f"UDP error: {exc}")

    def connection_lost(self, exc):
        """Report when the UDP transport closes."""
        print(f"UDP node {self.node_id} stopped")

    def cleanup_expired_peers(self, now=None):
        """Remove expired peers and their saved addresses."""
        removed = self.peer_manager.remove_expired_peers(now=now)
        for peer_id in removed:
            if peer_id != self.node_id:
                self.node_registry.pop(peer_id, None)
        return removed

    def send_message(self, message, address):
        """Send a plain-text or structured UDP message."""
        if self.transport is None:
            raise RuntimeError("UDP node is not started")

        if isinstance(message, Message):
            data = message.encode()
        else:
            data = message.encode("utf-8")

        self.transport.sendto(data, address)


async def start_udp_node(
    host="127.0.0.1",
    port=0,
    node_id="node",
    peer_manager=None,
    execution_engine=None,
):
    """Start a UDP node and return its transport and protocol."""
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(
            node_id=node_id,
            peer_manager=peer_manager,
            execution_engine=execution_engine,
        ),
        local_addr=(host, port),
    )

    return transport, protocol
