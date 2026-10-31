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
from net.tcp_node import send_tcp_message

MAX_CACHED_TASK_RESULTS = 1000
MAX_SEEN_MESSAGES = 10000
# Conservative UDP payload threshold to avoid IP fragmentation.
MAX_UDP_MESSAGE_SIZE = 1200
HEARTBEAT_INTERVAL_SECONDS = 5.0
VALID_NODE_STATES = {"ALIVE", "SUSPECT", "DEAD"}
DEFAULT_SEND_QUEUE_SIZE = 100
DEFAULT_SEND_RATE_LIMIT = 100.0


class UDPProtocol(asyncio.DatagramProtocol):
    """Receive UDP messages, track peers, and process task requests."""

    def __init__(
        self,
        node_id="node",
        peer_manager=None,
        execution_engine=None,
        send_queue_size=DEFAULT_SEND_QUEUE_SIZE,
        send_rate_limit=DEFAULT_SEND_RATE_LIMIT,
    ):
        """Initialize the UDP protocol and its bounded outgoing queue."""
        if not isinstance(send_queue_size, int) or isinstance(send_queue_size, bool):
            raise TypeError("send_queue_size must be a positive integer")
        if send_queue_size <= 0:
            raise ValueError("send_queue_size must be greater than zero")

        if (
            not isinstance(send_rate_limit, (int, float))
            or isinstance(send_rate_limit, bool)
        ):
            raise TypeError("send_rate_limit must be a positive number")
        if not math.isfinite(send_rate_limit) or send_rate_limit <= 0:
            raise ValueError("send_rate_limit must be finite and greater than zero")

        self.transport = None
        self.node_id = node_id
        self.execution_engine = (
            execution_engine if execution_engine is not None else ExecutionEngine()
        )

        self.send_rate_limit = float(send_rate_limit)
        self.send_queue = asyncio.Queue(maxsize=send_queue_size)
        self._send_queue_task = None

        self.received_messages = []
        self.received_envelopes = []
        self.tcp_send_errors = []
        self._tcp_send_tasks = set()
        self.seen_messages = OrderedDict()
        self._task_results = OrderedDict()
        self._peer_list_seq = int(time.time_ns())
        self._cleanup_task = None
        self._heartbeat_task = None
        self._heartbeat_seq = int(time.time_ns())

        self.peer_manager = (
            peer_manager if peer_manager is not None else PeerConnectionManager()
        )
        self.node_registry = {}
        self.tcp_host = None
        self.tcp_port = None

    def connection_made(self, transport):
        """Store the UDP transport and start background peer cleanup."""
        self.transport = transport
        self._refresh_self_node()
        self._start_send_queue_worker()
        self._start_peer_cleanup()
        self._start_peer_heartbeat()
        print(f"UDP node {self.node_id} started")

    def _start_send_queue_worker(self):
        """Start the background task that drains the outgoing UDP queue."""
        if self._send_queue_task is not None and not self._send_queue_task.done():
            return

        self._send_queue_task = asyncio.create_task(self._send_queue_worker())

    async def _send_queue_worker(self):
        """Send queued datagrams in FIFO order while respecting the rate limit."""
        send_interval = 1.0 / self.send_rate_limit

        while self.transport is not None and not self.transport.is_closing():
            data, address = await self.send_queue.get()
            try:
                transport = self.transport
                if transport is None or transport.is_closing():
                    continue

                transport.sendto(data, address)
                await asyncio.sleep(send_interval)
            except OSError as exc:
                print(f"UDP send failed for node {self.node_id}: {exc}")
            finally:
                self.send_queue.task_done()

    def _start_peer_cleanup(self):
        """Start the periodic stale-peer cleanup task."""
        if self._cleanup_task is not None and not self._cleanup_task.done():
            return

        self._cleanup_task = asyncio.create_task(self._cleanup_expired_peers_loop())

    def _start_peer_heartbeat(self):
        """Start periodic heartbeats to known peers."""
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return

        self._heartbeat_task = asyncio.create_task(self._peer_heartbeat_loop())

    async def _peer_heartbeat_loop(self):
        """Send periodic PING messages to known peers."""
        while self.transport is not None and not self.transport.is_closing():
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)

            if self.transport is None or self.transport.is_closing():
                break

            for peer_id, address in list(self.peer_manager.addresses.items()):
                if peer_id == self.node_id or not address:
                    continue

                self._heartbeat_seq += 1
                ping = Message(
                    type=PING,
                    sender_id=self.node_id,
                    seq=self._heartbeat_seq,
                    payload={"target": peer_id},
                )
                try:
                    self.send_message(ping, address)
                except (RuntimeError, OSError):
                    continue

    async def _cleanup_expired_peers_loop(self):
        """Periodically remove peers that have exceeded their timeout."""
        timeout = self.peer_manager.timeout_seconds
        interval = max(0.01, min(timeout / 2, 1.0))

        while self.transport is not None and not self.transport.is_closing():
            await asyncio.sleep(interval)

            if self.transport is None or self.transport.is_closing():
                break

            removed = self.cleanup_expired_peers()
            if removed:
                print(
                    f"UDP node {self.node_id} removed expired peers: "
                    f"{', '.join(removed)}"
                )

    def set_tcp_endpoint(self, host, port):
        """Register this node's TCP endpoint for peer discovery."""
        if not isinstance(host, str) or not host.strip():
            raise ValueError("TCP host must be a non-empty string")

        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError("TCP port must be between 1 and 65535")

        self.tcp_host = host
        self.tcp_port = port
        self._refresh_self_node()

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
            host=self.tcp_host or address[0],
            port=address[1],
            cpu=existing.cpu if existing else 0.0,
            ram=existing.ram if existing else 0.0,
            last_seen=now,
            state="ALIVE",
            tcp_port=self.tcp_port,
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
            tcp_port=existing.tcp_port if existing else None,
        )
        merge_node_info(self.node_registry, [node])

    def _remember_message(self, message_key):
        """Remember a message while keeping the duplicate cache bounded."""
        self.seen_messages[message_key] = None
        self.seen_messages.move_to_end(message_key)

        while len(self.seen_messages) > MAX_SEEN_MESSAGES:
            self.seen_messages.popitem(last=False)

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

        # Any valid structured packet proves the peer is currently reachable.
        self._record_direct_peer(message.sender_id, addr)

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

        self._remember_message(message_key)
        self.received_envelopes.append((message, addr))
        print(f"Received {message.type} from {message.sender_id}")

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
        tcp_port = record.get("tcp_port")
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

        # TCP is optional so older peer records remain compatible.
        if tcp_port is not None and (
            not isinstance(tcp_port, int)
            or isinstance(tcp_port, bool)
            or not 1 <= tcp_port <= 65535
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

        # FIX: Reject non-string states before checking set membership.
        if not isinstance(state, str) or state not in VALID_NODE_STATES:
            return None

        return NodeInfo(
            node_id=node_id,
            host=host,
            port=port,
            cpu=float(cpu),
            ram=float(ram),
            last_seen=float(last_seen),
            state=state,
            tcp_port=tcp_port,
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

            if node.tcp_port is not None:
                self.peer_manager.record_tcp_address(
                    node.node_id,
                    (node.host, node.tcp_port),
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
        """Cancel background cleanup when the UDP transport closes."""
        task = self._cleanup_task
        self._cleanup_task = None

        if task is not None and not task.done():
            task.cancel()

        heartbeat_task = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat_task is not None and not heartbeat_task.done():
            heartbeat_task.cancel()

        send_queue_task = self._send_queue_task
        self._send_queue_task = None
        if send_queue_task is not None and not send_queue_task.done():
            send_queue_task.cancel()

        for tcp_task in list(self._tcp_send_tasks):
            if not tcp_task.done():
                tcp_task.cancel()
        self._tcp_send_tasks.clear()

        self.transport = None
        print(f"UDP node {self.node_id} stopped")

    def cleanup_expired_peers(self, now=None):
        """Remove expired peers and their saved addresses."""
        removed = self.peer_manager.remove_expired_peers(now=now)
        for peer_id in removed:
            if peer_id != self.node_id:
                self.node_registry.pop(peer_id, None)
        return removed

    def _tcp_address_for_udp_peer(self, udp_address):
        """Find the advertised TCP endpoint for a known UDP peer."""
        for peer_id, known_udp_address in self.peer_manager.addresses.items():
            if tuple(known_udp_address[:2]) == tuple(udp_address[:2]):
                tcp_address = self.peer_manager.get_peer_tcp_address(peer_id)
                if tcp_address is not None:
                    return tuple(tcp_address)
                return None
        return None

    async def _send_large_message_tcp(self, message, tcp_address, udp_address):
        """Send a large structured message over TCP and record its response."""
        try:
            response = await send_tcp_message(
                tcp_address[0],
                tcp_address[1],
                message,
            )
            self.received_envelopes.append((response, udp_address))
        except (OSError, ConnectionError, ValueError, TimeoutError) as exc:
            self.tcp_send_errors.append((message, tcp_address, exc))
            print(
                f"TCP send failed from node {self.node_id} to "
                f"{tcp_address}: {exc}"
            )


    def _track_tcp_send_task(self, task):
        """Keep TCP send tasks referenced until they finish."""
        self._tcp_send_tasks.add(task)
        task.add_done_callback(self._tcp_send_tasks.discard)

    def send_message(self, message, address):
        """Send small messages through UDP and route large messages through TCP."""
        if self.transport is None or self.transport.is_closing():
            raise RuntimeError("UDP node is not started")

        if isinstance(message, Message):
            data = message.encode()
        elif isinstance(message, str):
            data = message.encode("utf-8")
        else:
            raise TypeError("message must be a Message or string")

        if len(data) > MAX_UDP_MESSAGE_SIZE:
            if not isinstance(message, Message):
                raise ValueError(
                    "Large UDP payloads require a structured Message for TCP fallback"
                )

            tcp_address = self._tcp_address_for_udp_peer(address)
            if tcp_address is None:
                raise RuntimeError(
                    f"No TCP endpoint is registered for UDP peer {address}"
                )

            task = asyncio.create_task(
                self._send_large_message_tcp(message, tcp_address, address)
            )
            self._track_tcp_send_task(task)
            return

        try:
            self.send_queue.put_nowait((data, address))
        except asyncio.QueueFull as exc:
            raise BufferError("UDP send queue is full") from exc



async def start_udp_node(
    host="127.0.0.1",
    port=0,
    node_id="node",
    peer_manager=None,
    execution_engine=None,
    send_queue_size=DEFAULT_SEND_QUEUE_SIZE,
    send_rate_limit=DEFAULT_SEND_RATE_LIMIT,
):
    """Start a UDP node and return its transport and protocol."""
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(
            node_id=node_id,
            peer_manager=peer_manager,
            execution_engine=execution_engine,
            send_queue_size=send_queue_size,
            send_rate_limit=send_rate_limit,
        ),
        local_addr=(host, port),
    )

    return transport, protocol
