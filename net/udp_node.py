"""Asynchronous UDP networking for MeshWeaver."""

import asyncio
import json
from collections import OrderedDict

from execution.engine import ExecutionEngine
from execution.models import TaskStatus
from execution.serializer import TaskSerializationError, deserialize_task
from net.messages import PING, PONG, TASK_REQUEST, TASK_RESULT, Message
from net.peer_manager import PeerConnectionManager

MAX_CACHED_TASK_RESULTS = 1000


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

        self.peer_manager = (
            peer_manager if peer_manager is not None else PeerConnectionManager()
        )

    def connection_made(self, transport):
        self.transport = transport
        print(f"UDP node {self.node_id} started")

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
                # A retry can legitimately produce a repeated task result.
                # Keep it available to callers waiting for that retry.
                print(
                    f"Accepting repeated task result from "
                    f"{message.sender_id}, seq={message.seq}"
                )
                self.received_envelopes.append((message, addr))
            else:
                print(
                    f"Ignoring duplicate message from "
                    f"{message.sender_id}, seq={message.seq}"
                )
            return

        self.seen_messages.add(message_key)
        self.received_envelopes.append((message, addr))
        print(f"Received {message.type} from {message.sender_id}")
        self.peer_manager.record_activity(message.sender_id)

        if message.type == PING:
            reply = Message(
                type=PONG,
                sender_id=self.node_id,
                seq=message.seq,
                payload={"reply_to": message.sender_id},
            )
            self.send_message(reply, addr)

        elif message.type == TASK_REQUEST:
            self._handle_task_request(message, addr)

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

            # Check that the result is JSON-serializable before sending it.
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
        """Remove peers that have exceeded their activity timeout."""
        return self.peer_manager.remove_expired_peers(now=now)

    def send_message(self, message, address):
        """Send a plain-text or structured message over UDP."""
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
