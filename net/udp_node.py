"""Asynchronous UDP networking for MeshWeaver."""

import asyncio
import json

from net.messages import PING, PONG, Message
from net.peer_manager import PeerConnectionManager


class UDPProtocol(asyncio.DatagramProtocol):
    """Receive UDP messages and track peer activity."""

    def __init__(self, node_id="node", peer_manager=None):
        self.transport = None
        self.node_id = node_id

        self.received_messages = []
        self.received_envelopes = []

        # Use a supplied manager or create one for this node.
        self.peer_manager = (
            peer_manager if peer_manager is not None else PeerConnectionManager()
        )

    def connection_made(self, transport):
        self.transport = transport
        print(f"UDP node {self.node_id} started")

    def datagram_received(self, data, addr):
        # Try to decode a structured MeshWeaver message.
        try:
            message = Message.decode(data)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            # Preserve support for plain-text messages.
            message = data.decode("utf-8", errors="replace")
            self.received_messages.append((message, addr))
            print(f"Received {message!r} from {addr}")
            return

        self.received_envelopes.append((message, addr))
        print(f"Received {message.type} from {message.sender_id}")

        # Record activity for the sender of a structured message.
        self.peer_manager.record_activity(message.sender_id)

        # Automatically reply to PING with PONG.
        if message.type == PING:
            reply = Message(
                type=PONG,
                sender_id=self.node_id,
                seq=message.seq,
                payload={"reply_to": message.sender_id},
            )
            self.send_message(reply, addr)

    def error_received(self, exc):
        print(f"UDP error: {exc}")

    def connection_lost(self, exc):
        print(f"UDP node {self.node_id} stopped")

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
):
    """Start a UDP node and return its transport and protocol."""
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(
            node_id=node_id,
            peer_manager=peer_manager,
        ),
        local_addr=(host, port),
    )

    return transport, protocol
