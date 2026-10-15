
"""Asynchronous UDP networking for MeshWeaver."""

import asyncio
import json

from net.messages import Message, PING, PONG


class UDPProtocol(asyncio.DatagramProtocol):
    """Receive UDP messages and respond to PING requests."""

    def __init__(self, node_id="node"):
        self.transport = None
        self.node_id = node_id

        # Keep plain-text messages compatible with the existing test.
        self.received_messages = []

        # Store structured MeshWeaver messages separately.
        self.received_envelopes = []

    def connection_made(self, transport):
        self.transport = transport
        print(f"UDP node {self.node_id} started")

    def datagram_received(self, data, addr):
        # First, try to decode a structured MeshWeaver message.
        try:
            message = Message.decode(data)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            # Fall back to the plain-text behavior from Day 2.
            message = data.decode("utf-8", errors="replace")
            self.received_messages.append((message, addr))
            print(f"Received {message!r} from {addr}")
            return

        self.received_envelopes.append((message, addr))
        print(f"Received {message.type} from {message.sender_id}")

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


async def start_udp_node(host="127.0.0.1", port=0, node_id="node"):
    """Start a UDP node and return its transport and protocol."""
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(node_id=node_id),
        local_addr=(host, port),
    )

    return transport, protocol
