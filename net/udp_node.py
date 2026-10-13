
"""Asynchronous UDP networking for MeshWeaver."""

import asyncio


class UDPProtocol(asyncio.DatagramProtocol):
    """Receive UDP messages and report connection events."""

    def __init__(self):
        self.transport = None
        self.received_messages = []

    def connection_made(self, transport):
        self.transport = transport
        print("UDP node started")

    def datagram_received(self, data, addr):
        message = data.decode("utf-8")
        self.received_messages.append((message, addr))
        print(f"Received {message!r} from {addr}")

    def error_received(self, exc):
        print(f"UDP error: {exc}")

    def connection_lost(self, exc):
        print("UDP node stopped")

    def send_message(self, message, address):
        """Send a UTF-8 message to another UDP node."""
        if self.transport is None:
            raise RuntimeError("UDP node is not started")

        self.transport.sendto(message.encode("utf-8"), address)


async def start_udp_node(host="127.0.0.1", port=0):
    """Start a UDP node and return its transport and protocol."""
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        UDPProtocol,
        local_addr=(host, port),
    )

    return transport, protocol
