"""Asynchronous TCP communication for MeshWeaver."""

import asyncio
import json

from net.messages import Message


async def read_message(reader: asyncio.StreamReader) -> Message:
    """Read one newline-delimited JSON message from a TCP stream."""
    raw = await reader.readline()

    if not raw:
        raise EOFError("TCP connection closed before a message was received")

    try:
        return Message.decode(raw.rstrip(b"\r\n"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError("Received an invalid TCP message") from exc


async def send_message(
    writer: asyncio.StreamWriter,
    message: Message,
) -> None:
    """Send one message over a TCP stream."""
    writer.write(message.encode() + b"\n")
    await writer.drain()


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    received_messages: list[Message],
) -> None:
    """Receive multiple messages from one connected TCP client."""
    try:
        while True:
            try:
                message = await read_message(reader)
            except EOFError:
                break

            received_messages.append(message)

            response = Message(
                type="ACK",
                sender_id="tcp-server",
                seq=message.seq,
                payload={
                    "received": True,
                    "original_type": message.type,
                },
            )
            await send_message(writer, response)

    except (ValueError, ConnectionError):
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass


async def start_tcp_server(
    host: str = "127.0.0.1",
    port: int = 0,
) -> tuple[asyncio.AbstractServer, list[Message]]:
    """Start a TCP server and return it with its received-message list."""
    received_messages: list[Message] = []

    async def client_connected(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await handle_client(reader, writer, received_messages)

    server = await asyncio.start_server(client_connected, host, port)
    return server, received_messages


async def send_tcp_message(
    host: str,
    port: int,
    message: Message,
) -> Message:
    """Connect to a TCP server, send a message, and return its response."""
    reader, writer = await asyncio.open_connection(host, port)

    try:
        await send_message(writer, message)
        return await read_message(reader)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass
