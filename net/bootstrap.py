"""Bootstrap helpers for joining the MeshWeaver network."""

import asyncio
from dataclasses import dataclass

from net.messages import PING, PONG, Message
from net.udp_node import start_udp_node


@dataclass(frozen=True)
class SeedAddress:
    """Address of one existing node used to join the network."""

    host: str
    port: int

    def __post_init__(self):
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("Seed host must not be empty")

        if not isinstance(self.port, int) or isinstance(self.port, bool):
            raise TypeError("Seed port must be an integer")

        if not 1 <= self.port <= 65535:
            raise ValueError("Seed port must be between 1 and 65535")

        object.__setattr__(self, "host", self.host.strip())


def parse_seed_address(address: str) -> SeedAddress:
    """Parse a seed address written as host:port."""
    if not isinstance(address, str) or not address.strip():
        raise ValueError("Seed address must not be empty")

    host, separator, port_text = address.strip().rpartition(":")

    if not separator or not host or not port_text:
        raise ValueError("Seed address must use host:port format")

    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError("Seed port must be an integer") from exc

    return SeedAddress(host=host, port=port)


async def join_via_seed(
    node_id: str,
    seed: SeedAddress,
    timeout: float = 2.0,
) -> bool:
    """Return True if the seed responds to a PING with a matching PONG."""
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
        raise TypeError("Timeout must be a positive number")

    if timeout <= 0:
        raise ValueError("Timeout must be positive")

    if not isinstance(seed, SeedAddress):
        raise TypeError("seed must be a SeedAddress")

    transport, protocol = await start_udp_node(node_id=node_id)

    try:
        ping = Message(
            type=PING,
            sender_id=node_id,
            seq=1,
            payload={"target": "seed"},
        )
        protocol.send_message(ping, (seed.host, seed.port))

        deadline = asyncio.get_running_loop().time() + timeout

        while asyncio.get_running_loop().time() < deadline:
            for message, _address in protocol.received_envelopes:
                if (
                    message.type == PONG
                    and message.seq == ping.seq
                    and message.sender_id != node_id
                ):
                    return True

            await asyncio.sleep(0.01)

        return False
    finally:
        transport.close()
