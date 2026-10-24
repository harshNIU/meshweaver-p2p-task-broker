"""Bootstrap helpers for joining the MeshWeaver network."""

import asyncio
import math
from dataclasses import dataclass

from net.messages import PEER_LIST, PING, PONG, Message
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


def _validate_timeout(timeout: float) -> None:
    """Validate a finite, positive timeout."""
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
        raise TypeError("Timeout must be a positive number")

    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Timeout must be a finite positive number")


async def request_peer_list(
    protocol,
    node_id: str,
    seed: SeedAddress,
    timeout: float = 2.0,
) -> bool:
    """Ping a seed and wait for both its PONG and peer list."""
    _validate_timeout(timeout)

    if not isinstance(seed, SeedAddress):
        raise TypeError("seed must be a SeedAddress")

    ping = Message(
        type=PING,
        sender_id=node_id,
        seq=1,
        payload={"target": "seed"},
    )
    protocol.send_message(ping, (seed.host, seed.port))

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    got_pong = False
    got_peer_list = False

    while loop.time() < deadline:
        for message, address in protocol.received_envelopes:
            if message.sender_id == node_id:
                continue

            if message.type == PONG and message.seq == ping.seq:
                got_pong = True

            if (
                message.type == PEER_LIST
                and isinstance(address, tuple)
                and len(address) >= 2
                and address[1] == seed.port
            ):
                got_peer_list = True

        if got_pong and got_peer_list:
            return True

        await asyncio.sleep(0.01)

    return False


async def join_via_seed(
    node_id: str,
    seed: SeedAddress,
    timeout: float = 2.0,
) -> bool:
    """Join through a seed, returning whether discovery succeeded."""
    _validate_timeout(timeout)

    if not isinstance(seed, SeedAddress):
        raise TypeError("seed must be a SeedAddress")

    transport, protocol = await start_udp_node(node_id=node_id)

    try:
        return await request_peer_list(
            protocol=protocol,
            node_id=node_id,
            seed=seed,
            timeout=timeout,
        )
    finally:
        transport.close()
