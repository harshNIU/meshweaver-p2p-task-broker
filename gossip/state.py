from dataclasses import dataclass
from typing import Literal

NodeState = Literal["ALIVE", "SUSPECT", "DEAD"]


@dataclass
class NodeInfo:
    """Store the status and resource usage of a network node."""

    node_id: str
    host: str
    port: int
    cpu: float = 0.0
    ram: float = 0.0
    last_seen: float = 0.0
    state: NodeState = "ALIVE"
    tcp_port: int | None = None
