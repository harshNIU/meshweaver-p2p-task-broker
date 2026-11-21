import asyncio
import time
from typing import Callable

from gossip.state import NodeInfo


class HeartbeatMonitor:
    """Monitor node heartbeats and detect failed nodes."""

    def __init__(
        self,
        nodes: dict[str, NodeInfo],
        suspect_after: float = 6.0,
        dead_after: float = 10.0,
    ):
        self.nodes = nodes
        self.suspect_after = suspect_after
        self.dead_after = dead_after

    def record_heartbeat(self, node_id: str) -> None:
        """Record that a node is responding."""
        node = self.nodes.get(node_id)
        if node is not None:
            node.last_seen = time.time()
            node.state = "ALIVE"

    def check_nodes(self) -> dict[str, str]:
        """Update node states based on their last heartbeat."""
        now = time.time()
        statuses = {}

        for node_id, node in self.nodes.items():
            elapsed = now - node.last_seen

            if elapsed >= self.dead_after:
                node.state = "DEAD"
            elif elapsed >= self.suspect_after:
                node.state = "SUSPECT"
            else:
                node.state = "ALIVE"

            statuses[node_id] = node.state

        return statuses

    async def monitor(
        self,
        interval: float = 2.0,
        on_dead: Callable[[str], None] | None = None,
    ) -> None:
        """Continuously check node health until cancelled."""
        while True:
            statuses = self.check_nodes()

            if on_dead is not None:
                for node_id, status in statuses.items():
                    if status == "DEAD":
                        on_dead(node_id)

            await asyncio.sleep(interval)