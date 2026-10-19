"""Gossip engine for MeshWeaver cluster membership and node health."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence

from gossip.metrics import sample_load
from gossip.state import NodeInfo


# Callback receives a peer and the current list of known nodes.
SendNodesCallback = Callable[
    [NodeInfo, list[NodeInfo]],
    Awaitable[None],
]


class GossipEngine:
    """Manage local node information and known cluster nodes."""

    def __init__(self, self_node: NodeInfo):
        """Initialize the gossip engine with the local node."""
        self.self_node = self_node
        self.nodes: dict[str, NodeInfo] = {
            self_node.node_id: self_node
        }

    def refresh_self_metrics(self) -> NodeInfo:
        """Update the local node's CPU and RAM usage."""
        load = sample_load()

        self.self_node.cpu = load["cpu"]
        self.self_node.ram = load["ram"]

        self.nodes[self.self_node.node_id] = self.self_node

        return self.self_node

    def get_nodes(self) -> dict[str, NodeInfo]:
        """Return a copy of the currently known node table."""
        return self.nodes.copy()

    def merge_nodes(self, incoming: Sequence[NodeInfo]) -> None:
        """Merge received node information into the local node table."""
        for node in incoming:
            # Never replace our own local node with remote information.
            if node.node_id == self.self_node.node_id:
                continue

            existing = self.nodes.get(node.node_id)

            if existing is None:
                self.nodes[node.node_id] = node

            elif node.last_seen > existing.last_seen:
                # Prefer the most recently observed node information.
                self.nodes[node.node_id] = node

    def remove_dead_nodes(self) -> list[str]:
        """Remove DEAD remote nodes while retaining the local node."""
        dead_nodes = [
            node_id
            for node_id, node in self.nodes.items()
            if node.state == "DEAD"
            and node_id != self.self_node.node_id
        ]

        for node_id in dead_nodes:
            del self.nodes[node_id]

        return dead_nodes

    def get_cluster_load(self) -> dict[str, float | int]:
        """Return average CPU and RAM usage for alive cluster nodes."""
        alive_nodes = [
            node
            for node in self.nodes.values()
            if node.state == "ALIVE"
        ]

        if not alive_nodes:
            return {
                "average_cpu": 0.0,
                "average_ram": 0.0,
                "alive_nodes": 0,
            }

        average_cpu = sum(
            node.cpu for node in alive_nodes
        ) / len(alive_nodes)

        average_ram = sum(
            node.ram for node in alive_nodes
        ) / len(alive_nodes)

        return {
            "average_cpu": round(average_cpu, 2),
            "average_ram": round(average_ram, 2),
            "alive_nodes": len(alive_nodes),
        }

    async def gossip_once(
        self,
        send_nodes: SendNodesCallback,
    ) -> None:
        """Exchange known node information with alive remote peers."""
        self.refresh_self_metrics()

        # Take a snapshot before sending.
        known_nodes = list(self.nodes.values())

        peers = [
            node
            for node_id, node in self.nodes.items()
            if node_id != self.self_node.node_id
            and node.state == "ALIVE"
        ]

        if not peers:
            return

        # A failed peer must not prevent gossip with other peers.
        await asyncio.gather(
            *(
                self._send_safely(send_nodes, peer, known_nodes)
                for peer in peers
            )
        )

    async def _send_safely(
        self,
        send_nodes: SendNodesCallback,
        peer: NodeInfo,
        known_nodes: list[NodeInfo],
    ) -> None:
        """Attempt to send gossip without stopping other peer sends."""
        try:
            await send_nodes(peer, known_nodes)
        except Exception:
            # Network integration should log or handle the failure.
            # Heartbeat monitoring determines peer health separately.
            return

    async def run_gossip(
        self,
        send_nodes: SendNodesCallback,
        interval: float = 5.0,
    ) -> None:
        """Run periodic gossip until the task is cancelled."""
        if interval <= 0:
            raise ValueError("Gossip interval must be greater than zero.")

        while True:
            await self.gossip_once(send_nodes)
            await asyncio.sleep(interval)