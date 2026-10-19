from gossip.metrics import sample_load
from gossip.state import NodeInfo


class GossipEngine:
    """Manage local node information and known cluster nodes."""

    def __init__(self, self_node: NodeInfo):
        """Initialize the gossip engine with the local node."""
        self.self_node = self_node
        self.nodes: dict[str, NodeInfo] = {
            self_node.node_id: self_node
        }

    def refresh_self_metrics(self) -> NodeInfo:
        """Update this node's CPU and RAM usage."""
        load = sample_load()

        self.self_node.cpu = load["cpu"]
        self.self_node.ram = load["ram"]

        self.nodes[self.self_node.node_id] = self.self_node

        return self.self_node

    def get_nodes(self) -> dict[str, NodeInfo]:
        """Return a copy of the currently known nodes."""
        return self.nodes.copy()

    def merge_nodes(self, incoming: list[NodeInfo]) -> None:
        """Merge received node information into the local node table."""
        for node in incoming:
            if node.node_id == self.self_node.node_id:
                continue

            existing = self.nodes.get(node.node_id)

            if existing is None:
                self.nodes[node.node_id] = node

            elif node.last_seen > existing.last_seen:
                self.nodes[node.node_id] = node

    def remove_dead_nodes(self) -> list[str]:
        """Remove dead remote nodes while retaining the local node."""
        dead_nodes = [
            node_id
            for node_id, node in self.nodes.items()
            if node.state == "DEAD"
            and node_id != self.self_node.node_id
        ]

        for node_id in dead_nodes:
            del self.nodes[node_id]

        return dead_nodes