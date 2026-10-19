from gossip.metrics import sample_load
from gossip.state import NodeInfo


class GossipEngine:
    """Manage local node metrics and known cluster nodes."""

    def __init__(self, self_node: NodeInfo):
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
        """Return the current known nodes."""
        return self.nodes.copy()

    def merge_nodes(self, incoming: list[NodeInfo]) -> None:
        """Merge received node information into the local node table."""
        for node in incoming:
            existing = self.nodes.get(node.node_id)

            if existing is None or node.last_seen > existing.last_seen:
                self.nodes[node.node_id] = node