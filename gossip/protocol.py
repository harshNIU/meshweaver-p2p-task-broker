from gossip.state import NodeInfo


def merge_node_info(
    current: dict[str, NodeInfo],
    incoming: list[NodeInfo],
) -> dict[str, NodeInfo]:
    """Merge received node information into the current node table."""
    for node in incoming:
        existing = current.get(node.node_id)

        if existing is None or node.last_seen >= existing.last_seen:
            current[node.node_id] = node

    return current
