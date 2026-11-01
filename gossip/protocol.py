from dataclasses import replace

from gossip.state import NodeInfo


def merge_node_info(
    current: dict[str, NodeInfo],
    incoming: list[NodeInfo],
) -> dict[str, NodeInfo]:
    """Merge received node information into the current node table."""
    for node in incoming:
        existing = current.get(node.node_id)

        if existing is None:
            current[node.node_id] = node

        elif node.last_seen >= existing.last_seen:
            # Preserve a previously known TCP port if the incoming
            # record does not advertise one.
            if node.tcp_port is None and existing.tcp_port is not None:
                node = replace(node, tcp_port=existing.tcp_port)

            current[node.node_id] = node

    return current
