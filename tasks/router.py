"""Deterministic lowest-CPU selection from the shared NodeInfo snapshot."""

import math
import time
from collections.abc import Mapping


class NoAvailableNodes(RuntimeError):
    """No fresh, healthy worker satisfies the routing policy."""


def select_node(
    nodes, *, exclude=(), max_cpu=95.0, max_ram=95.0, max_age=15.0, now=None
):
    if not 0 < max_cpu <= 100 or not 0 < max_ram <= 100:
        raise ValueError("load thresholds must be in (0, 100]")
    if not math.isfinite(max_age) or max_age <= 0:
        raise ValueError("max_age must be finite and positive")
    now = time.time() if now is None else now
    values = nodes.values() if isinstance(nodes, Mapping) else nodes
    excluded = set(exclude)
    candidates = []
    for node in values:
        if node.node_id in excluded or node.state != "ALIVE":
            continue
        metrics = (node.cpu, node.ram, node.last_seen)
        if any(
            isinstance(x, bool)
            or not isinstance(x, (int, float))
            or not math.isfinite(x)
            for x in metrics
        ):
            continue
        if not (0 <= node.cpu < max_cpu and 0 <= node.ram < max_ram):
            continue
        if node.last_seen <= 0 or not 0 <= now - node.last_seen <= max_age:
            continue
        candidates.append(node)
    if not candidates:
        raise NoAvailableNodes("No fresh ALIVE worker below CPU/RAM thresholds")
    return min(candidates, key=lambda node: (node.cpu, node.ram, node.node_id))
