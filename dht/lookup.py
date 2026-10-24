"""Iterative alpha-parallel lookup with bounded candidates and dead-peer removal."""

import asyncio

from dht.ids import distance, normalize_id
from dht.routing_table import Contact
from dht.rpc import FIND_NODE


async def iterative_lookup(node, target_id):
    target_id = normalize_id(target_id)
    candidates = {c.node_id: c for c in node.routing.closest(target_id)}
    queried = set()
    responsive = {}
    while True:
        nearest = sorted(
            candidates.values(), key=lambda c: distance(c.node_id, target_id)
        )
        batch = [c for c in nearest[: node.k] if c.node_id not in queried][: node.alpha]
        if not batch:
            break
        queried.update(c.node_id for c in batch)
        results = await asyncio.gather(
            *(
                node.request(c.addr, FIND_NODE, {"target_id": target_id}, c.node_id)
                for c in batch
            ),
            return_exceptions=True,
        )
        for peer, result in zip(batch, results):
            if isinstance(result, Exception):
                candidates.pop(peer.node_id, None)
                node.routing.remove(peer.node_id)
                continue
            responsive[peer.node_id] = peer
            for raw in result.get("nodes", [])[: node.k]:
                try:
                    contact = Contact.from_dict(raw)
                except (KeyError, TypeError, ValueError):
                    continue
                if contact.node_id != node.node_id and contact.node_id not in queried:
                    candidates[contact.node_id] = contact
        # Bound memory even if a peer returns an endless stream of new IDs.
        candidates = {
            c.node_id: c
            for c in sorted(
                candidates.values(), key=lambda c: distance(c.node_id, target_id)
            )[: node.k * 4]
        }
        if len(queried) >= node.k * 16:
            break
    return sorted(responsive.values(), key=lambda c: distance(c.node_id, target_id))[
        : node.k
    ]
