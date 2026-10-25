
"""Iterative alpha-parallel lookup with bounded candidates and dead-peer handling."""

import asyncio

from dht.ids import distance, normalize_id
from dht.routing_table import Contact
from dht.rpc import FIND_NODE


async def iterative_lookup(node, target_id):
    """Find responsive peers and learn discovered contacts."""
    target_id = normalize_id(target_id)

    candidates = {
        contact.node_id: contact
        for contact in node.routing.closest(target_id)
    }
    queried = set()
    responsive = {}

    while True:
        nearest = sorted(
            candidates.values(),
            key=lambda contact: distance(contact.node_id, target_id),
        )

        batch = [
            contact
            for contact in nearest[: node.k]
            if contact.node_id not in queried
        ][: node.alpha]

        if not batch:
            break

        queried.update(contact.node_id for contact in batch)

        results = await asyncio.gather(
            *(
                node.request(
                    contact.addr,
                    FIND_NODE,
                    {"target_id": target_id},
                    contact.node_id,
                )
                for contact in batch
            ),
            return_exceptions=True,
        )

        for peer, result in zip(batch, results):
            # Retry once before treating the peer as unresponsive.
            if isinstance(result, Exception):
                try:
                    result = await node.request(
                        peer.addr,
                        FIND_NODE,
                        {"target_id": target_id},
                        peer.node_id,
                    )
                except Exception:
                    candidates.pop(peer.node_id, None)

                    # Keep the routing entry. A failed lookup alone
                    # does not prove that the peer is dead.
                    continue

            responsive[peer.node_id] = peer
            node._learn(peer)

            for raw in result.get("nodes", [])[: node.k]:
                try:
                    contact = Contact.from_dict(raw)
                except (KeyError, TypeError, ValueError):
                    continue

                if contact.node_id == node.node_id:
                    continue

                candidates[contact.node_id] = contact
                node._learn(contact)

        candidates = {
            contact.node_id: contact
            for contact in sorted(
                candidates.values(),
                key=lambda contact: distance(contact.node_id, target_id),
            )[: node.k * 4]
        }

        if len(queried) >= node.k * 16:
            break

    return sorted(
        responsive.values(),
        key=lambda contact: distance(contact.node_id, target_id),
    )[: node.k]
