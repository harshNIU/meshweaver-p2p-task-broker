"""Run with python -m scripts.network_audit --nodes 10 (real UDP sockets)."""

import argparse
import asyncio
import json
import time

from dht import DHTNode, make_node_id


async def audit(count=10, base_port=0):
    if not 2 <= count <= 20:
        raise ValueError("Use 2 to 20 nodes for the all-peers audit (k=20)")
    nodes = []
    started = time.perf_counter()
    try:
        for i in range(count):
            node = await DHTNode.start(
                port=base_port + i if base_port else 0,
                node_id=make_node_id(f"audit-{i}"),
            )
            nodes.append(node)
            if i:
                await node.join(nodes[0].contact.addr)
        await asyncio.gather(*(node.lookup(node.node_id) for node in nodes))
        counts = [len(node.routing) for node in nodes]
        if counts != [count - 1] * count:
            raise AssertionError(f"Incomplete discovery: {counts}")
        replicas = await nodes[0].put("review", {"mesh": count})
        # A newly joined reader has no local copy and must use FIND_VALUE.
        reader = await DHTNode.start()
        nodes.append(reader)
        await reader.join(nodes[0].contact.addr)
        assert await reader.get("review") == {"mesh": count}
        report = {
            "status": "PASS",
            "nodes": count,
            "peers_per_node": counts,
            "store_replicas": replicas,
            "remote_find_value": True,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
        print(json.dumps(report, indent=2))
        return report
    finally:
        await asyncio.gather(*(node.close() for node in nodes))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--base-port", type=int, default=0)
    args = parser.parse_args()
    asyncio.run(audit(args.nodes, args.base_port))


if __name__ == "__main__":
    main()
