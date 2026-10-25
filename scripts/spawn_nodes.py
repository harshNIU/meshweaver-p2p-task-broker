"""Ten independent Python processes, each running its own UDP DHT node.

The parent is a test controller only. Children discover contacts through the
DHT; the controller supplies only the seed address to each joining child.
"""

import argparse
import asyncio
import json
import multiprocessing
import time

from dht import DHTNode, make_node_id


async def _serve(connection, index, port):
    node = await DHTNode.start(port=port, node_id=make_node_id(f"process-{index}"))
    try:
        connection.send(node.contact.to_dict())
        while True:
            command, argument = await asyncio.to_thread(connection.recv)
            if command == "stop":
                break
            if command == "join":
                await node.join(tuple(argument))
                connection.send(True)
            elif command == "lookup":
                await node.lookup(node.node_id)
                connection.send(sorted(c.node_id for c in node.routing.contacts()))
            elif command == "put":
                connection.send(await node.put("audit", argument))
            elif command == "get":
                connection.send(await node.get("audit"))
    finally:
        await node.close()
        connection.close()


def _child(connection, index, port):
    try:
        asyncio.run(_serve(connection, index, port))
    except Exception as exc:
        connection.send({"error": f"{type(exc).__name__}: {exc}"})
        raise


def _receive(connection):
    if not connection.poll(20):
        raise TimeoutError("Child did not respond within 20 seconds")
    value = connection.recv()
    if isinstance(value, dict) and "error" in value:
        raise RuntimeError(value["error"])
    return value


def audit(count=10, base_port=9000):
    if not 2 <= count <= 20:
        raise ValueError("Use 2 to 20 nodes for an all-peers audit")
    if base_port and not 1 <= base_port <= 65536 - count:
        raise ValueError("Invalid base port range")
    context = multiprocessing.get_context("spawn")
    processes, connections, contacts = [], [], []
    started = time.perf_counter()
    try:
        for index in range(count):
            parent, child = context.Pipe()
            process = context.Process(
                target=_child,
                args=(child, index, base_port + index if base_port else 0),
            )
            process.start()
            child.close()
            processes.append(process)
            connections.append(parent)
            contacts.append(_receive(parent))
            if index:
                parent.send(("join", (contacts[0]["host"], contacts[0]["port"])))
                _receive(parent)
        discovered = []
        for connection in connections:
            connection.send(("lookup", None))
            discovered.append(_receive(connection))
        all_ids = {contact["node_id"] for contact in contacts}
        for own, peers in zip(contacts, discovered):
            assert set(peers) == all_ids - {own["node_id"]}, peers
        connections[0].send(("put", {"processes": count}))
        replicas = _receive(connections[0])
        connections[-1].send(("get", None))
        assert _receive(connections[-1]) == {"processes": count}
        report = {
            "status": "PASS",
            "processes": count,
            "ports": [c["port"] for c in contacts],
            "peers_per_node": list(map(len, discovered)),
            "replicas": replicas,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
        print(json.dumps(report, indent=2))
        return report
    finally:
        for connection in connections:
            try:
                connection.send(("stop", None))
            except (BrokenPipeError, OSError):
                pass
        for process in processes:
            process.join(timeout=3)
            if process.is_alive():
                process.terminate()
                process.join(timeout=3)
        for connection in connections:
            connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--base-port", type=int, default=9000)
    args = parser.parse_args()
    audit(args.nodes, args.base_port)
