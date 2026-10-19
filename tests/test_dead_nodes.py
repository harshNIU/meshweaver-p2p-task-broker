import time

from gossip.engine import GossipEngine
from gossip.state import NodeInfo


def test_remove_dead_nodes_keeps_alive_nodes():
    local = NodeInfo(
        node_id="node-1",
        host="127.0.0.1",
        port=8000,
        last_seen=time.time(),
    )

    remote = NodeInfo(
        node_id="node-2",
        host="127.0.0.2",
        port=8001,
        last_seen=time.time(),
        state="DEAD",
    )

    engine = GossipEngine(local)
    engine.merge_nodes([remote])

    removed = engine.remove_dead_nodes()

    assert removed == ["node-2"]
    assert "node-1" in engine.get_nodes()
    assert "node-2" not in engine.get_nodes()