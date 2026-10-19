from gossip.engine import GossipEngine
from gossip.state import NodeInfo


def test_engine_initializes_with_self_node():
    node = NodeInfo(
        node_id="node-1",
        host="127.0.0.1",
        port=8000,
    )

    engine = GossipEngine(node)

    assert engine.self_node.node_id == "node-1"
    assert "node-1" in engine.get_nodes()


def test_refresh_self_metrics(monkeypatch):
    node = NodeInfo(
        node_id="node-1",
        host="127.0.0.1",
        port=8000,
    )

    engine = GossipEngine(node)

    monkeypatch.setattr(
        "gossip.engine.sample_load",
        lambda: {"cpu": 25.0, "ram": 60.0},
    )

    updated_node = engine.refresh_self_metrics()

    assert updated_node.cpu == 25.0
    assert updated_node.ram == 60.0

def test_merge_nodes_adds_new_node():
    local_node = NodeInfo(
        node_id="node-1",
        host="127.0.0.1",
        port=8000,
    )

    remote_node = NodeInfo(
        node_id="node-2",
        host="127.0.0.2",
        port=8001,
        last_seen=10.0,
    )

    engine = GossipEngine(local_node)
    engine.merge_nodes([remote_node])

    assert "node-2" in engine.get_nodes()
    assert engine.get_nodes()["node-2"].host == "127.0.0.2"