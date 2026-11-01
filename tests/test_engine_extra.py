import asyncio
import pytest

from gossip.engine import GossipEngine
from gossip.state import NodeInfo


def make_node(node_id, last_seen=100.0, state="ALIVE"):
    return NodeInfo(
        node_id=node_id,
        host="127.0.0.1",
        port=8000,
        cpu=10.0,
        ram=20.0,
        last_seen=last_seen,
        state=state,
    )


def test_get_nodes_returns_copy():
    engine = GossipEngine(make_node("self"))
    nodes = engine.get_nodes()

    nodes["fake"] = make_node("fake")

    assert "fake" not in engine.get_nodes()


def test_merge_nodes_adds_new_peer():
    engine = GossipEngine(make_node("self"))
    peer = make_node("peer")

    engine.merge_nodes([peer])

    assert "peer" in engine.get_nodes()


def test_merge_nodes_preserves_newer_information():
    engine = GossipEngine(make_node("self"))
    engine.merge_nodes([make_node("peer", last_seen=100.0)])

    engine.merge_nodes([make_node("peer", last_seen=200.0)])

    assert engine.get_nodes()["peer"].last_seen == 200.0


def test_merge_nodes_does_not_replace_local_node():
    engine = GossipEngine(make_node("self", last_seen=100.0))

    engine.merge_nodes([make_node("self", last_seen=500.0)])

    assert engine.get_nodes()["self"].last_seen == 100.0


def test_remove_dead_nodes_keeps_alive_nodes():
    engine = GossipEngine(make_node("self"))
    engine.merge_nodes(
        [
            make_node("alive", state="ALIVE"),
            make_node("dead", state="DEAD"),
        ]
    )

    removed = engine.remove_dead_nodes()

    assert "dead" in removed
    assert "dead" not in engine.get_nodes()
    assert "alive" in engine.get_nodes()


def test_remove_dead_nodes_preserves_local_node():
    engine = GossipEngine(make_node("self", state="DEAD"))

    engine.remove_dead_nodes()

    assert "self" in engine.get_nodes()


def test_refresh_self_metrics_updates_node(monkeypatch):
    engine = GossipEngine(make_node("self"))

    monkeypatch.setattr(
        "gossip.engine.sample_load",
        lambda: {"cpu": 45.0, "ram": 60.0},
    )

    node = engine.refresh_self_metrics()

    assert node.cpu == 45.0
    assert node.ram == 60.0
    assert engine.get_nodes()["self"].cpu == 45.0
