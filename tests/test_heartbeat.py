import time

from gossip.heartbeat import HeartbeatMonitor
from gossip.state import NodeInfo


def make_node(node_id: str, last_seen: float) -> NodeInfo:
    """Create a node for heartbeat testing."""
    return NodeInfo(
        node_id=node_id,
        host="127.0.0.1",
        port=8000,
        last_seen=last_seen,
    )


def test_heartbeat_marks_node_alive():
    """A recently responding node should remain alive."""
    node = make_node("node-1", time.time())
    monitor = HeartbeatMonitor({"node-1": node})

    assert monitor.check_nodes()["node-1"] == "ALIVE"


def test_heartbeat_marks_node_suspect():
    """A node missing recent heartbeats should become suspect."""
    node = make_node("node-1", time.time() - 7)
    monitor = HeartbeatMonitor(
        {"node-1": node},
        suspect_after=6.0,
        dead_after=10.0,
    )

    assert monitor.check_nodes()["node-1"] == "SUSPECT"


def test_heartbeat_marks_node_dead():
    """A node missing heartbeats beyond the threshold is dead."""
    node = make_node("node-1", time.time() - 11)
    monitor = HeartbeatMonitor(
        {"node-1": node},
        suspect_after=6.0,
        dead_after=10.0,
    )

    assert monitor.check_nodes()["node-1"] == "DEAD"


def test_record_heartbeat_restores_alive_state():
    """Receiving a heartbeat restores the node to alive."""
    node = make_node("node-1", time.time() - 20)
    node.state = "DEAD"

    monitor = HeartbeatMonitor({"node-1": node})
    monitor.record_heartbeat("node-1")

    assert node.state == "ALIVE"
    assert monitor.check_nodes()["node-1"] == "ALIVE"


def test_unknown_node_heartbeat_is_ignored():
    """An unknown node must not be added automatically."""
    monitor = HeartbeatMonitor({})

    monitor.record_heartbeat("unknown-node")

    assert "unknown-node" not in monitor.nodes


def test_dead_node_stays_dead_without_heartbeat():
    """A node remains dead while no new heartbeat arrives."""
    node = make_node("node-1", time.time() - 20)
    monitor = HeartbeatMonitor(
        {"node-1": node},
        suspect_after=6.0,
        dead_after=10.0,
    )

    assert monitor.check_nodes()["node-1"] == "DEAD"
    assert monitor.check_nodes()["node-1"] == "DEAD"


def test_heartbeat_updates_last_seen():
    """A heartbeat updates the last-seen timestamp."""
    node = make_node("node-1", 1.0)
    monitor = HeartbeatMonitor({"node-1": node})

    monitor.record_heartbeat("node-1")

    assert node.last_seen > 1.0
    assert node.state == "ALIVE"