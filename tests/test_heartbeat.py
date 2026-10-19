import time

from gossip.heartbeat import HeartbeatMonitor
from gossip.state import NodeInfo


def make_node(node_id: str, last_seen: float) -> NodeInfo:
    return NodeInfo(
        node_id=node_id,
        host="127.0.0.1",
        port=8000,
        last_seen=last_seen,
    )


def test_heartbeat_marks_node_alive():
    node = make_node("node-1", time.time())
    monitor = HeartbeatMonitor({"node-1": node})

    assert monitor.check_nodes()["node-1"] == "ALIVE"


def test_heartbeat_marks_node_suspect():
    node = make_node("node-1", time.time() - 7)
    monitor = HeartbeatMonitor({"node-1": node})

    assert monitor.check_nodes()["node-1"] == "SUSPECT"


def test_heartbeat_marks_node_dead():
    node = make_node("node-1", time.time() - 11)
    monitor = HeartbeatMonitor({"node-1": node})

    assert monitor.check_nodes()["node-1"] == "DEAD"


def test_record_heartbeat_restores_alive_state():
    node = make_node("node-1", time.time() - 11)
    monitor = HeartbeatMonitor({"node-1": node})

    monitor.record_heartbeat("node-1")

    assert node.state == "ALIVE"
    assert monitor.check_nodes()["node-1"] == "ALIVE"