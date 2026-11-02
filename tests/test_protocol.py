from gossip.protocol import merge_node_info
from gossip.state import NodeInfo


def test_merge_adds_new_node():
    current = {}
    incoming = [
        NodeInfo(
            node_id="node-2",
            host="127.0.0.2",
            port=8001,
            last_seen=10.0,
        )
    ]

    result = merge_node_info(current, incoming)

    assert "node-2" in result
    assert result["node-2"].host == "127.0.0.2"


def test_merge_keeps_newer_information():
    old_node = NodeInfo(
        node_id="node-2",
        host="127.0.0.2",
        port=8001,
        last_seen=10.0,
    )

    new_node = NodeInfo(
        node_id="node-2",
        host="127.0.0.2",
        port=8001,
        last_seen=20.0,
    )

    current = {"node-2": old_node}

    result = merge_node_info(current, [new_node])

    assert result["node-2"].last_seen == 20.0


def test_merge_preserves_tcp_port_from_newer_record_without_tcp_port():
    known_node = NodeInfo(
        node_id="node-2",
        host="127.0.0.2",
        port=8001,
        last_seen=10.0,
        tcp_port=9001,
    )

    older_record = NodeInfo(
        node_id="node-2",
        host="127.0.0.2",
        port=8001,
        last_seen=20.0,
        tcp_port=None,
    )

    current = {"node-2": known_node}

    result = merge_node_info(current, [older_record])

    assert result["node-2"].last_seen == 20.0
    assert result["node-2"].tcp_port == 9001


def test_merge_updates_tcp_port_from_newer_peer_record():
    old_node = NodeInfo(
        node_id="node-2",
        host="127.0.0.2",
        port=8001,
        last_seen=10.0,
        tcp_port=9001,
    )

    new_node = NodeInfo(
        node_id="node-2",
        host="127.0.0.2",
        port=8001,
        last_seen=20.0,
        tcp_port=9002,
    )

    current = {"node-2": old_node}

    result = merge_node_info(current, [new_node])

    assert result["node-2"].tcp_port == 9002
