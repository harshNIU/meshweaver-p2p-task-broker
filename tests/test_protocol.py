from gossip.protocol import merge_node_info
from gossip.state import NodeInfo


def make_node(node_id, last_seen=100.0, cpu=10.0, ram=20.0):
    return NodeInfo(
        node_id=node_id,
        host="127.0.0.1",
        port=8000,
        cpu=cpu,
        ram=ram,
        last_seen=last_seen,
        state="ALIVE",
    )


def test_merge_adds_new_node():
    current = {"node-1": make_node("node-1")}
    incoming = [make_node("node-2")]

    result = merge_node_info(current, incoming)

    assert "node-2" in result
    assert "node-1" in result


def test_merge_keeps_newer_information():
    current = {"node-1": make_node("node-1", last_seen=100.0)}
    incoming = [make_node("node-1", last_seen=200.0)]

    result = merge_node_info(current, incoming)

    assert result["node-1"].last_seen == 200.0


def test_merge_preserves_newer_existing_information():
    current = {"node-1": make_node("node-1", last_seen=200.0)}
    incoming = [make_node("node-1", last_seen=100.0)]

    result = merge_node_info(current, incoming)

    assert result["node-1"].last_seen == 200.0


def test_merge_updates_cpu_and_ram_with_newer_data():
    current = {"node-1": make_node("node-1", last_seen=100.0)}
    incoming = [
        make_node("node-1", last_seen=200.0, cpu=50.0, ram=60.0)
    ]

    result = merge_node_info(current, incoming)

    assert result["node-1"].cpu == 50.0
    assert result["node-1"].ram == 60.0


def test_merge_empty_incoming_list():
    current = {"node-1": make_node("node-1")}

    result = merge_node_info(current, [])

    assert result == current


def test_merge_does_not_add_older_information():
    current = {"node-1": make_node("node-1", last_seen=200.0)}
    incoming = [make_node("node-1", last_seen=100.0)]

    result = merge_node_info(current, incoming)

    assert result["node-1"].last_seen == 200.0
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
