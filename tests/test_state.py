from gossip.state import NodeInfo


def test_node_info_defaults():
    node = NodeInfo(
        node_id="node-1",
        host="127.0.0.1",
        port=8000,
    )

    assert node.node_id == "node-1"
    assert node.host == "127.0.0.1"
    assert node.port == 8000
    assert node.cpu == 0.0
    assert node.ram == 0.0
    assert node.state == "ALIVE"