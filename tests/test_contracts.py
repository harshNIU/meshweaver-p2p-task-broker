
import json
from dataclasses import fields

import pytest

from gossip.state import NodeInfo
from net.messages import Message


def test_message_contract_fields():
    """Verify the standard message envelope fields."""
    field_names = {field.name for field in fields(Message)}

    assert field_names == {
        "type",
        "sender_id",
        "seq",
        "payload",
        "ts",
        "sig",
    }


def test_message_default_values():
    """Verify timestamp and signature defaults."""
    message = Message(
        type="PING",
        sender_id="node-1",
        seq=1,
        payload={},
    )

    assert message.ts == 0.0
    assert message.sig == ""


def test_message_encode_decode_contract():
    """Verify that encoding and decoding preserve message data."""
    original = Message(
        type="PING",
        sender_id="node-1",
        seq=1,
        payload={"target": "node-2"},
        ts=123.45,
        sig="",
    )

    encoded = original.encode()
    decoded = Message.decode(encoded)

    assert isinstance(encoded, bytes)
    assert decoded == original


def test_encoded_message_is_valid_json():
    """Verify that the encoded envelope contains all contract fields."""
    message = Message(
        type="PING",
        sender_id="node-1",
        seq=1,
        payload={},
    )

    data = json.loads(message.encode().decode("utf-8"))

    assert set(data) == {
        "type",
        "sender_id",
        "seq",
        "payload",
        "ts",
        "sig",
    }


@pytest.mark.parametrize("state", ["ALIVE", "SUSPECT", "DEAD"])
def test_node_info_supported_states(state):
    """Verify the supported node states."""
    node = NodeInfo(
        node_id="node-1",
        host="127.0.0.1",
        port=8000,
        state=state,
    )

    assert node.state == state


def test_node_info_contract_fields():
    """Verify required and optional NodeInfo fields."""
    field_names = {field.name for field in fields(NodeInfo)}

    assert field_names == {
        "node_id",
        "host",
        "port",
        "cpu",
        "ram",
        "last_seen",
        "state",
        "tcp_port",
    }


def test_node_info_tcp_port_defaults_to_none():
    """Verify TCP port is optional for backward compatibility."""
    node = NodeInfo(
        node_id="node-1",
        host="127.0.0.1",
        port=8000,
    )

    assert node.tcp_port is None
