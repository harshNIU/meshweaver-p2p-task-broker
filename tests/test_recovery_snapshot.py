from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from cli.main import _build_recovery_snapshot


@dataclass(frozen=True)
class FakePeer:
    node_id: str
    host: str
    port: int


class FakePeerManager:
    def __init__(self, alive=None, addresses=None):
        self.alive = alive or {}
        self.addresses = addresses or {}

    def is_peer_alive(self, peer_id):
        return self.alive.get(peer_id, False)

    def get_peer_tcp_address(self, peer_id):
        return self.addresses.get(peer_id)


def make_protocol(peer_id="worker-1", address=("127.0.0.1", 9001)):
    peer = FakePeer(node_id=peer_id, host="old-host", port=8000)
    return SimpleNamespace(
        node_registry={peer_id: peer},
        peer_manager=FakePeerManager(
            alive={peer_id: True},
            addresses={peer_id: address},
        ),
    )


def test_recovery_snapshot_uses_live_worker_tcp_endpoint():
    protocol = make_protocol()

    workers = _build_recovery_snapshot(protocol, "local-node")

    assert len(workers) == 1
    assert workers[0].node_id == "worker-1"
    assert workers[0].host == "127.0.0.1"
    assert workers[0].port == 9001


def test_recovery_snapshot_excludes_local_node():
    peer = FakePeer("local-node", "127.0.0.1", 8000)
    protocol = SimpleNamespace(
        node_registry={"local-node": peer},
        peer_manager=FakePeerManager(
            alive={"local-node": True},
            addresses={"local-node": ("127.0.0.1", 9001)},
        ),
    )

    assert _build_recovery_snapshot(protocol, "local-node") == []


def test_recovery_snapshot_excludes_inactive_workers():
    protocol = make_protocol()
    protocol.peer_manager.alive["worker-1"] = False

    assert _build_recovery_snapshot(protocol, "local-node") == []


@pytest.mark.parametrize("host", ["", "   ", None, 123])
def test_recovery_snapshot_rejects_invalid_hostnames(host):
    protocol = make_protocol(address=(host, 9001))

    assert _build_recovery_snapshot(protocol, "local-node") == []


@pytest.mark.parametrize("port", [0, 65536, True, False, "9001", None])
def test_recovery_snapshot_rejects_invalid_ports(port):
    protocol = make_protocol(address=("127.0.0.1", port))

    assert _build_recovery_snapshot(protocol, "local-node") == []


def test_recovery_snapshot_handles_missing_protocol():
    assert _build_recovery_snapshot(None, "local-node") == []


def test_recovery_snapshot_handles_missing_peer_manager():
    protocol = SimpleNamespace(node_registry={})

    assert _build_recovery_snapshot(protocol, "local-node") == []


def test_recovery_snapshot_rejects_malformed_tcp_address():
    protocol = make_protocol(address=("127.0.0.1", 9001))
    protocol.peer_manager.addresses["worker-1"] = ("127.0.0.1",)

    assert _build_recovery_snapshot(protocol, "local-node") == []
