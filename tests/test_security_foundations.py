"""Tests for trusted peers and replay protection."""

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from net.messages import Message
from security.replay import ReplayError, ReplayGuard
from security.signing import public_key_bytes, sign_message
from security.trust import TrustStore, UntrustedPeerError


def make_signed_message(private_key, nonce="nonce-1", timestamp=1000.0):
    message = Message(
        type="TASK_REQUEST",
        sender_id="node-1",
        seq=1,
        payload={"task": {"task_id": "task-1"}, "nonce": nonce},
        ts=timestamp,
    )
    return sign_message(message, private_key)


def test_trusted_peer_signature_verifies():
    private_key = Ed25519PrivateKey.generate()
    store = TrustStore()
    store.add_peer("node-1", private_key.public_key())

    message = make_signed_message(private_key)

    assert store.is_trusted("node-1")
    assert store.verify_message(message)


def test_trusted_peer_accepts_raw_public_key():
    private_key = Ed25519PrivateKey.generate()
    store = TrustStore()
    store.add_peer(
        "node-1",
        public_key_bytes(private_key.public_key()),
    )

    message = make_signed_message(private_key)

    assert store.verify_message(message)


def test_unknown_peer_is_rejected():
    private_key = Ed25519PrivateKey.generate()
    store = TrustStore()
    message = make_signed_message(private_key)

    with pytest.raises(UntrustedPeerError):
        store.verify_message(message)


def test_tampered_message_fails_trust_verification():
    private_key = Ed25519PrivateKey.generate()
    store = TrustStore()
    store.add_peer("node-1", private_key.public_key())

    message = make_signed_message(private_key)
    message.payload["task"]["task_id"] = "tampered"

    assert not store.verify_message(message)


def test_peer_can_be_removed():
    private_key = Ed25519PrivateKey.generate()
    store = TrustStore()
    store.add_peer("node-1", private_key.public_key())

    assert store.remove_peer("node-1")
    assert not store.is_trusted("node-1")
    assert not store.remove_peer("node-1")


def test_replay_guard_accepts_first_message():
    private_key = Ed25519PrivateKey.generate()
    guard = ReplayGuard()

    message = make_signed_message(private_key, timestamp=1000.0)
    guard.check_message(message, now=1000.0)

    assert len(guard) == 1


def test_replay_guard_rejects_reused_nonce():
    private_key = Ed25519PrivateKey.generate()
    guard = ReplayGuard()

    first = make_signed_message(private_key, "same-nonce", 1000.0)
    second = make_signed_message(private_key, "same-nonce", 1000.0)

    guard.check_message(first, now=1000.0)

    with pytest.raises(ReplayError, match="Replay detected"):
        guard.check_message(second, now=1000.0)


def test_replay_guard_rejects_old_timestamp():
    private_key = Ed25519PrivateKey.generate()
    guard = ReplayGuard(max_age_seconds=300)

    message = make_signed_message(private_key, timestamp=1000.0)

    with pytest.raises(ReplayError, match="too old"):
        guard.check_message(message, now=1401.0)


def test_replay_guard_rejects_future_timestamp():
    private_key = Ed25519PrivateKey.generate()
    guard = ReplayGuard(max_future_seconds=30)

    message = make_signed_message(private_key, timestamp=1100.0)

    with pytest.raises(ReplayError, match="future"):
        guard.check_message(message, now=1000.0)


def test_replay_guard_requires_nonce():
    private_key = Ed25519PrivateKey.generate()
    guard = ReplayGuard()

    message = Message(
        type="TASK_REQUEST",
        sender_id="node-1",
        seq=1,
        payload={"task": {"task_id": "task-1"}},
        ts=1000.0,
    )
    sign_message(message, private_key)

    with pytest.raises(ReplayError, match="nonce"):
        guard.check_message(message, now=1000.0)


def test_replay_cache_stays_bounded():
    private_key = Ed25519PrivateKey.generate()
    guard = ReplayGuard(cache_size=2)

    for index in range(3):
        message = make_signed_message(
            private_key,
            nonce=f"nonce-{index}",
            timestamp=1000.0,
        )
        guard.check_message(message, now=1000.0)

    assert len(guard) == 2
