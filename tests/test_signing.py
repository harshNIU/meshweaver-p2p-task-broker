"""Tests for Ed25519 message signing and verification."""

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from net.messages import Message
from security.signing import (
    public_key_bytes,
    sign_message,
    verify_signature,
)


def make_message() -> Message:
    return Message(
        type="TASK_ASSIGN",
        sender_id="node-1",
        seq=1,
        payload={"task_id": "task-123", "command": "demo"},
        ts=1000.0,
    )


def test_signed_message_verifies() -> None:
    private_key = Ed25519PrivateKey.generate()
    message = make_message()

    sign_message(message, private_key)

    assert message.sig
    assert verify_signature(message, private_key.public_key())


def test_modified_payload_fails_verification() -> None:
    private_key = Ed25519PrivateKey.generate()
    message = make_message()

    sign_message(message, private_key)
    message.payload["task_id"] = "task-modified"

    assert not verify_signature(message, private_key.public_key())


def test_wrong_public_key_fails_verification() -> None:
    signing_key = Ed25519PrivateKey.generate()
    other_key = Ed25519PrivateKey.generate()
    message = make_message()

    sign_message(message, signing_key)

    assert not verify_signature(message, other_key.public_key())


def test_unsigned_message_fails_verification() -> None:
    message = make_message()
    public_key = Ed25519PrivateKey.generate().public_key()

    assert not verify_signature(message, public_key)


def test_signature_survives_message_encoding_roundtrip() -> None:
    private_key = Ed25519PrivateKey.generate()
    message = make_message()

    sign_message(message, private_key)
    decoded = Message.decode(message.encode())

    assert verify_signature(decoded, private_key.public_key())


def test_public_key_can_be_exported() -> None:
    public_key = Ed25519PrivateKey.generate().public_key()

    exported = public_key_bytes(public_key)

    assert isinstance(exported, bytes)
    assert len(exported) == 32


def test_modified_correlation_id_fails_verification() -> None:
    private_key = Ed25519PrivateKey.generate()
    message = make_message()
    message.correlation_id = "request-original"

    sign_message(message, private_key)
    message.correlation_id = "request-modified"

    assert not verify_signature(message, private_key.public_key())
