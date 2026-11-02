"""Ed25519 signing and verification for MeshWeaver message envelopes."""

import base64
import json

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
)

from net.messages import Message


def _canonical_message(message: Message) -> bytes:
    """Create deterministic bytes from all message metadata being signed."""
    envelope = {
        "type": message.type,
        "sender_id": message.sender_id,
        "seq": message.seq,
        "payload": message.payload,
        "ts": message.ts,
        "correlation_id": message.correlation_id,
    }

    return json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sign_message(
    message: Message,
    private_key: Ed25519PrivateKey,
) -> Message:
    """Sign a message and store its Base64 signature in message.sig."""
    signature = private_key.sign(_canonical_message(message))
    message.sig = base64.b64encode(signature).decode("ascii")
    return message


def verify_signature(
    message: Message,
    public_key: Ed25519PublicKey,
) -> bool:
    """Verify a message signature, returning False if it is invalid."""
    if not message.sig:
        return False

    try:
        signature = base64.b64decode(message.sig, validate=True)
        public_key.verify(signature, _canonical_message(message))
    except (ValueError, TypeError, InvalidSignature):
        return False

    return True


def public_key_bytes(public_key: Ed25519PublicKey) -> bytes:
    """Export an Ed25519 public key in raw 32-byte format."""
    return public_key.public_bytes(
        encoding=Encoding.Raw,
        format=PublicFormat.Raw,
    )
