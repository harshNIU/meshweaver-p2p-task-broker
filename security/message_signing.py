"""HMAC-SHA256 authentication for MeshWeaver network messages."""

import hashlib
import hmac
import json
import os
import time

from net.messages import Message

ENV_SECRET_NAME = "MESHWEAVER_SHARED_SECRET"
MIN_SECRET_BYTES = 32


class MessageAuthenticationError(ValueError):
    """Raised when a network message cannot be authenticated."""


def load_shared_secret():
    """Load the cluster secret from the environment."""
    value = os.environ.get(ENV_SECRET_NAME)

    if not value:
        raise MessageAuthenticationError(
            f"{ENV_SECRET_NAME} is not configured; remote task execution is disabled."
        )

    secret = value.encode("utf-8")

    if len(secret) < MIN_SECRET_BYTES:
        raise MessageAuthenticationError(
            f"{ENV_SECRET_NAME} must contain at least {MIN_SECRET_BYTES} bytes."
        )

    return secret


def _canonical_message(message):
    """Return deterministic bytes for all security-relevant message fields."""
    if not isinstance(message, Message):
        raise TypeError("message must be a Message instance")

    if not message.ts:
        message.ts = time.time()

    data = {
        "type": message.type,
        "sender_id": message.sender_id,
        "seq": message.seq,
        "payload": message.payload,
        "ts": message.ts,
        "correlation_id": message.correlation_id,
    }

    try:
        encoded = json.dumps(
            data,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise MessageAuthenticationError(
            "Message contains data that cannot be authenticated."
        ) from exc

    return encoded.encode("utf-8")


def sign_message(message, secret=None):
    """Sign a message in place and return its hexadecimal HMAC."""
    key = load_shared_secret() if secret is None else secret

    if not isinstance(key, bytes) or len(key) < MIN_SECRET_BYTES:
        raise MessageAuthenticationError(
            f"The shared secret must contain at least {MIN_SECRET_BYTES} bytes."
        )

    signature = hmac.new(
        key,
        _canonical_message(message),
        hashlib.sha256,
    ).hexdigest()

    message.sig = signature
    return signature


def verify_message(message, secret=None):
    """Verify a message signature before its payload is processed."""
    key = load_shared_secret() if secret is None else secret

    if not isinstance(key, bytes) or len(key) < MIN_SECRET_BYTES:
        raise MessageAuthenticationError(
            f"The shared secret must contain at least {MIN_SECRET_BYTES} bytes."
        )

    supplied = getattr(message, "sig", "")

    if not isinstance(supplied, str) or len(supplied) != 64:
        raise MessageAuthenticationError(
            "Message is unsigned or has an invalid signature."
        )

    expected = hmac.new(
        key,
        _canonical_message(message),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(supplied, expected):
        raise MessageAuthenticationError("Message signature verification failed.")

    return True
