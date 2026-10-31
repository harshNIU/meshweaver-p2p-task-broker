"""Timestamp and nonce-based replay protection for MeshWeaver."""

import time
from collections import OrderedDict

from net.messages import Message


class ReplayError(ValueError):
    """Raised when a message is stale or has already been received."""


class ReplayGuard:
    """Reject stale messages and previously seen sender/nonce pairs."""

    def __init__(
        self,
        *,
        max_age_seconds: float = 300.0,
        max_future_seconds: float = 30.0,
        cache_size: int = 10_000,
    ) -> None:
        if max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive")
        if max_future_seconds < 0:
            raise ValueError("max_future_seconds cannot be negative")
        if cache_size < 1:
            raise ValueError("cache_size must be positive")

        self.max_age_seconds = max_age_seconds
        self.max_future_seconds = max_future_seconds
        self.cache_size = cache_size
        self._seen: OrderedDict[tuple[str, str], None] = OrderedDict()

    def check_message(
        self,
        message: Message,
        *,
        now: float | None = None,
    ) -> None:
        """Validate timestamp and nonce; raise ReplayError on rejection.

        The caller must verify the message signature before calling this
        method. The nonce is expected in message.payload["nonce"].
        """
        current_time = time.time() if now is None else now

        if not isinstance(message.ts, (int, float)):
            raise ReplayError("Message timestamp is invalid")

        age = current_time - message.ts

        if age > self.max_age_seconds:
            raise ReplayError("Message timestamp is too old")

        if age < -self.max_future_seconds:
            raise ReplayError("Message timestamp is too far in the future")

        if not isinstance(message.payload, dict):
            raise ReplayError("Message payload must be an object")

        nonce = message.payload.get("nonce")
        if not isinstance(nonce, str) or not nonce.strip():
            raise ReplayError("Message nonce is missing or invalid")

        key = (message.sender_id, nonce)

        if key in self._seen:
            raise ReplayError("Replay detected: nonce was already used")

        self._seen[key] = None

        while len(self._seen) > self.cache_size:
            self._seen.popitem(last=False)

    def clear(self) -> None:
        """Clear the replay cache."""
        self._seen.clear()

    def __len__(self) -> int:
        """Return the number of nonce entries currently cached."""
        return len(self._seen)
