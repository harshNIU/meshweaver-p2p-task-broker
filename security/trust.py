
"""Trusted-peer registry for MeshWeaver."""

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PublicKey,
)

from net.messages import Message
from security.signing import verify_signature


class UntrustedPeerError(ValueError):
    """Raised when a message comes from an unknown peer."""


class TrustStore:
    """Store trusted Ed25519 public keys indexed by peer ID."""

    def __init__(self) -> None:
        self._trusted_peers: dict[str, Ed25519PublicKey] = {}

    def add_peer(
        self,
        peer_id: str,
        public_key: Ed25519PublicKey | bytes,
    ) -> None:
        """Register a peer using its public key or raw 32-byte key."""
        if not isinstance(peer_id, str) or not peer_id.strip():
            raise ValueError("peer_id must be a non-empty string")

        if isinstance(public_key, bytes):
            if len(public_key) != 32:
                raise ValueError("Ed25519 public keys must be 32 bytes")
            public_key = Ed25519PublicKey.from_public_bytes(public_key)

        if not isinstance(public_key, Ed25519PublicKey):
            raise TypeError("public_key must be an Ed25519 public key")

        self._trusted_peers[peer_id] = public_key

    def remove_peer(self, peer_id: str) -> bool:
        """Remove a trusted peer. Return False if it was not registered."""
        return self._trusted_peers.pop(peer_id, None) is not None

    def is_trusted(self, peer_id: str) -> bool:
        """Check whether a peer ID is registered."""
        return peer_id in self._trusted_peers

    def get_public_key(self, peer_id: str) -> Ed25519PublicKey:
        """Return a peer's public key or reject an unknown peer."""
        public_key = self._trusted_peers.get(peer_id)
        if public_key is None:
            raise UntrustedPeerError(f"Untrusted peer: {peer_id}")
        return public_key

    def verify_message(self, message: Message) -> bool:
        """Verify a message against the sender's registered public key."""
        public_key = self.get_public_key(message.sender_id)
        return verify_signature(message, public_key)
