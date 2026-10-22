"""Track peer activity and detect connection timeouts."""

import time


class PeerConnectionManager:
    """Maintain last-seen timestamps for MeshWeaver peers."""

    def __init__(self, timeout_seconds=10.0):
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self.timeout_seconds = timeout_seconds
        self.peers = {}

    def record_activity(self, peer_id, now=None):
        """Record the latest activity received from a peer."""
        if now is None:
            now = time.monotonic()

        self.peers[peer_id] = now

    def get_expired_peers(self, now=None):
        """Return peer IDs that have exceeded the timeout."""
        if now is None:
            now = time.monotonic()

        return [
            peer_id
            for peer_id, last_seen in self.peers.items()
            if now - last_seen >= self.timeout_seconds
        ]

    def remove_expired_peers(self, now=None):
        """Remove expired peers and return their IDs."""
        expired = self.get_expired_peers(now)

        for peer_id in expired:
            del self.peers[peer_id]

        return expired

    def is_peer_alive(self, peer_id, now=None):
        """Check whether a known peer is still within its timeout."""
        if peer_id not in self.peers:
            return False

        if now is None:
            now = time.monotonic()

        return now - self.peers[peer_id] < self.timeout_seconds
