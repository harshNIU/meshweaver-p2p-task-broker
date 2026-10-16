
import unittest

from net.peer_manager import PeerConnectionManager


class TestPeerConnectionManager(unittest.TestCase):

    def test_peer_is_alive_before_timeout(self):
        manager = PeerConnectionManager(timeout_seconds=10)
        manager.record_activity("node-b", now=100)

        self.assertTrue(manager.is_peer_alive("node-b", now=105))

    def test_peer_expires_after_timeout(self):
        manager = PeerConnectionManager(timeout_seconds=10)
        manager.record_activity("node-b", now=100)

        expired = manager.get_expired_peers(now=110)

        self.assertEqual(expired, ["node-b"])

    def test_expired_peer_can_be_removed(self):
        manager = PeerConnectionManager(timeout_seconds=10)
        manager.record_activity("node-b", now=100)

        removed = manager.remove_expired_peers(now=110)

        self.assertEqual(removed, ["node-b"])
        self.assertFalse(manager.is_peer_alive("node-b", now=110))

    def test_unknown_peer_is_not_alive(self):
        manager = PeerConnectionManager(timeout_seconds=10)

        self.assertFalse(manager.is_peer_alive("unknown-node", now=100))

    def test_activity_refreshes_timeout(self):
        manager = PeerConnectionManager(timeout_seconds=10)
        manager.record_activity("node-b", now=100)
        manager.record_activity("node-b", now=108)

        self.assertTrue(manager.is_peer_alive("node-b", now=115))


if __name__ == "__main__":
    unittest.main()
