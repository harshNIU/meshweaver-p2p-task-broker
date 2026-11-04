"""Tests for UDP peer-expiration recovery integration."""

import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

from net.udp_node import UDPProtocol


class UDPRecoveryIntegrationTests(unittest.IsolatedAsyncioTestCase):

    async def test_expired_peer_schedules_recovery(self):
        coordinator = Mock()
        coordinator.recover_node = AsyncMock(return_value=[])
        protocol = UDPProtocol(node_id="local", recovery_coordinator=coordinator)
        protocol.peer_manager.remove_expired_peers = Mock(return_value=["dead-node"])

        removed = protocol.cleanup_expired_peers()
        await asyncio.gather(*list(protocol._recovery_tasks.values()))

        self.assertEqual(removed, ["dead-node"])
        coordinator.recover_node.assert_awaited_once_with("dead-node")
        self.assertNotIn("dead-node", protocol._recovery_tasks)
        self.assertNotIn("dead-node", protocol._pending_recovery_peers)

    async def test_duplicate_cleanup_does_not_schedule_concurrent_recovery(self):
        coordinator = Mock()
        gate = asyncio.Event()

        async def blocked_recovery(peer_id):
            await gate.wait()
            return []

        coordinator.recover_node = AsyncMock(side_effect=blocked_recovery)
        protocol = UDPProtocol(node_id="local", recovery_coordinator=coordinator)
        protocol.peer_manager.remove_expired_peers = Mock(
            side_effect=[["dead-node"], ["dead-node"]]
        )

        protocol.cleanup_expired_peers()
        first_task = protocol._recovery_tasks["dead-node"]
        protocol.cleanup_expired_peers()

        self.assertIs(protocol._recovery_tasks["dead-node"], first_task)
        await asyncio.sleep(0)
        self.assertEqual(coordinator.recover_node.await_count, 1)

        gate.set()
        await first_task
        coordinator.recover_node.assert_awaited_once_with("dead-node")

    async def test_no_coordinator_preserves_peer_cleanup_behavior(self):
        protocol = UDPProtocol(node_id="local")
        protocol.peer_manager.remove_expired_peers = Mock(return_value=["dead-node"])

        removed = protocol.cleanup_expired_peers()

        self.assertEqual(removed, ["dead-node"])
        self.assertEqual(protocol._recovery_tasks, {})
        self.assertEqual(protocol._pending_recovery_peers, set())

    async def test_invalid_recovery_coordinator_is_rejected(self):
        with self.assertRaises(TypeError):
            UDPProtocol(recovery_coordinator=object())

    async def test_connection_lost_cancels_pending_recovery(self):
        coordinator = Mock()
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def blocked_recovery(peer_id):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        coordinator.recover_node = AsyncMock(side_effect=blocked_recovery)
        protocol = UDPProtocol(node_id="local", recovery_coordinator=coordinator)
        protocol.peer_manager.remove_expired_peers = Mock(return_value=["dead-node"])

        protocol.cleanup_expired_peers()
        await started.wait()
        protocol.connection_lost(None)
        await asyncio.sleep(0)

        self.assertTrue(cancelled.is_set())
        self.assertEqual(protocol._recovery_tasks, {})

    async def test_recovery_exception_cleans_up_and_allows_retry(self):
        coordinator = Mock()
        coordinator.recover_node = AsyncMock(
            side_effect=[RuntimeError("Unexpected recovery failure"), []]
        )
        protocol = UDPProtocol(node_id="local", recovery_coordinator=coordinator)
        protocol.peer_manager.remove_expired_peers = Mock(
            return_value=["dead-node"]
        )

        protocol.cleanup_expired_peers()
        first_task = protocol._recovery_tasks["dead-node"]
        await first_task
        self.assertNotIn("dead-node", protocol._recovery_tasks)

        protocol.cleanup_expired_peers()
        second_task = protocol._recovery_tasks["dead-node"]
        await second_task

        coordinator.recover_node.assert_has_awaits(
            [call("dead-node"), call("dead-node")]
        )
        self.assertEqual(coordinator.recover_node.await_count, 2)
        self.assertNotIn("dead-node", protocol._recovery_tasks)

    async def test_synchronous_cleanup_queues_recovery_for_next_loop_cleanup(self):
        coordinator = Mock()
        coordinator.recover_node = AsyncMock(return_value=[])
        protocol = UDPProtocol(node_id="local", recovery_coordinator=coordinator)
        protocol.peer_manager.remove_expired_peers = Mock(
            side_effect=[["dead-node"], []]
        )

        with patch(
            "net.udp_node.asyncio.get_running_loop",
            side_effect=RuntimeError("no running event loop"),
        ):
            removed = protocol.cleanup_expired_peers()

        self.assertEqual(removed, ["dead-node"])
        self.assertIn("dead-node", protocol._pending_recovery_peers)
        self.assertEqual(protocol._recovery_tasks, {})

        protocol.cleanup_expired_peers()
        recovery_task = protocol._recovery_tasks["dead-node"]
        await recovery_task

        coordinator.recover_node.assert_awaited_once_with("dead-node")
        self.assertNotIn("dead-node", protocol._pending_recovery_peers)
        self.assertNotIn("dead-node", protocol._recovery_tasks)


if __name__ == "__main__":
    unittest.main()
