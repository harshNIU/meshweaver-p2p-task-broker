"""Tests for asynchronous request/response correlation."""

import asyncio
import unittest

from net.correlation import CorrelationManager, RequestTimeoutError


class TestCorrelationManager(unittest.IsolatedAsyncioTestCase):
    async def test_response_matches_request(self):
        manager = CorrelationManager()
        correlation_id, _ = manager.create_request()

        self.assertTrue(manager.deliver_response(correlation_id, {"result": 42}))

        response = await manager.wait_for_response(correlation_id, timeout=1)

        self.assertEqual(response, {"result": 42})
        self.assertEqual(manager.pending_count, 0)

    async def test_unknown_correlation_id_is_rejected(self):
        manager = CorrelationManager()

        with self.assertRaises(KeyError):
            await manager.wait_for_response("unknown-id", timeout=1)

    async def test_timeout_cleans_up_pending_request(self):
        manager = CorrelationManager()
        correlation_id, _ = manager.create_request()

        with self.assertRaises(RequestTimeoutError):
            await manager.wait_for_response(correlation_id, timeout=0.01)

        self.assertEqual(manager.pending_count, 0)
        self.assertFalse(manager.deliver_response(correlation_id, "late"))

    async def test_invalid_timeout_is_rejected(self):
        manager = CorrelationManager()
        correlation_id, _ = manager.create_request()

        with self.assertRaises(ValueError):
            await manager.wait_for_response(correlation_id, timeout=0)

        manager.cancel_request(correlation_id)
        self.assertEqual(manager.pending_count, 0)

    async def test_concurrent_requests_receive_correct_responses(self):
        manager = CorrelationManager()
        first_id, _ = manager.create_request()
        second_id, _ = manager.create_request()

        self.assertNotEqual(first_id, second_id)

        first_waiter = asyncio.create_task(
            manager.wait_for_response(first_id, timeout=1)
        )
        second_waiter = asyncio.create_task(
            manager.wait_for_response(second_id, timeout=1)
        )

        await asyncio.sleep(0)

        manager.deliver_response(second_id, "second")
        manager.deliver_response(first_id, "first")

        first_response, second_response = await asyncio.gather(
            first_waiter,
            second_waiter,
        )

        self.assertEqual(first_response, "first")
        self.assertEqual(second_response, "second")
        self.assertEqual(manager.pending_count, 0)

    async def test_cancel_request_removes_pending_entry(self):
        manager = CorrelationManager()
        correlation_id, _ = manager.create_request()

        self.assertTrue(manager.cancel_request(correlation_id))
        self.assertFalse(manager.cancel_request(correlation_id))
        self.assertEqual(manager.pending_count, 0)

    async def test_duplicate_response_is_not_delivered_twice(self):
        manager = CorrelationManager()
        correlation_id, _ = manager.create_request()

        self.assertTrue(manager.deliver_response(correlation_id, "first"))
        self.assertFalse(manager.deliver_response(correlation_id, "second"))

        response = await manager.wait_for_response(correlation_id, timeout=1)
        self.assertEqual(response, "first")


if __name__ == "__main__":
    unittest.main()
