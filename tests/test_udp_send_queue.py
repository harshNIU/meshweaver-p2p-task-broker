
import asyncio
import unittest

from net.udp_node import start_udp_node


class TestUDPSendQueue(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.transport, self.protocol = await start_udp_node(
            node_id="queue-test-node"
        )

    async def asyncTearDown(self):
        self.transport.close()
        await asyncio.sleep(0)

    async def test_send_message_is_accepted_without_blocking(self):
        address = ("127.0.0.1", 9999)
        result = self.protocol.send_message("Hello MeshWeaver", address)
        self.assertIsNone(result)

    async def test_send_queue_is_created(self):
        self.assertTrue(hasattr(self.protocol, "send_queue"))

    async def test_send_queue_is_bounded(self):
        self.assertTrue(hasattr(self.protocol, "send_queue"))
        self.assertGreater(self.protocol.send_queue.maxsize, 0)

    async def test_send_queue_worker_is_running(self):
        self.assertTrue(hasattr(self.protocol, "_send_queue_task"))
        self.assertIsNotNone(self.protocol._send_queue_task)
        self.assertFalse(self.protocol._send_queue_task.done())

    async def test_send_queue_overflow_raises_buffer_error(self):
        address = ("127.0.0.1", 9999)

        self.protocol._send_queue_task.cancel()
        await asyncio.gather(
            self.protocol._send_queue_task,
            return_exceptions=True,
        )
        self.protocol._send_queue_task = None

        for index in range(self.protocol.send_queue.maxsize):
            self.protocol.send_message(f"Message {index}", address)

        with self.assertRaises(BufferError):
            self.protocol.send_message("Overflow message", address)

    async def test_send_rate_limit_controls_send_interval(self):
        class FakeTransport:
            def __init__(self):
                self.send_times = []

            def is_closing(self):
                return False

            def sendto(self, data, address):
                self.send_times.append(
                    asyncio.get_running_loop().time()
                )

        # Stop the original worker.
        self.protocol._send_queue_task.cancel()
        await asyncio.gather(
            self.protocol._send_queue_task,
            return_exceptions=True,
        )
        self.protocol._send_queue_task = None

        # Configure a rate of 10 messages per second.
        self.protocol.send_rate_limit = 10.0

        fake_transport = FakeTransport()
        self.protocol.transport = fake_transport

        self.protocol._start_send_queue_worker()

        address = ("127.0.0.1", 9999)

        for index in range(3):
            self.protocol.send_message(f"Rate test {index}", address)

        await asyncio.wait_for(self.protocol.send_queue.join(), timeout=2)

        self.assertEqual(len(fake_transport.send_times), 3)

        first_interval = (
            fake_transport.send_times[1] - fake_transport.send_times[0]
        )
        second_interval = (
            fake_transport.send_times[2] - fake_transport.send_times[1]
        )

        self.assertGreaterEqual(first_interval, 0.08)
        self.assertGreaterEqual(second_interval, 0.08)

        # Stop the test worker.
        self.protocol._send_queue_task.cancel()
        await asyncio.gather(
            self.protocol._send_queue_task,
            return_exceptions=True,
        )
        self.protocol._send_queue_task = None
