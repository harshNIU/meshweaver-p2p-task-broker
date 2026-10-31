
"""Integration tests for authenticated TCP task transport."""

import asyncio
import time
import unittest

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from execution.engine import ExecutionEngine
from execution.models import Task
from execution.serializer import task_to_dict
from gossip.state import NodeInfo
from net.messages import TASK_REQUEST, Message
from net.tcp_node import read_message, send_message
from security.replay import ReplayGuard
from security.signing import sign_message
from security.trust import TrustStore
from tasks.transport import TaskServer, send_task


class TestSecureTransport(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client_key = Ed25519PrivateKey.generate()
        self.worker_key = Ed25519PrivateKey.generate()

        self.worker_trust = TrustStore()
        self.worker_trust.add_peer(
            "task-client",
            self.client_key.public_key(),
        )

        self.client_trust = TrustStore()
        self.client_trust.add_peer(
            "task-worker",
            self.worker_key.public_key(),
        )

        self.executions = []

        self.engine = ExecutionEngine()
        self.engine.register(
            "square",
            lambda value: self.executions.append(value) or value * value,
        )

        self.server = await TaskServer(
            self.engine,
            secure=True,
            trust_store=self.worker_trust,
            private_key=self.worker_key,
            replay_guard=ReplayGuard(),
        ).start()

        host, port = self.server.address
        self.peer = NodeInfo(
            "worker",
            host,
            port,
            cpu=10,
            ram=10,
        )

    async def asyncTearDown(self):
        await self.server.close()

    def make_request(
        self,
        *,
        sender_id="task-client",
        private_key=None,
        nonce="test-nonce",
        signed=True,
    ):
        key = private_key or self.client_key

        message = Message(
            TASK_REQUEST,
            sender_id,
            1,
            {
                "task": task_to_dict(Task("square", args=(7,))),
                "nonce": nonce,
            },
            ts=time.time(),
        )

        if signed:
            sign_message(message, key)

        return message

    async def send_raw_request(self, message):
        reader, writer = await asyncio.open_connection(
            *self.server.address
        )

        try:
            await send_message(writer, message)
            return await asyncio.wait_for(
                read_message(reader),
                timeout=2,
            )
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_trusted_client_executes_task_and_verifies_response(self):
        result = await send_task(
            self.peer,
            Task("square", args=(7,)),
            secure=True,
            private_key=self.client_key,
            trust_store=self.client_trust,
        )

        self.assertEqual(result.value, 49)
        self.assertEqual(result.status.value, "completed")
        self.assertEqual(self.executions, [7])

    async def test_unsigned_request_is_rejected(self):
        message = self.make_request(
            nonce="unsigned-nonce",
            signed=False,
        )

        with self.assertRaises(EOFError):
            await self.send_raw_request(message)

        self.assertEqual(self.executions, [])

    async def test_tampered_request_is_rejected(self):
        message = self.make_request(nonce="tampered-nonce")
        message.payload["task"]["args"] = [100]

        with self.assertRaises(EOFError):
            await self.send_raw_request(message)

        self.assertEqual(self.executions, [])

    async def test_untrusted_peer_is_rejected(self):
        stranger_key = Ed25519PrivateKey.generate()

        message = self.make_request(
            sender_id="unknown-peer",
            private_key=stranger_key,
            nonce="unknown-nonce",
        )

        with self.assertRaises(EOFError):
            await self.send_raw_request(message)

        self.assertEqual(self.executions, [])

    async def test_replayed_request_is_rejected(self):
        message = self.make_request(nonce="replay-nonce")

        first_response = await self.send_raw_request(message)

        self.assertEqual(first_response.type, "TASK_RESULT")
        self.assertEqual(self.executions, [7])

        with self.assertRaises(EOFError):
            await self.send_raw_request(message)

        self.assertEqual(self.executions, [7])


if __name__ == "__main__":
    unittest.main()
