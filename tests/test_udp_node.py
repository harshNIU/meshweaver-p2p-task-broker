import asyncio
import json
import unittest

from execution.engine import ExecutionEngine
from execution.models import Task
from execution.serializer import serialize_task
from net.messages import PING, PONG, TASK_REQUEST, TASK_RESULT, Message
from net.udp_node import start_udp_node


class TestUDPNode(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.transport_a, self.protocol_a = await start_udp_node(node_id="node-a")
        self.transport_b, self.protocol_b = await start_udp_node(node_id="node-b")

    async def asyncTearDown(self):
        self.transport_a.close()
        self.transport_b.close()
        await asyncio.sleep(0)

    async def wait_for_condition(self, condition, timeout=2.0):
        """Wait until a condition becomes true or the timeout expires."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while not condition():
            if loop.time() >= deadline:
                self.fail("Timed out waiting for UDP communication")

            await asyncio.sleep(0.02)

    async def test_two_nodes_exchange_plain_text_messages(self):
        address_b = self.transport_b.get_extra_info("sockname")
        self.protocol_a.send_message("Hello MeshWeaver", address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_messages) == 1
        )

        message, sender = self.protocol_b.received_messages[0]
        self.assertEqual(message, "Hello MeshWeaver")
        self.assertEqual(sender[0], "127.0.0.1")

    async def test_structured_message_is_received(self):
        address_b = self.transport_b.get_extra_info("sockname")
        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=1,
            payload={"text": "Hello MeshWeaver"},
        )

        self.protocol_a.send_message(message, address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_envelopes) == 1
        )

        received, sender = self.protocol_b.received_envelopes[0]
        self.assertEqual(received.type, "TEST")
        self.assertEqual(received.sender_id, "node-a")
        self.assertEqual(received.seq, 1)
        self.assertEqual(received.payload, {"text": "Hello MeshWeaver"})
        self.assertEqual(sender[0], "127.0.0.1")

    async def test_ping_receives_pong_response(self):
        address_b = self.transport_b.get_extra_info("sockname")
        ping = Message(
            type=PING,
            sender_id="node-a",
            seq=42,
            payload={},
        )

        self.protocol_a.send_message(ping, address_b)

        await self.wait_for_condition(
            lambda: any(
                received.type == PONG
                for received, _address in self.protocol_a.received_envelopes
            )
        )

        pong, sender = next(
            (received, sender)
            for received, sender in self.protocol_a.received_envelopes
            if received.type == PONG
        )

        self.assertEqual(pong.sender_id, "node-b")
        self.assertEqual(pong.seq, 42)
        self.assertEqual(pong.payload, {"reply_to": "node-a"})
        self.assertEqual(sender[0], "127.0.0.1")

    async def test_unknown_structured_message_does_not_reply(self):
        address_b = self.transport_b.get_extra_info("sockname")
        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=2,
            payload={},
        )

        self.protocol_a.send_message(message, address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_envelopes) == 1
        )
        await asyncio.sleep(0.1)

        self.assertEqual(len(self.protocol_a.received_envelopes), 0)

    async def test_structured_message_records_peer_activity(self):
        address_b = self.transport_b.get_extra_info("sockname")
        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=10,
            payload={},
        )

        self.protocol_a.send_message(message, address_b)

        await self.wait_for_condition(
            lambda: "node-a" in self.protocol_b.peer_manager.peers
        )

        self.assertTrue(self.protocol_b.peer_manager.is_peer_alive("node-a"))

    async def test_cleanup_expired_peers(self):
        """Remove expired peers while preserving active peers."""
        manager = self.protocol_b.peer_manager
        manager.timeout_seconds = 10

        manager.record_activity("node-old", now=100)
        manager.record_activity("node-active", now=105)

        removed = self.protocol_b.cleanup_expired_peers(now=110)

        self.assertEqual(removed, ["node-old"])
        self.assertNotIn("node-old", manager.peers)
        self.assertIn("node-active", manager.peers)

    async def test_plain_text_does_not_register_structured_peer(self):
        address_b = self.transport_b.get_extra_info("sockname")
        self.protocol_a.send_message("Hello MeshWeaver", address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_messages) == 1
        )

        self.assertEqual(self.protocol_b.peer_manager.peers, {})

    async def test_duplicate_structured_message_is_ignored(self):
        address_b = self.transport_b.get_extra_info("sockname")
        message = Message(
            type="TEST",
            sender_id="node-a",
            seq=100,
            payload={"text": "Hello"},
        )

        self.protocol_a.send_message(message, address_b)
        self.protocol_a.send_message(message, address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_envelopes) == 1
        )
        await asyncio.sleep(0.1)

        self.assertEqual(len(self.protocol_b.received_envelopes), 1)

    async def test_same_sequence_from_different_senders_is_accepted(self):
        address_b = self.transport_b.get_extra_info("sockname")
        message_a = Message(
            type="TEST",
            sender_id="node-a",
            seq=5,
            payload={},
        )
        message_c = Message(
            type="TEST",
            sender_id="node-c",
            seq=5,
            payload={},
        )

        self.protocol_a.send_message(message_a, address_b)
        self.protocol_a.send_message(message_c, address_b)

        await self.wait_for_condition(
            lambda: len(self.protocol_b.received_envelopes) == 2
        )

        self.assertEqual(len(self.protocol_b.received_envelopes), 2)

    async def test_task_request_executes_registered_task(self):
        engine = ExecutionEngine()
        engine.register("add", lambda a, b: a + b)

        self.transport_b.close()
        await asyncio.sleep(0)

        self.transport_b, self.protocol_b = await start_udp_node(
            node_id="node-b",
            execution_engine=engine,
        )
        address_b = self.transport_b.get_extra_info("sockname")

        task = Task(task_name="add", args=(2, 3))
        request = Message(
            type=TASK_REQUEST,
            sender_id="node-a",
            seq=200,
            payload={"task": json.loads(serialize_task(task))},
        )

        self.protocol_a.send_message(request, address_b)

        await self.wait_for_condition(
            lambda: any(
                message.type == TASK_RESULT
                for message, _address in self.protocol_a.received_envelopes
            )
        )

        result = next(
            message
            for message, _address in self.protocol_a.received_envelopes
            if message.type == TASK_RESULT
        )

        self.assertEqual(result.payload["task_id"], task.task_id)
        self.assertEqual(result.payload["status"], "completed")
        self.assertEqual(result.payload["value"], 5)

    async def test_task_request_unknown_task_returns_failure(self):
        address_b = self.transport_b.get_extra_info("sockname")
        task = Task(task_name="unknown_task")

        request = Message(
            type=TASK_REQUEST,
            sender_id="node-a",
            seq=201,
            payload={"task": json.loads(serialize_task(task))},
        )

        self.protocol_a.send_message(request, address_b)

        await self.wait_for_condition(
            lambda: any(
                message.type == TASK_RESULT
                for message, _address in self.protocol_a.received_envelopes
            )
        )

        result = next(
            message
            for message, _address in self.protocol_a.received_envelopes
            if message.type == TASK_RESULT
        )

        self.assertEqual(result.payload["status"], "failed")
        self.assertIn("Unknown task", result.payload["error"])

    async def test_malformed_task_payload_returns_failure(self):
        address_b = self.transport_b.get_extra_info("sockname")
        request = Message(
            type=TASK_REQUEST,
            sender_id="node-a",
            seq=300,
            payload={"task": "not-a-task-object"},
        )

        self.protocol_a.send_message(request, address_b)

        await self.wait_for_condition(
            lambda: any(
                message.type == TASK_RESULT
                for message, _address in self.protocol_a.received_envelopes
            )
        )

        result = next(
            message
            for message, _address in self.protocol_a.received_envelopes
            if message.type == TASK_RESULT
        )

        self.assertEqual(result.payload["status"], "failed")
        self.assertIn("task object", result.payload["error"])

    async def test_duplicate_task_request_reuses_cached_result(self):
        engine = ExecutionEngine()
        calls = []

        def count_call():
            calls.append(1)
            return len(calls)

        engine.register("count_call", count_call)

        self.transport_b.close()
        await asyncio.sleep(0)
        self.transport_b, self.protocol_b = await start_udp_node(
            node_id="node-b",
            execution_engine=engine,
        )
        address_b = self.transport_b.get_extra_info("sockname")

        task = Task(task_name="count_call")
        request = Message(
            type=TASK_REQUEST,
            sender_id="node-a",
            seq=301,
            payload={"task": json.loads(serialize_task(task))},
        )

        self.protocol_a.send_message(request, address_b)

        await self.wait_for_condition(
            lambda: any(
                message.type == TASK_RESULT and message.seq == 301
                for message, _address in self.protocol_a.received_envelopes
            )
        )

        first_result = next(
            message
            for message, _address in self.protocol_a.received_envelopes
            if message.type == TASK_RESULT and message.seq == 301
        )

        self.protocol_a.send_message(request, address_b)

        await self.wait_for_condition(
            lambda: len(
                [
                    message
                    for message, _address in self.protocol_a.received_envelopes
                    if message.type == TASK_RESULT and message.seq == 301
                ]
            )
            >= 2
        )

        results = [
            message
            for message, _address in self.protocol_a.received_envelopes
            if message.type == TASK_RESULT and message.seq == 301
        ]

        self.assertEqual(len(calls), 1)
        self.assertEqual(results[0].payload["value"], 1)
        self.assertEqual(results[1].payload, first_result.payload)

    async def test_same_task_id_with_new_sequence_reuses_cached_result(self):
        engine = ExecutionEngine()
        calls = []

        def count_call():
            calls.append(1)
            return len(calls)

        engine.register("count_call", count_call)

        self.transport_b.close()
        await asyncio.sleep(0)
        self.transport_b, self.protocol_b = await start_udp_node(
            node_id="node-b",
            execution_engine=engine,
        )
        address_b = self.transport_b.get_extra_info("sockname")

        task = Task(task_name="count_call")
        task_payload = json.loads(serialize_task(task))

        first_request = Message(
            type=TASK_REQUEST,
            sender_id="node-a",
            seq=302,
            payload={"task": task_payload},
        )
        second_request = Message(
            type=TASK_REQUEST,
            sender_id="node-a",
            seq=303,
            payload={"task": task_payload},
        )

        self.protocol_a.send_message(first_request, address_b)

        await self.wait_for_condition(
            lambda: any(
                message.type == TASK_RESULT and message.seq == 302
                for message, _address in self.protocol_a.received_envelopes
            )
        )

        self.protocol_a.send_message(second_request, address_b)

        await self.wait_for_condition(
            lambda: any(
                message.type == TASK_RESULT and message.seq == 303
                for message, _address in self.protocol_a.received_envelopes
            )
        )

        second_result = next(
            message
            for message, _address in self.protocol_a.received_envelopes
            if message.type == TASK_RESULT and message.seq == 303
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(second_result.payload["value"], 1)


if __name__ == "__main__":
    unittest.main()
