"""End-to-end tests for seed-based task routing."""

import asyncio
import json
import unittest

from cli.main import select_seed_destination
from execution.builtin_tasks import register_builtin_tasks
from execution.engine import ExecutionEngine
from execution.models import Task
from execution.serializer import serialize_task
from net.bootstrap import SeedAddress, request_peer_list
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.udp_node import start_udp_node
from security.message_signing import sign_message

TEST_SECRET = b"meshweaver-test-secret-32-bytes!!"


class TestSeedRoutingIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_client_discovers_worker_and_executes_task_through_seed(self):
        seed_transport = None
        worker_transport = None
        client_transport = None

        try:
            # Start the seed node.
            seed_transport, _seed_protocol = await start_udp_node(
                node_id="seed-node",
            )
            seed_host, seed_port = seed_transport.get_extra_info("sockname")
            seed = SeedAddress(seed_host, seed_port)

            # Start a worker with the built-in add and multiply tasks.
            engine = ExecutionEngine()
            register_builtin_tasks(engine)

            worker_transport, worker_protocol = await start_udp_node(
                node_id="worker-node",
                execution_engine=engine,
                secret=TEST_SECRET,
            )
            worker_host, worker_port = worker_transport.get_extra_info("sockname")

            # Register/discover through the seed.
            worker_joined = await request_peer_list(
                protocol=worker_protocol,
                node_id="worker-node",
                seed=seed,
                timeout=1.0,
            )
            self.assertTrue(worker_joined, "Worker failed to discover the seed")

            # Start a client and discover peers through the same seed.
            client_transport, client_protocol = await start_udp_node(
                node_id="client-node",
                secret=TEST_SECRET,
            )

            client_joined = await request_peer_list(
                protocol=client_protocol,
                node_id="client-node",
                seed=seed,
                timeout=1.0,
            )
            self.assertTrue(client_joined, "Client failed to discover the seed")

            # The client should choose the worker instead of the seed.
            destination = select_seed_destination(client_protocol, seed)
            self.assertEqual(destination, (worker_host, worker_port))

            # Submit add(2, 3) to the discovered worker.
            task = Task(
                task_name="add",
                args=(2, 3),
                kwargs={},
            )
            task_data = json.loads(serialize_task(task))

            request = Message(
                type=TASK_REQUEST,
                sender_id="client-node",
                seq=1,
                payload={"task": task_data},
            )

            # Authenticate the task request before sending it.
            sign_message(request, TEST_SECRET)

            client_protocol.send_message(request, destination)

            # Wait for the matching task result.
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 2.0
            result = None

            while loop.time() < deadline:
                for message, _address in reversed(client_protocol.received_envelopes):
                    if (
                        message.type == TASK_RESULT
                        and message.seq == 1
                        and isinstance(message.payload, dict)
                        and message.payload.get("task_id") == task.task_id
                    ):
                        result = message.payload
                        break

                if result is not None:
                    break

                await asyncio.sleep(0.01)

            self.assertIsNotNone(
                result,
                "Client did not receive a result from the discovered worker",
            )
            self.assertEqual(result.get("status"), "completed")
            self.assertEqual(result.get("value"), 5)

        finally:
            if client_transport is not None:
                client_transport.close()
            if worker_transport is not None:
                worker_transport.close()
            if seed_transport is not None:
                seed_transport.close()


if __name__ == "__main__":
    unittest.main()
