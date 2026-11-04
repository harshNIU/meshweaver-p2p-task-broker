import asyncio
import unittest
from dataclasses import replace

from execution.engine import ExecutionEngine
from execution.models import TaskStatus
from net.bootstrap import SeedAddress, request_peer_list
from net.udp_node import start_udp_node
from tasks.submit import TaskClient
from tasks.transport import TaskServer, send_task


class SubmitEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def test_three_node_submit_and_result(self):
        engine = ExecutionEngine(max_workers=2)
        engine.register("add", lambda left, right: left + right)

        task_server = TaskServer(engine)

        seed_transport = None
        worker_transport = None
        client_transport = None

        async def wait_until(predicate, timeout=3.0):
            deadline = asyncio.get_running_loop().time() + timeout

            while asyncio.get_running_loop().time() < deadline:
                if predicate():
                    return True
                await asyncio.sleep(0.01)

            return predicate()

        try:
            # Start the worker's TCP task server.
            await task_server.start(host="127.0.0.1", port=0)
            tcp_host, tcp_port = task_server.address

            # Start the seed node.
            seed_transport, seed_protocol = await start_udp_node(node_id="seed")
            seed_host, seed_port = seed_transport.get_extra_info("sockname")[:2]
            seed_address = SeedAddress(seed_host, seed_port)

            # Start the worker's UDP node and advertise its TCP endpoint.
            worker_transport, worker_protocol = await start_udp_node(node_id="worker")
            worker_protocol.set_tcp_endpoint(tcp_host, tcp_port)

            # Worker joins the network through the seed.
            joined = await request_peer_list(
                worker_protocol,
                "worker",
                seed_address,
            )
            self.assertTrue(joined, "Worker failed to discover the seed")

            # Advertise the worker's TCP endpoint to the seed.
            worker_protocol.send_peer_list((seed_host, seed_port))

            self.assertTrue(
                await wait_until(
                    lambda: (
                        "worker" in seed_protocol.node_registry
                        and seed_protocol.node_registry["worker"].tcp_port == tcp_port
                    )
                ),
                "Seed did not learn the worker's TCP endpoint",
            )

            # Start the client and let it discover the worker through the seed.
            client_transport, client_protocol = await start_udp_node(node_id="client")

            joined = await request_peer_list(
                client_protocol,
                "client",
                seed_address,
            )
            self.assertTrue(joined, "Client failed to discover the seed")

            self.assertTrue(
                await wait_until(
                    lambda: (
                        "worker" in client_protocol.node_registry
                        and client_protocol.node_registry["worker"].tcp_port == tcp_port
                    )
                ),
                "Client did not discover the worker's TCP endpoint",
            )

            # Use the discovered worker snapshot for task routing.
            def snapshot():
                worker = client_protocol.node_registry.get("worker")
                if worker is None or worker.tcp_port is None:
                    return []
                return [worker]

            # Adapt the discovered UDP NodeInfo to the TCP endpoint.
            async def send_to_worker(peer, task):
                tcp_peer = replace(peer, port=peer.tcp_port)
                return await send_task(tcp_peer, task)

            client = TaskClient(
                snapshot,
                send_to_worker,
                timeout=3.0,
            )

            # Submit a task through the client and receive the worker's result.
            result = await client.submit_task(
                "add",
                2,
                3,
                max_retries=0,
            )

            self.assertEqual(result.status, TaskStatus.COMPLETED)
            self.assertEqual(result.value, 5)
            self.assertEqual(result.attempts, 1)
            self.assertIsNotNone(engine.get_task_result(result.task_id))

        finally:
            if client_transport is not None:
                client_transport.close()

            if worker_transport is not None:
                worker_transport.close()

            if seed_transport is not None:
                seed_transport.close()

            await task_server.close()
            engine.shutdown()


if __name__ == "__main__":
    unittest.main()
