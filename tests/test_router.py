"""Routing policy, actual TCP submission, de-duplication, and bounded failover."""

import asyncio
import time
import unittest
from dataclasses import replace

from execution.engine import ExecutionEngine
from execution.models import Task, TaskResult, TaskStatus
from gossip.state import NodeInfo
from tasks import NoAvailableNodes, TaskClient, select_node
from tasks.transport import TaskServer, send_task


class RouterPolicy(unittest.TestCase):
    def setUp(self):
        self.a = NodeInfo("a", "127.0.0.1", 9000, cpu=10, ram=20, last_seen=100)
        self.b = replace(self.a, node_id="b")

    def test_cpu_ram_and_id_order(self):
        self.assertEqual(select_node([self.b, self.a], now=100).node_id, "a")
        self.assertEqual(
            select_node([replace(self.a, cpu=20), self.b], now=100).node_id, "b"
        )
        self.assertEqual(
            select_node([replace(self.a, ram=30), self.b], now=100).node_id, "b"
        )

    def test_filters(self):
        invalid = [
            replace(self.a, state="DEAD"),
            replace(self.a, state="SUSPECT"),
            replace(self.a, cpu=95),
            replace(self.a, ram=95),
            replace(self.a, last_seen=0),
            replace(self.a, last_seen=80),
            replace(self.a, last_seen=101),
            replace(self.a, cpu=float("nan")),
            replace(self.a, ram=-1),
        ]
        for peer in invalid:
            with self.subTest(peer=peer), self.assertRaises(NoAvailableNodes):
                select_node([peer], now=100)
        with self.assertRaises(NoAvailableNodes):
            select_node([self.a], exclude={"a"}, now=100)
        with self.assertRaises(NoAvailableNodes):
            select_node([], now=100)


class Submission(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.servers = []
        self.engine = ExecutionEngine()
        self.engine.register("square", lambda x: x * x)

    async def asyncTearDown(self):
        await asyncio.gather(*(server.close() for server in self.servers))

    async def worker(self, name, cpu):
        server = await TaskServer(self.engine).start()
        self.servers.append(server)
        return NodeInfo(name, *server.address, cpu=cpu, ram=10, last_seen=time.time())

    async def test_tcp_result_and_failover(self):
        peers = [await self.worker("a", 10), await self.worker("b", 20)]
        client = TaskClient(lambda: peers, send_task)
        result = await client.submit_task("square", 7)
        self.assertEqual((result.value, result.attempts), (49, 1))
        await self.servers[0].close()
        result = await client.submit_task("square", 8)
        self.assertEqual((result.value, result.attempts), (64, 2))

    async def test_concurrent_duplicate_executes_once(self):
        calls = []

        def slow():
            calls.append(1)
            time.sleep(0.02)
            return 42

        self.engine.register("slow", slow)
        peer = await self.worker("a", 10)
        task = Task("slow")
        results = await asyncio.gather(*(send_task(peer, task) for _ in range(3)))
        self.assertEqual([r.value for r in results], [42] * 3)
        self.assertEqual(calls, [1])
        self.assertEqual((await send_task(peer, task)).value, 42)
        self.assertEqual(calls, [1])

    async def test_application_failure_not_retried(self):
        peer = await self.worker("a", 10)
        result = await TaskClient(lambda: [peer], send_task).submit_task("missing")
        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertEqual(result.attempts, 1)

    async def test_timeout_retries_next_worker_with_same_id(self):
        peers = [await self.worker("a", 10), await self.worker("b", 20)]
        seen = []

        async def transport(peer, task):
            seen.append((peer.node_id, task.task_id))
            if peer.node_id == "a":
                await asyncio.sleep(1)
            return TaskResult(task.task_id, TaskStatus.COMPLETED, value=10)

        client = TaskClient(lambda: peers, transport, timeout=0.01)
        result = await client.submit_task("square", 2)
        self.assertEqual(result.value, 10)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(seen[0][1], seen[1][1])
        self.assertEqual([s[0] for s in seen], ["a", "b"])

    async def test_retry_limit_and_no_workers(self):
        peers = [await self.worker("a", 10), await self.worker("b", 20)]

        async def failed(peer, task):
            raise ConnectionError("offline")

        result = await TaskClient(lambda: peers, failed).submit_task(
            "square", max_retries=0
        )
        self.assertEqual((result.status, result.attempts), (TaskStatus.FAILED, 1))
        result = await TaskClient(list, failed).submit_task("square")
        self.assertEqual(result.attempts, 0)

    async def test_mismatched_result_rejected(self):
        peer = await self.worker("a", 10)

        async def wrong(peer, task):
            return TaskResult("wrong", TaskStatus.COMPLETED)

        result = await TaskClient(lambda: [peer], wrong).submit_task("square")
        self.assertEqual(result.status, TaskStatus.FAILED)
