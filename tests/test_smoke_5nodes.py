"""Day 19 smoke test: five independent workers and twenty tasks."""

import asyncio
import multiprocessing
import time
import unittest

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from gossip.state import NodeInfo
from tasks import TaskClient
from tasks.transport import TaskServer, send_task


def _slow_square(value):
    """Keep tasks active long enough to exercise worker failover."""
    time.sleep(0.5)
    return value * value


def _worker_process(ready):
    """Start one independent TCP task worker."""
    asyncio.run(_serve_worker(ready))


async def _serve_worker(ready):
    engine = ExecutionEngine(max_workers=20)
    engine.register("slow_square", _slow_square)
    server = await TaskServer(engine, workers=20).start()

    try:
        ready.send(server.address)
        await asyncio.Event().wait()
    finally:
        await server.close()
        engine.shutdown()
        ready.close()


class FiveNodeSmokeTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        context = multiprocessing.get_context("spawn")
        self.processes = []
        self.connections = []
        self.peers = []

        try:
            for index in range(5):
                parent, child = context.Pipe(duplex=False)
                process = context.Process(
                    target=_worker_process,
                    args=(child,),
                    name=f"meshweaver-smoke-worker-{index + 1}",
                )
                process.start()
                child.close()

                self.connections.append(parent)
                self.processes.append(process)

                if not parent.poll(15):
                    raise TimeoutError(f"Worker {index + 1} did not start")

                host, port = parent.recv()
                self.peers.append(
                    NodeInfo(
                        node_id=f"smoke-node-{index + 1}",
                        host=host,
                        port=port,
                        cpu=float(index * 10),
                        ram=10.0,
                        last_seen=time.time(),
                    )
                )
        except BaseException:
            self._terminate_workers()
            raise

        self.client = TaskClient(
            self._available_peers,
            send_task,
            timeout=5.0,
        )

    def _available_peers(self):
        """Return only workers whose operating-system processes are alive."""
        now = time.time()
        return [
            NodeInfo(
                node_id=peer.node_id,
                host=peer.host,
                port=peer.port,
                cpu=peer.cpu,
                ram=peer.ram,
                last_seen=now,
                state=peer.state,
            )
            for peer, process in zip(self.peers, self.processes)
            if process.is_alive()
        ]

    def _terminate_workers(self):
        for process in self.processes:
            if process.is_alive():
                process.terminate()

        for process in self.processes:
            process.join(timeout=3)

        for connection in self.connections:
            connection.close()

    async def asyncTearDown(self):
        self._terminate_workers()

    async def test_five_processes_twenty_tasks_survive_two_killed_workers(self):
        tasks = [
            Task("slow_square", args=(value,), max_retries=10) for value in range(20)
        ]

        first_batch = [
            asyncio.create_task(self.client.submit(task)) for task in tasks[:4]
        ]

        # Allow the first batch to begin before killing two workers.
        await asyncio.sleep(0.15)

        for process in self.processes[:2]:
            if process.is_alive():
                process.terminate()
                process.join(timeout=3)

        remaining_batch = [
            asyncio.create_task(self.client.submit(task)) for task in tasks[4:]
        ]

        results = await asyncio.gather(*first_batch, *remaining_batch)

        failures = [
            (result.task_id, result.status, result.error)
            for result in results
            if result.status != TaskStatus.COMPLETED
        ]

        self.assertEqual(len(results), 20)
        self.assertEqual(failures, [])
        self.assertEqual(
            sorted(result.value for result in results),
            [value * value for value in range(20)],
        )


if __name__ == "__main__":
    unittest.main()
