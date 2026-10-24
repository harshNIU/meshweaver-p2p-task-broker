"""Demonstrate lowest-CPU routing and retry after a worker stops."""

import asyncio
import json
import time

from execution.engine import ExecutionEngine
from gossip.state import NodeInfo
from tasks import TaskClient
from tasks.transport import TaskServer, send_task


async def demo():
    engine = ExecutionEngine()
    engine.register("sum_squares", lambda n: sum(i * i for i in range(n)))
    servers = []
    try:
        servers = [await TaskServer(engine).start() for _ in range(3)]
        snapshot = {
            f"worker-{i}": NodeInfo(
                f"worker-{i}",
                *server.address,
                cpu=10 + i * 20,
                ram=20,
                last_seen=time.time(),
            )
            for i, server in enumerate(servers)
        }
        client = TaskClient(lambda: snapshot, send_task)
        first = await client.submit_task("sum_squares", 100)
        await servers[0].close()
        recovered = await client.submit_task("sum_squares", 100)
        assert first.value == recovered.value == 328350
        assert recovered.attempts == 2
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "result": first.value,
                    "first_attempts": first.attempts,
                    "after_worker_stop_attempts": recovered.attempts,
                },
                indent=2,
            )
        )
    finally:
        await asyncio.gather(*(server.close() for server in servers))


if __name__ == "__main__":
    asyncio.run(demo())
