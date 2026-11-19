"""Member 4 demonstration: authenticated remote mathematical task execution."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from execution.engine import ExecutionEngine
from execution.models import Task
from net.tcp_node import send_callable_task, start_tcp_server

DEMO_SECRET = b"meshweaver-member4-demo-secret-2026"


def calculate_expression(a: int, b: int, c: int) -> int:
    return (a * b) + (c ** 2)


async def main() -> None:
    engine = ExecutionEngine(max_workers=2)
    server = None

    try:
        server, _ = await start_tcp_server(
            host="127.0.0.1",
            port=0,
            execution_engine=engine,
            secret=DEMO_SECRET,
        )

        port = server.sockets[0].getsockname()[1]

        task = Task(
            task_name="mathematical_calculation",
            args=(12, 8, 5),
        )

        print("MeshWeaver Member 4 - Remote Execution Demo")
        print("-" * 48)
        print(f"Worker started on 127.0.0.1:{port}")

        result = await send_callable_task(
            "127.0.0.1",
            port,
            task,
            calculate_expression,
            secret=DEMO_SECRET,
        )

        print("\nRemote Execution Result")
        print("-" * 48)
        print(f"Task ID : {result.task_id}")
        print(f"Status  : {result.status.value}")
        print(f"Attempts: {result.attempts}")
        print(f"Result  : {result.value}")
        print(f"Duration: {result.duration_seconds:.6f} seconds")

        if result.value != 121:
            raise RuntimeError("Unexpected mathematical result.")

        print("\nDEMO PASSED")

    finally:
        if server is not None:
            server.close()
            await server.wait_closed()

        engine.shutdown(wait=True)
        print("Worker and execution engine shut down.")


if __name__ == "__main__":
    asyncio.run(main())
