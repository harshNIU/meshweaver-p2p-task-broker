
"""Command-line interface for MeshWeaver."""

import asyncio
import json
import math
from uuid import uuid4

import typer
from rich.console import Console

from execution.models import Task
from execution.serializer import TaskSerializationError, serialize_task
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.udp_node import start_udp_node

app = typer.Typer(help="MeshWeaver peer-to-peer task broker CLI")
console = Console()


async def wait_for_task_result(protocol, seq, task_id, timeout):
    """Wait for the result matching a submitted task."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        for message, _address in reversed(protocol.received_envelopes):
            if (
                message.type == TASK_RESULT
                and message.seq == seq
                and isinstance(message.payload, dict)
                and message.payload.get("task_id") == task_id
            ):
                return message.payload

        await asyncio.sleep(0.01)

    raise TimeoutError


@app.command()
def start(
    port: int = typer.Option(
        8000,
        "--port",
        help="UDP port for this node.",
    ),
    demo_tasks: bool = typer.Option(
        False,
        "--demo-tasks",
        help="Register the built-in add and multiply demonstration tasks.",
    ),
):
    """Start a MeshWeaver node."""
    if not 1 <= port <= 65535:
        console.print(
            "[red]Error: port must be between 1 and 65535.[/red]"
        )
        raise typer.Exit(code=1)

    async def run_node():
        transport = None

        try:
            from execution.engine import ExecutionEngine

            engine = ExecutionEngine()

            if demo_tasks:
                from execution.builtin_tasks import register_builtin_tasks

                register_builtin_tasks(engine)
                console.print(
                    "[green]Registered demo tasks: add, multiply[/green]"
                )

            transport, _protocol = await start_udp_node(
                host="127.0.0.1",
                port=port,
                node_id=f"node-{port}",
                execution_engine=engine,
            )

            console.print(
                f"[green]MeshWeaver node running on "
                f"127.0.0.1:{port}[/green]"
            )
            console.print("Press Ctrl+C to stop the node.")

            await asyncio.Event().wait()

        finally:
            if transport is not None:
                transport.close()

    try:
        asyncio.run(run_node())
    except KeyboardInterrupt:
        console.print("\n[yellow]MeshWeaver node stopped.[/yellow]")


@app.command()
def submit(
    task_name: str = typer.Argument(
        ...,
        help="Name of the registered task.",
    ),
    args_json: str = typer.Option(
        "[]",
        "--args",
        help="Task positional arguments as a JSON array.",
    ),
    kwargs_json: str = typer.Option(
        "{}",
        "--kwargs",
        help="Task keyword arguments as a JSON object.",
    ),
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Destination node host.",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        help="Destination node UDP port.",
    ),
    timeout: float = typer.Option(
        5.0,
        "--timeout",
        help="Seconds to wait for a task result.",
    ),
):
    """Submit a task to a remote MeshWeaver UDP node."""
    try:
        args = json.loads(args_json)
        kwargs = json.loads(kwargs_json)

        if not isinstance(args, list):
            raise TypeError("--args must be a JSON array.")

        if not isinstance(kwargs, dict):
            raise TypeError("--kwargs must be a JSON object.")

        if not all(isinstance(key, str) for key in kwargs):
            raise TypeError("--kwargs keys must be strings.")

        if not 1 <= port <= 65535:
            raise ValueError("--port must be between 1 and 65535.")

        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("--timeout must be a finite positive number.")

        task = Task(
            task_name=task_name,
            args=tuple(args),
            kwargs=kwargs,
        )

        task_data = json.loads(serialize_task(task))

    except (
        json.JSONDecodeError,
        TypeError,
        ValueError,
        TaskSerializationError,
    ) as exc:
        console.print(
            f"[red]Invalid task submission: {exc}[/red]"
        )
        raise typer.Exit(code=1) from exc

    async def send_task():
        transport = None

        try:
            # Use a unique sender ID for each CLI invocation.
            sender_id = f"cli-{uuid4()}"
            seq = 1

            transport, protocol = await start_udp_node(
                host="127.0.0.1",
                port=0,
                node_id=sender_id,
            )

            request = Message(
                type=TASK_REQUEST,
                sender_id=sender_id,
                seq=seq,
                payload={"task": task_data},
            )

            protocol.send_message(request, (host, port))

            console.print(
                f"Submitted task '{task.task_name}' "
                f"to {host}:{port}"
            )
            console.print(f"Task ID: {task.task_id}")

            result = await wait_for_task_result(
                protocol=protocol,
                seq=seq,
                task_id=task.task_id,
                timeout=timeout,
            )

            return result

        finally:
            if transport is not None:
                transport.close()

    try:
        result = asyncio.run(send_task())

    except TimeoutError as exc:
        console.print(
            f"[red]Timed out after {timeout:g} seconds waiting for "
            f"task result. Check that the destination node is running "
            f"and reachable at {host}:{port}.[/red]"
        )
        raise typer.Exit(code=1) from exc

    except OSError as exc:
        console.print(
            f"[red]UDP communication failed: {exc}[/red]"
        )
        raise typer.Exit(code=1) from exc

    except RuntimeError as exc:
        console.print(
            f"[red]Could not submit task: {exc}[/red]"
        )
        raise typer.Exit(code=1) from exc

    status = result.get("status", "unknown")
    console.print(f"Status: {status}")

    if status == "completed":
        console.print(f"Result: {result.get('value')}")
        console.print(
            f"Duration: "
            f"{result.get('duration_seconds', 0.0):.6f} seconds"
        )
    else:
        console.print(
            f"Error: "
            f"{result.get('error') or 'Unknown execution error'}"
        )

    if status != "completed":
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
