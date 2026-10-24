"""Command-line interface for MeshWeaver."""

import asyncio
import json
import math
from uuid import uuid4

import typer
from rich.console import Console

from execution.models import Task
from execution.serializer import TaskSerializationError, serialize_task
from net.bootstrap import SeedAddress, parse_seed_address, request_peer_list
from net.messages import PEER_LIST, TASK_REQUEST, TASK_RESULT, Message
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


def select_seed_destination(protocol, seed: SeedAddress):
    """Select a discovered peer, preferring peers other than the seed."""
    registry = getattr(protocol, "node_registry", {})
    if not isinstance(registry, dict):
        raise TypeError("The node registry must be a dictionary.")

    seed_node_id = None
    client_node_id = getattr(protocol, "node_id", None)

    for message, address in protocol.received_envelopes:
        if (
            message.type == PEER_LIST
            and isinstance(address, tuple)
            and len(address) >= 2
            and address[1] == seed.port
        ):
            seed_node_id = message.sender_id
            break

    candidates = []
    seed_candidate = None

    for node in registry.values():
        node_id = getattr(node, "node_id", None)
        if not node_id or node_id == client_node_id:
            continue

        if getattr(node, "state", "ALIVE") != "ALIVE":
            continue

        candidate = (node.host, node.port)

        if node_id == seed_node_id or (
            node.host == seed.host and node.port == seed.port
        ):
            seed_candidate = candidate
        else:
            candidates.append(candidate)

    if candidates:
        return candidates[0]

    if seed_candidate is not None:
        return seed_candidate

    raise RuntimeError(
        "The seed did not advertise any eligible peers. "
        "Make sure a worker node is running and has registered with the seed."
    )


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
    seed: str | None = typer.Option(
        None,
        "--seed",
        help="Register this node with a seed address, such as 127.0.0.1:8000.",
    ),
):
    """Start a MeshWeaver node, optionally registering with a seed."""
    if not 1 <= port <= 65535:
        console.print("[red]Error: port must be between 1 and 65535.[/red]")
        raise typer.Exit(code=1)

    try:
        seed_address = parse_seed_address(seed) if seed else None
    except (TypeError, ValueError) as exc:
        console.print(f"[red]Invalid seed address: {exc}[/red]")
        raise typer.Exit(code=1) from exc

    async def run_node():
        transport = None

        try:
            from execution.engine import ExecutionEngine

            engine = ExecutionEngine()

            if demo_tasks:
                from execution.builtin_tasks import register_builtin_tasks

                register_builtin_tasks(engine)
                console.print("[green]Registered demo tasks: add, multiply[/green]")

            node_id = f"node-{port}"
            transport, protocol = await start_udp_node(
                host="127.0.0.1",
                port=port,
                node_id=node_id,
                execution_engine=engine,
            )

            if seed_address is not None:
                joined = await request_peer_list(
                    protocol=protocol,
                    node_id=node_id,
                    seed=seed_address,
                    timeout=2.0,
                )
                if not joined:
                    raise RuntimeError(
                        f"Could not discover peers through seed "
                        f"{seed_address.host}:{seed_address.port}."
                    )
                console.print(
                    f"[green]Registered with seed "
                    f"{seed_address.host}:{seed_address.port}[/green]"
                )

            console.print(f"[green]MeshWeaver node running on 127.0.0.1:{port}[/green]")
            console.print("Press Ctrl+C to stop the node.")

            await asyncio.Event().wait()

        finally:
            if transport is not None:
                transport.close()

    try:
        asyncio.run(run_node())
    except KeyboardInterrupt:
        console.print("\n[yellow]MeshWeaver node stopped.[/yellow]")
    except RuntimeError as exc:
        console.print(f"[red]Could not start node: {exc}[/red]")
        raise typer.Exit(code=1) from exc


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
        help="Destination node host for direct submission.",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        help="Destination node UDP port for direct submission.",
    ),
    timeout: float = typer.Option(
        5.0,
        "--timeout",
        help="Seconds to wait for a task result.",
    ),
    seed: str | None = typer.Option(
        None,
        "--seed",
        help="Discover a destination through a seed address, such as 127.0.0.1:8000.",
    ),
):
    """Submit a task directly or through a discovered peer."""
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

        seed_address = parse_seed_address(seed) if seed else None

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
        console.print(f"[red]Invalid task submission: {exc}[/red]")
        raise typer.Exit(code=1) from exc

    async def send_task():
        transport = None

        try:
            sender_id = f"cli-{uuid4()}"
            seq = 1

            transport, protocol = await start_udp_node(
                host="127.0.0.1",
                port=0,
                node_id=sender_id,
            )

            if seed_address is not None:
                joined = await request_peer_list(
                    protocol=protocol,
                    node_id=sender_id,
                    seed=seed_address,
                    timeout=2.0,
                )
                if not joined:
                    raise TimeoutError(
                        f"Could not discover peers through seed "
                        f"{seed_address.host}:{seed_address.port}"
                    )

                destination = select_seed_destination(protocol, seed_address)
            else:
                destination = (host, port)

            request = Message(
                type=TASK_REQUEST,
                sender_id=sender_id,
                seq=seq,
                payload={"task": task_data},
            )
            protocol.send_message(request, destination)

            console.print(
                f"Submitted task '{task.task_name}' "
                f"to {destination[0]}:{destination[1]}"
            )
            console.print(f"Task ID: {task.task_id}")

            return await wait_for_task_result(
                protocol=protocol,
                seq=seq,
                task_id=task.task_id,
                timeout=timeout,
            )

        finally:
            if transport is not None:
                transport.close()

    try:
        result = asyncio.run(send_task())

    except TimeoutError as exc:
        if seed_address is not None:
            target = f"seed {seed_address.host}:{seed_address.port}"
        else:
            target = f"{host}:{port}"

        console.print(
            f"[red]Timed out after {timeout:g} seconds. "
            f"Check that the destination is running and reachable "
            f"through {target}.[/red]"
        )
        raise typer.Exit(code=1) from exc

    except OSError as exc:
        console.print(f"[red]UDP communication failed: {exc}[/red]")
        raise typer.Exit(code=1) from exc

    except RuntimeError as exc:
        console.print(f"[red]Could not submit task: {exc}[/red]")
        raise typer.Exit(code=1) from exc

    status = result.get("status", "unknown")
    console.print(f"Status: {status}")

    if status == "completed":
        console.print(f"Result: {result.get('value')}")
        console.print(f"Duration: {result.get('duration_seconds', 0.0):.6f} seconds")
    else:
        console.print(f"Error: {result.get('error') or 'Unknown execution error'}")

    if status != "completed":
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
