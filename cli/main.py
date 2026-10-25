"""MeshWeaver CLI for starting nodes and submitting tasks."""

import asyncio
import json
import math
import time
import uuid

import typer

from execution.builtin_tasks import register_builtin_tasks
from execution.engine import ExecutionEngine
from execution.models import Task
from execution.task_registry import TaskRegistry
from net.bootstrap import parse_seed_address, request_peer_list
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.udp_node import start_udp_node

app = typer.Typer(help="MeshWeaver P2P Task Broker")

WEB_ALLOWED_TASKS = frozenset({"add", "multiply"})


def _create_engine():
    """Create an execution engine with built-in tasks and lifecycle tracking."""
    task_registry = TaskRegistry()
    engine = ExecutionEngine(task_registry=task_registry)
    register_builtin_tasks(engine)
    return engine


def _validate_port(port, option):
    """Validate a port and print a useful error."""
    if not 1 <= port <= 65535:
        typer.echo(f"{option} must be between 1 and 65535")
        raise typer.Exit(code=1)


def _peer_address(peer, key=None):
    """Extract a peer's network address from a NodeInfo-like object."""
    address = getattr(peer, "address", None)
    if address is None:
        address = getattr(peer, "addr", None)

    if address is None:
        host = getattr(peer, "host", None)
        peer_port = getattr(peer, "port", None)
        if host is not None and peer_port is not None:
            address = (host, peer_port)

    if address is None and isinstance(key, tuple) and len(key) == 2:
        address = key

    if address is None:
        return None

    try:
        return tuple(address)
    except TypeError:
        return None


def select_seed_destination(protocol, seed):
    """Choose a live worker discovered through the seed."""
    seed_address = (seed.host, seed.port)
    registry = getattr(protocol, "node_registry", {})
    current_node_id = getattr(protocol, "node_id", None)

    peers = registry.items() if isinstance(registry, dict) else []
    seed_is_known = False

    for key, peer in peers:
        address = _peer_address(peer, key)
        if address is None:
            continue

        peer_id = getattr(peer, "node_id", None)
        if peer_id is None:
            peer_id = getattr(peer, "id", None)

        if address == seed_address:
            seed_is_known = True
            continue

        if current_node_id is not None and peer_id == current_node_id:
            continue

        status = getattr(peer, "status", None)
        status = getattr(status, "name", status)

        if status is not None and not str(status).upper().endswith("ALIVE"):
            continue

        return address

    if seed_is_known:
        return seed_address

    raise RuntimeError("No available worker or seed peer was discovered.")


def _wait_for_task_result(protocol, seq, task_id, timeout):
    """Wait for the result matching the request sequence and task ID."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        envelopes = getattr(protocol, "received_envelopes", [])

        for envelope in list(envelopes):
            if isinstance(envelope, tuple):
                message = envelope[0]
            else:
                message = getattr(envelope, "message", envelope)

            if getattr(message, "type", None) != TASK_RESULT:
                continue

            if getattr(message, "seq", None) != seq:
                continue

            payload = getattr(message, "payload", {})
            if payload.get("task_id") != task_id:
                continue

            try:
                envelopes.remove(envelope)
            except (AttributeError, ValueError):
                pass

            return payload

        time.sleep(0.005)

    raise TimeoutError(f"Timed out waiting for task result ({timeout:g}s).")


@app.command()
def start(
    host: str = typer.Option("127.0.0.1", help="Host address to bind."),
    port: int = typer.Option(8000, help="UDP port."),
    node_id: str = typer.Option(None, help="Optional node identifier."),
    seed: str = typer.Option(None, help="Seed address in host:port format."),
    demo_tasks: bool = typer.Option(
        False, "--demo-tasks", help="Register built-in demo tasks."
    ),
    web: bool = typer.Option(False, "--web", help="Start the web dashboard."),
    web_port: int = typer.Option(8765, help="Dashboard HTTP port."),
):
    """Start a UDP node and optionally its dashboard."""
    _validate_port(port, "port")
    _validate_port(web_port, "--web-port")

    engine = _create_engine()

    async def run_node():
        transport, protocol = await start_udp_node(
            host=host,
            port=port,
            node_id=node_id or f"node-{port}",
            execution_engine=engine,
        )

        dashboard = None

        try:
            if seed:
                seed_address = parse_seed_address(seed)
                await request_peer_list(
                    protocol=protocol,
                    node_id=node_id or f"node-{port}",
                    seed=seed_address,
                )

            typer.echo(f"MeshWeaver node running at {host}:{port}")

            if web:
                from web.server import create_dashboard_server
                from web.snapshot import build_snapshot

                def submit_demo(payload):
                    """Validate and queue an allowlisted dashboard demo task."""
                    task_name = payload["name"]
                    args = payload.get("args", [])
                    kwargs = payload.get("kwargs", {})

                    if (
                        not isinstance(task_name, str)
                        or task_name not in WEB_ALLOWED_TASKS
                    ):
                        raise ValueError("Only add and multiply are allowed.")

                    if not isinstance(args, list):
                        raise TypeError("'args' must be a JSON array.")

                    if not isinstance(kwargs, dict):
                        raise TypeError("'kwargs' must be a JSON object.")

                    if not all(isinstance(key, str) for key in kwargs):
                        raise TypeError("'kwargs' keys must be strings.")

                    task = Task(
                        task_name,
                        args=tuple(args),
                        kwargs=kwargs,
                    )

                    engine.submit_task(task)

                    return {
                        "task_id": task.task_id,
                        "state": "PENDING",
                    }

                dashboard = create_dashboard_server(
                    host=host,
                    port=web_port,
                    snapshot_provider=lambda: build_snapshot(
                        protocol=protocol,
                        transport=transport,
                        node_id=node_id or f"node-{port}",
                        host=host,
                        port=port,
                        task_registry=engine.task_registry,
                    ),
                    task_submitter=submit_demo,
                )

                typer.echo(f"Dashboard: http://{host}:{web_port}/")
                typer.echo(f"Submission token: {dashboard.submit_token}")
                typer.echo("Use the token in the X-Token header for POST /api/submit.")

            await asyncio.Event().wait()

        finally:
            if dashboard is not None:
                if hasattr(dashboard, "shutdown"):
                    dashboard.shutdown()
                elif hasattr(dashboard, "close"):
                    dashboard.close()

            transport.close()
            engine.shutdown(wait=False, cancel_futures=True)

    try:
        asyncio.run(run_node())
    except KeyboardInterrupt:
        typer.echo("Node stopped.")
    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(f"Error starting node: {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def submit(
    task_name: str = typer.Argument(..., help="Task name, such as add or multiply."),
    args: str = typer.Option(
        "[]", "--args", help="JSON array of positional arguments."
    ),
    kwargs: str = typer.Option(
        "{}", "--kwargs", help="JSON object of keyword arguments."
    ),
    host: str = typer.Option("127.0.0.1", help="Target node host."),
    port: int = typer.Option(8000, help="Target node UDP port."),
    timeout: float = typer.Option(5.0, help="Result timeout in seconds."),
    seed: str = typer.Option(None, help="Optional seed address in host:port format."),
):
    """Submit a task to a remote MeshWeaver node."""
    _validate_port(port, "--port")

    if not math.isfinite(timeout) or timeout <= 0:
        typer.echo("--timeout must be a finite positive number")
        raise typer.Exit(code=1)

    try:
        task_args = json.loads(args)
    except json.JSONDecodeError as exc:
        typer.echo(f"Invalid --args JSON: {exc}")
        raise typer.Exit(code=1) from exc

    if not isinstance(task_args, list):
        typer.echo("--args must be a JSON array")
        raise typer.Exit(code=1)

    try:
        task_kwargs = json.loads(kwargs)
    except json.JSONDecodeError as exc:
        typer.echo(f"Invalid --kwargs JSON: {exc}")
        raise typer.Exit(code=1) from exc

    if not isinstance(task_kwargs, dict):
        typer.echo("--kwargs must be a JSON object")
        raise typer.Exit(code=1)

    task_id = str(uuid.uuid4())

    try:
        Task(
            task_name,
            args=tuple(task_args),
            kwargs=task_kwargs,
            task_id=task_id,
        )
    except (TypeError, ValueError) as exc:
        typer.echo(f"Invalid task: {exc}")
        raise typer.Exit(code=1) from exc

    task_payload = {
        "task_id": task_id,
        "task_name": task_name,
        "args": task_args,
        "kwargs": task_kwargs,
    }

    async def send_task():
        transport, protocol = await start_udp_node(
            host="127.0.0.1",
            port=0,
            node_id=f"client-{uuid.uuid4().hex[:8]}",
        )

        try:
            destination = (host, port)

            if seed:
                seed_address = parse_seed_address(seed)
                joined = await request_peer_list(
                    protocol=protocol,
                    node_id=getattr(protocol, "node_id", "meshweaver-client"),
                    seed=seed_address,
                    timeout=timeout,
                )

                if not joined:
                    raise RuntimeError("Could not discover peers through the seed.")

                destination = select_seed_destination(protocol, seed_address)

            request = Message(
                type=TASK_REQUEST,
                sender_id=getattr(protocol, "node_id", "meshweaver-client"),
                seq=1,
                payload={"task": task_payload},
            )

            protocol.send_message(request, destination)

            return await asyncio.to_thread(
                _wait_for_task_result,
                protocol,
                request.seq,
                task_id,
                timeout,
            )

        finally:
            transport.close()

    typer.echo(f"Submitted task '{task_name}'")

    try:
        result = asyncio.run(send_task())
    except TimeoutError as exc:
        typer.echo(f"Timed out: {exc}")
        raise typer.Exit(code=1) from exc
    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(f"Task submission failed: {exc}")
        raise typer.Exit(code=1) from exc

    status = str(result.get("status", "failed")).lower()
    typer.echo(f"Status: {status}")

    if status == "completed":
        typer.echo(f"Result: {result.get('value')}")
        return

    if result.get("error"):
        typer.echo(str(result["error"]))

    raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
