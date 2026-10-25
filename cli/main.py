"""Command-line interface for the MeshWeaver peer-to-peer task broker."""
import asyncio
import json
import math
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import typer
from rich.console import Console

from execution.engine import ExecutionEngine
from execution.models import Task
from execution.task_registry import TaskRegistry
from net.udp_node import start_udp_node

app = typer.Typer(help="MeshWeaver peer-to-peer task broker CLI")
console = Console()


def _add_numbers(*args: Any) -> int | float:
    """Add numeric arguments."""
    if not args:
        raise ValueError("add requires at least one numeric argument.")
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float))
        for value in args
    ):
        raise ValueError("add accepts only numeric arguments.")

    result = sum(args)
    if not math.isfinite(result):
        raise ValueError("Result must be finite.")
    return result


def _multiply_numbers(*args: Any) -> int | float:
    """Multiply numeric arguments."""
    if not args:
        raise ValueError("multiply requires at least one numeric argument.")
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float))
        for value in args
    ):
        raise ValueError("multiply accepts only numeric arguments.")

    result: int | float = 1
    for value in args:
        result *= value

    if not math.isfinite(result):
        raise ValueError("Result must be finite.")
    return result


def _create_engine(registry: TaskRegistry) -> ExecutionEngine:
    """Create an engine with explicitly registered built-in functions."""
    engine = ExecutionEngine(task_registry=registry)
    engine.register("add", _add_numbers)
    engine.register("multiply", _multiply_numbers)
    return engine


def _submit_payload(
    payload: dict[str, Any],
    engine: ExecutionEngine,
) -> dict[str, Any]:
    """Validate a request and execute an explicitly registered task."""
    name = payload.get("task")
    args = payload.get("args", [])
    kwargs = payload.get("kwargs", {})

    if not isinstance(name, str) or not name.strip():
        raise ValueError("'task' must be a non-empty string.")

    if not isinstance(args, list):
        raise ValueError("'args' must be a JSON array.")

    if not isinstance(kwargs, dict) or not all(
        isinstance(key, str) for key in kwargs
    ):
        raise ValueError("'kwargs' must be a JSON object with string keys.")

    if name not in engine.registered_tasks():
        raise ValueError(
            f"Unknown task '{name}'. Available tasks: "
            f"{', '.join(engine.registered_tasks())}"
        )

    task = Task(task_name=name, args=tuple(args), kwargs=kwargs)
    result = engine.execute(task)

    response: dict[str, Any] = {
        "task_id": result.task_id,
        "task": name,
        "status": result.status.value.upper(),
        "attempts": result.attempts,
        "duration_seconds": result.duration_seconds,
    }

    if result.error is not None:
        response["error"] = result.error
    else:
        response["result"] = result.value

    return response


@app.command()
def start(
    port: int = typer.Option(
        8000, "--port", help="UDP port for this node."
    ),
    web: bool = typer.Option(
        False,
        "--web",
        help="Start the live local web dashboard alongside this node.",
    ),
    web_port: int = typer.Option(
        8765,
        "--web-port",
        min=1,
        max=65535,
        help="Local HTTP port for the dashboard (used with --web).",
    ),
):
    """Start a MeshWeaver node, optionally with the live dashboard."""
    if not 1 <= port <= 65535:
        console.print("[red]Error: UDP port must be between 1 and 65535.[/red]")
        raise typer.Exit(code=1)

    registry = TaskRegistry()
    engine = _create_engine(registry)

    async def run_node():
        transport = None
        dashboard_server = None

        try:
            transport, protocol = await start_udp_node(
                host="127.0.0.1",
                port=port,
                node_id=f"node-{port}",
            )

            console.print(
                f"[green]MeshWeaver node running on 127.0.0.1:{port}[/green]"
            )

            if web:
                from web.server import HOST as WEB_HOST
                from web.server import create_dashboard_server
                from web.snapshot import build_snapshot

                dashboard_server = create_dashboard_server(
                    lambda: build_snapshot(
                        protocol=protocol,
                        transport=transport,
                        node_id=f"node-{port}",
                        host="127.0.0.1",
                        port=port,
                        task_registry=registry,
                    ),
                    host=WEB_HOST,
                    port=web_port,
                    task_submitter=lambda payload: _submit_payload(
                        payload, engine
                    ),
                )

                console.print(
                    f"[green]Live dashboard: "
                    f"http://{WEB_HOST}:{web_port}/[/green]"
                )
                console.print(
                    "[dim]Tasks execute locally in this process. "
                    "UDP task distribution is not implemented yet.[/dim]"
                )

            console.print("Registered tasks: add, multiply")
            console.print("Press Ctrl+C to stop the node.")
            await asyncio.Event().wait()

        finally:
            if dashboard_server is not None:
                dashboard_server.shutdown()
                dashboard_server.server_close()

            if transport is not None:
                transport.close()

    try:
        asyncio.run(run_node())
    except KeyboardInterrupt:
        console.print("\n[yellow]MeshWeaver node stopped.[/yellow]")


@app.command()
def submit(
    task: str | None = typer.Option(
        None,
        "--task",
        help="Registered task name.",
    ),
    args_json: str = typer.Option(
        "[]",
        "--args-json",
        help='JSON array of positional arguments, e.g. "[2,3]".',
    ),
    kwargs_json: str = typer.Option(
        "{}",
        "--kwargs-json",
        help='JSON object of keyword arguments, e.g. "{}".',
    ),
    url: str = typer.Option(
        "http://127.0.0.1:8765",
        "--url",
        help="Base URL of the running dashboard runtime.",
    ),
):
    """Submit a task to a running local MeshWeaver node."""
    # Preserve the behavior expected by the existing CLI test.
    if task is None:
        console.print(
            "[yellow]Submit command is not implemented yet.[/yellow]"
        )
        return

    try:
        args = json.loads(args_json)
        kwargs = json.loads(kwargs_json)
    except json.JSONDecodeError as exc:
        console.print(f"[red]Invalid JSON: {exc.msg}[/red]")
        raise typer.Exit(code=2) from exc

    if not isinstance(args, list):
        console.print("[red]--args-json must be a JSON array.[/red]")
        raise typer.Exit(code=2)

    if not isinstance(kwargs, dict) or not all(
        isinstance(key, str) for key in kwargs
    ):
        console.print(
            "[red]--kwargs-json must be a JSON object with string keys.[/red]"
        )
        raise typer.Exit(code=2)

    payload = json.dumps(
        {"task": task, "args": args, "kwargs": kwargs}
    ).encode("utf-8")

    request = Request(
        url.rstrip("/") + "/api/tasks",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))

    except HTTPError as exc:
        try:
            message = json.loads(exc.read().decode("utf-8")).get(
                "error", str(exc)
            )
        except (json.JSONDecodeError, UnicodeDecodeError):
            message = str(exc)

        console.print(f"[red]Task submission rejected: {message}[/red]")
        raise typer.Exit(code=1) from exc

    except (URLError, TimeoutError, OSError) as exc:
        console.print(
            "[red]Could not reach the running node. Start it with "
            "'python -m cli.main start --web' first.[/red]"
        )
        console.print(f"[dim]{exc}[/dim]")
        raise typer.Exit(code=1) from exc

    console.print_json(json.dumps(result))


if __name__ == "__main__":
    app()
