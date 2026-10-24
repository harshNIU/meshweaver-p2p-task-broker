import asyncio
import json

import typer
from rich.console import Console

from execution.models import Task
from execution.serializer import TaskSerializationError, serialize_task
from net.udp_node import start_udp_node

app = typer.Typer(help="MeshWeaver peer-to-peer task broker CLI")
console = Console()


@app.command()
def start(
    port: int = typer.Option(8000, "--port", help="UDP port for this node."),
):
    """Start a MeshWeaver node."""

    if not 1 <= port <= 65535:
        console.print("[red]Error: port must be between 1 and 65535.[/red]")
        raise typer.Exit(code=1)

    async def run_node():
        transport = None
        try:
            transport, _ = await start_udp_node(
                host="127.0.0.1",
                port=port,
                node_id=f"node-{port}",
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


@app.command()
def submit(
    task_name: str = typer.Argument(..., help="Name of the registered task."),
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
):
    """Validate and serialize a task submission request.

    This command prepares a task request. Remote execution is not implemented yet.
    """
    try:
        args = json.loads(args_json)
        kwargs = json.loads(kwargs_json)

        if not isinstance(args, list):
            raise TypeError("--args must be a JSON array.")
        if not isinstance(kwargs, dict):
            raise TypeError("--kwargs must be a JSON object.")

        task = Task(
            task_name=task_name,
            args=tuple(args),
            kwargs=kwargs,
        )
        serialized = serialize_task(task)
    except (json.JSONDecodeError, TypeError, ValueError, TaskSerializationError) as exc:
        console.print(f"[red]Invalid task submission: {exc}[/red]")
        raise typer.Exit(code=1) from exc

    console.print("[green]Task request created successfully.[/green]")
    console.print(f"Task ID: {task.task_id}")
    console.print(f"Task name: {task.task_name}")
    console.print(f"Serialized request: {serialized}")
    console.print(
        "[yellow]Remote execution is not implemented yet; no task was executed.[/yellow]"
    )


if __name__ == "__main__":
    app()
