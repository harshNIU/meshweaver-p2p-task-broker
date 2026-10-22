import asyncio

import typer
from rich.console import Console

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
def submit():
    """Submit a task to MeshWeaver."""
    console.print("[yellow]Submit command is not implemented yet.[/yellow]")


if __name__ == "__main__":
    app()
