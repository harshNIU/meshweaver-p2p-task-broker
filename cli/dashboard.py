"""Rich dashboard for live MeshWeaver node state."""

import asyncio
from collections.abc import Callable
from typing import Any

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

console = Console()


def _percentage(value: Any) -> str:
    """Format a resource usage value as a percentage."""
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return "N/A"


def create_dashboard(snapshot: dict | None = None) -> Layout:
    """Create a dashboard using live node and task snapshot data."""
    snapshot = snapshot or {}

    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
    )
    layout["body"].split_row(
        Layout(name="mesh", ratio=1),
        Layout(name="tasks", ratio=2),
    )

    node_id = snapshot.get("self", "Waiting for live node")
    layout["header"].update(
        Panel(
            f"[bold cyan]MeshWeaver - P2P Task Broker Dashboard[/bold cyan]"
            f"  |  Node: [bold]{node_id}[/bold]"
        )
    )

    mesh_table = Table(title="Network Nodes")
    mesh_table.add_column("Node ID", style="cyan")
    mesh_table.add_column("Address")
    mesh_table.add_column("Status")
    mesh_table.add_column("CPU", justify="right")
    mesh_table.add_column("RAM", justify="right")

    nodes = snapshot.get("nodes", [])
    if nodes:
        for node in nodes:
            status = str(node.get("state", "UNKNOWN")).upper()
            status_style = {
                "ALIVE": "green",
                "SUSPECT": "yellow",
                "DEAD": "red",
            }.get(status, "white")

            address = f"{node.get('host', 'N/A')}:{node.get('port', 'N/A')}"
            mesh_table.add_row(
                str(node.get("node_id", "Unknown")),
                address,
                f"[{status_style}]{status}[/{status_style}]",
                _percentage(node.get("cpu")),
                _percentage(node.get("ram")),
            )
    else:
        mesh_table.add_row("No live nodes", "-", "-", "-", "-")

    layout["mesh"].update(Panel(mesh_table, title="Mesh Overview"))

    task_table = Table(title="Task Overview")
    task_table.add_column("Task ID", style="cyan")
    task_table.add_column("Task Name")
    task_table.add_column("Assigned Node")
    task_table.add_column("Status")

    tasks = snapshot.get("tasks", [])
    if tasks:
        for task in tasks:
            task_table.add_row(
                str(task.get("task_id", "N/A")),
                str(task.get("name", task.get("task_name", "N/A"))),
                str(task.get("node") or "Unassigned"),
                str(task.get("state", "UNKNOWN")).upper(),
            )
    else:
        task_table.add_row("No tasks", "-", "-", "-")

    layout["tasks"].update(Panel(task_table, title="Task Overview"))

    return layout


async def run_live_dashboard(
    snapshot_provider: Callable[[], dict],
    refresh_interval: float = 1.0,
) -> None:
    """Refresh the Rich dashboard using snapshots from a running node."""
    with Live(
        create_dashboard(snapshot_provider()),
        console=console,
        refresh_per_second=4,
        screen=True,
    ) as live:
        while True:
            await asyncio.sleep(refresh_interval)
            live.update(create_dashboard(snapshot_provider()))


def main() -> None:
    """Display an empty dashboard until connected to a live node."""
    console.print(create_dashboard())


if __name__ == "__main__":
    main()
