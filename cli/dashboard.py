from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table

console = Console()


def create_dashboard() -> Layout:
    """Create a mock MeshWeaver network dashboard."""

    layout = Layout()

    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
    )

    layout["body"].split_row(
        Layout(name="mesh", ratio=1),
        Layout(name="tasks", ratio=2),
    )

    # Dashboard header
    layout["header"].update(
        Panel("[bold cyan]MeshWeaver - P2P Task Broker Dashboard[/bold cyan]")
    )

    # Mock network nodes
    mesh_table = Table(title="Network Nodes")
    mesh_table.add_column("Node ID", style="cyan")
    mesh_table.add_column("Address")
    mesh_table.add_column("Status", style="green")

    mesh_table.add_row("Node-01", "127.0.0.1:9001", "ACTIVE")
    mesh_table.add_row("Node-02", "127.0.0.1:9002", "ACTIVE")
    mesh_table.add_row("Node-03", "127.0.0.1:9003", "ACTIVE")

    layout["mesh"].update(Panel(mesh_table, title="Mesh Overview"))

    # Mock task information
    task_table = Table(title="Task Overview")
    task_table.add_column("Task ID", style="cyan")
    task_table.add_column("Task Name")
    task_table.add_column("Assigned Node")
    task_table.add_column("Status", style="green")

    task_table.add_row("TASK-001", "Data Processing", "Node-01", "COMPLETED")
    task_table.add_row("TASK-002", "Image Analysis", "Node-02", "RUNNING")
    task_table.add_row("TASK-003", "Report Generation", "Node-03", "PENDING")

    layout["tasks"].update(Panel(task_table, title="Task Overview"))

    return layout


def main() -> None:
    """Display the dashboard."""

    console.print(create_dashboard())


if __name__ == "__main__":
    main()
