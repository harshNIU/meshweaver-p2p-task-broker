import typer
from rich.console import Console

app = typer.Typer(help="MeshWeaver peer-to-peer task broker CLI")
console = Console()


@app.command()
def start():
    """Start a MeshWeaver node."""
    console.print("[yellow]Start command is not implemented yet.[/yellow]")


@app.command()
def submit():
    """Submit a task to MeshWeaver."""
    console.print("[yellow]Submit command is not implemented yet.[/yellow]")


if __name__ == "__main__":
    app()
