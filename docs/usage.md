# MeshWeaver CLI Usage Guide

## Overview

MeshWeaver provides a Typer-based command-line interface for starting a local UDP node and submitting tasks in future workflows.

## Prerequisites

- Python installed
- Project dependencies installed
- The project's virtual environment activated

Install dependencies from the project root:

```powershell
python -m pip install -r requirements.txt
```

## Start a Node

From the project root, run:

```powershell
python -m cli.main start
```

By default, the node listens on `127.0.0.1` using UDP port `8000`.

## Choose a Port

Use the `--port` option to specify a different port:

```powershell
python -m cli.main start --port 8123
```

Valid port numbers are between `1` and `65535`.

The CLI displays a startup message when the node starts. Press **Ctrl+C** to stop it safely.

## View Command Help

To view the available commands:

```powershell
python -m cli.main --help
```

To view options for starting a node:

```powershell
python -m cli.main start --help
```

## Submit a Task

The `submit` command is currently a placeholder. Task submission is not yet implemented.

```powershell
python -m cli.main submit
```

## Networking Note

The current CLI binds to `127.0.0.1`, which is the loopback address. The node is accessible from the same computer, not directly from other computers on the network.

## Join an Existing Mesh

Use `--bootstrap host:port` to register a node with an existing MeshWeaver node and discover peers.

Start the first node in one terminal with `python -m cli.main start --port 9000`. In a second terminal, run `python -m cli.main start --port 9001 --bootstrap 127.0.0.1:9000`.

The bootstrap node must be running and reachable. The `--seed` option remains available as a backward-compatible alternative to `--bootstrap`.
