py# MeshWeaver — P2P Task Broker

MeshWeaver is a collaborative Python project exploring peer-to-peer communication, distributed task execution, node discovery, routing, and fault-tolerant coordination.

This branch, `Shrinidhi-async-network`, focuses on UDP networking, structured messages, peer discovery, PING/PONG heartbeats, peer timeout management, a four-node local launcher, and CLI-based task submission.

> **Implementation note:** The project contains additional team-developed DHT, routing, gossip, metrics, and security components. Their exact integration status should be assessed from the relevant module implementations and team review documentation.

## Features

### Networking
- Asynchronous UDP node communication using Python's `asyncio`.
- Structured JSON message envelopes.
- PING/PONG communication and peer discovery.
- Periodic heartbeat messages for peer liveness.
- Peer activity tracking and timeout cleanup.
- Handling of malformed peer-state records.

### Task execution
- Built-in `add` and `multiply` demo tasks.
- CLI-based task submission to a running node.
- UDP task requests and task-result responses.
- Task result and failure reporting.

### Local development tools
- PowerShell script for launching four local nodes.
- Web dashboard for observing the live runtime.
- Automated tests for networking, peer management, discovery, routing, and task-related behavior.

### Technologies
- Python
- `asyncio` and UDP sockets
- JSON
- Typer CLI
- `psutil` for resource metrics
- `unittest` and pytest-compatible testing
- Git and GitHub

## Prerequisites

- Git
- Python installed and accessible through `python`
- Windows PowerShell for the supplied four-node launcher

The development environment used for validation included Python 3.14.8.

## Installation

### 1. Clone the repository

```powershell
git clone https://github.com/harshNIU/meshweaver-p2p-task-broker.git
cd meshweaver-p2p-task-broker
```

### 2. Create a virtual environment

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks environment activation, use the virtual environment's Python executable directly rather than changing the machine's execution policy.

### 3. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

## Run the Four-Node Demonstration

The repository includes `start_four_nodes.ps1`, which starts four local nodes on UDP ports 8000–8003. The launcher registers the built-in demo tasks and starts the web dashboard on port 8765.

### 1. Start the nodes

From the project root, in PowerShell:

```powershell
.\start_four_nodes.ps1
```

Expected startup messages include:

```text
Starting node-8000...
Starting node-8001...
Starting node-8002...
Starting node-8003...

All four nodes started.
Dashboard: http://127.0.0.1:8765/
Demo tasks: add, multiply
```

Keep this terminal open while running the demo.

### 2. Open the dashboard

Visit:

http://127.0.0.1:8765/

The dashboard provides a live view of the running network and its available runtime information.

Observed-node counts can temporarily include client nodes created during task submission. They should not automatically be interpreted as the number of long-running worker processes.

### 3. Submit an addition task

Open a **second PowerShell terminal** in the project directory and activate the virtual environment if necessary.

Run:

```powershell
python -m cli.main submit add --args "[2,3]" --host 127.0.0.1 --port 8000 --timeout 10
```

Expected successful result:

```text
Submitted task 'add'
Received TASK_RESULT from node-8000
Status: completed
Result: 5
```

### 4. Submit a multiplication task

```powershell
python -m cli.main submit multiply --args "[4,5]" --host 127.0.0.1 --port 8000 --timeout 10
```

Expected successful result:

```text
Submitted task 'multiply'
Received TASK_RESULT from node-8000
Status: completed
Result: 20
```

The commands and results above were verified during local development.

### 5. Stop the nodes

Return to the terminal running `start_four_nodes.ps1` and press:

```text
Ctrl+C
```

The launcher attempts to stop the node processes it started.

## Run the Tests

With the virtual environment activated, run the project's test suite:

```powershell
python -m unittest discover -s tests -v
```

**Recorded validation result:** 89 tests ran successfully, with `OK`.

The test suite covers networking, PING/PONG exchange, peer discovery, timeout handling, malformed messages, task-result handling, and other repository components.

Run the tests again after future code changes; a previous successful test run does not guarantee that later changes are correct.

The repository also supports pytest-based workflows where configured:

```powershell
python -m pytest -q
```

## Command Reference

| Purpose | Command |
|---|---|
| Start four local nodes and dashboard | `.\start_four_nodes.ps1` |
| Open dashboard | `http://127.0.0.1:8765/` |
| Submit addition | `python -m cli.main submit add --args "[2,3]" --host 127.0.0.1 --port 8000 --timeout 10` |
| Submit multiplication | `python -m cli.main submit multiply --args "[4,5]" --host 127.0.0.1 --port 8000 --timeout 10` |
| Run unit tests | `python -m unittest discover -s tests -v` |
| View CLI help | `python -m cli.main --help` |
| Stop local nodes | Press `Ctrl+C` in the launcher terminal |

## Repository Structure

```text
meshweaver-p2p-task-broker/
├── cli/
│   └── main.py
├── docs/
│   ├── member2_review.md
│   ├── network_audit.md
│   ├── networking.md
│   └── setup.md
├── execution/
├── gossip/
├── net/
│   ├── bootstrap.py
│   ├── messages.py
│   ├── peer_manager.py
│   └── udp_node.py
├── routing/
├── security/
├── tests/
├── web/
├── start_four_nodes.ps1
├── requirements.txt
└── README.md
```

This is an illustrative overview, not a guaranteed exhaustive tree. Actual files may differ between branches.

## Team Contributions

### Member 1 — Networking

Networking work in `Shrinidhi-async-network` includes:

- UDP node communication.
- Structured message envelopes.
- PING/PONG exchange.
- Peer activity and timeout tracking.
- Heartbeat support.
- Peer discovery validation.
- Four-node local launcher.
- Networking and peer-management tests.

### Collaborative work

The repository includes additional team-developed functionality for DHT discovery, routing, task execution, gossip, metrics, and other components. Credit each contribution according to the actual work completed and the team's agreed responsibilities.

For the DHT and routing review, consult `docs/member2_review.md` and `docs/network_audit.md` when those files are present in the checked-out branch.

## Limitations and Security Notes

- The four-node demonstration runs locally on one machine; it is not by itself proof of deployment across multiple computers.
- The dashboard's displayed topology should be interpreted according to the actual runtime data and dashboard implementation.
- CPU and RAM measurements can vary between runs.
- A message field reserved for a signature does not, by itself, provide cryptographic signing or verification.
- Do not assume encryption, authentication, or production-grade security without corresponding implementation and tests.
- The successful `add` and `multiply` demonstrations establish those specific task workflows, not every possible distributed workload or failure scenario.

## Development Workflow

Before committing changes, check the working tree:
git status


Run the test suite:
python -m unittest discover -s tests -v

Review whitespace errors:


git diff --check


Stage only the intended files:


git add README.md start_four_nodes.ps1


Commit:
git commit -m "docs: document local task execution workflow"


Push your branch:
git push origin Shrinidhi-async-network


If the branch has already been merged into `main`, merge or cherry-pick subsequent changes through your team's normal workflow. A new commit on the feature branch does not automatically update `main`.


## License

Add the license selected by the project maintainers. Do not claim a license until the team has confirmed and included it.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project setup, testing, code quality checks, branch workflow, and pull request guidelines.