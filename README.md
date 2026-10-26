# MeshWeaver — P2P Task Broker

## Harsh's DHT and routing branch

Kademlia discovery and task routing are implemented on this branch. Start with
[the Member 2 review guide](docs/member2_review.md) for setup, code explanations,
integration APIs, and the exact limitations of the review demo.

```bash
python -m pip install -r requirements.txt
python -m pytest -q
python -m scripts.spawn_nodes --nodes 10 --base-port 9000
python -m scripts.routing_demo
```

The network audit runs ten independent Python processes. The task demo uses
the team's registered-function executor and proves lowest-load routing and
retry after a worker stops. [Recorded validation](docs/network_audit.md).

The team overview below describes the broader project; the Member 2 guide
states what this branch currently implements and what still requires integration.


MeshWeaver is a decentralized Python framework designed to explore peer-to-peer communication, distributed task execution, and fault-tolerant coordination across multiple computers.

The project is organized into networking, gossip, CLI, and security modules. It includes asynchronous UDP communication, structured peer messages, peer connection tracking, node metrics, and tests for core components.

## Overview

The goal of MeshWeaver is to provide a foundation for communication and coordination between peer nodes in a distributed system.

Instead of relying on a single central coordinator for all communication, peer-to-peer systems allow nodes to communicate directly. MeshWeaver explores the networking and coordination components needed to support this approach.

### Project goals

- Establish asynchronous communication between peer nodes.
- Define a consistent message format for node communication.
- Support PING/PONG messages to check peer communication.
- Track peer activity and handle connection timeouts.
- Maintain node information through gossip-related components.
- Provide a command-line interface foundation and automated tests.

## Features

### 1. Asynchronous UDP networking

The `net/udp_node.py` module uses Python's `asyncio` datagram APIs to send and receive UDP messages.

Current capabilities include:

- Starting a UDP node.
- Sending and receiving messages.
- Handling connection and communication events.
- Exchanging structured messages between nodes.
- Responding to PING messages with PONG messages.

### 2. Standard message format

The `net/messages.py` module defines a `Message` data class for structured communication.

The message envelope contains:

- `type` — message type, such as `PING` or `PONG`.
- `sender_id` — identifier of the sending node.
- `seq` — sequence number.
- `payload` — message-specific data.
- `ts` — timestamp.
- `sig` — field reserved for a signature value.

Messages can be encoded into JSON bytes and decoded back into message objects.

**Note:** The presence of the `sig` field does not mean message signing or cryptographic verification is implemented.

### 3. Peer connection management

The `net/peer_manager.py` module provides peer activity and timeout tracking.

Its tested behavior includes:

- Checking whether a peer is alive before its timeout.
- Detecting when a peer has exceeded its timeout.
- Refreshing a peer's activity time.
- Removing expired peers.
- Returning a negative result for unknown peers.

### 4. Node metrics

The `gossip/metrics.py` module uses `psutil` to sample CPU and RAM usage. Automated tests check that the metrics function returns CPU and RAM information.

### 5. Gossip and node state

The `gossip/` package contains protocol and state components for working with node information.

The current tests verify that merging can add a new node and preserve newer information, and that node information has expected default values.

### 6. Command-line interface foundation

The `cli/` package contains a Typer-based CLI entry point. Tests currently verify the placeholder behavior of the `start` and `submit` commands; these tests do not establish that a complete task-execution workflow is implemented.

### 7. Automated testing

The `tests/` directory contains tests for message encoding and decoding, UDP communication, PING/PONG behavior, peer management, metrics, gossip protocol, node state, and CLI placeholders.

## Technologies

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| `asyncio` | Asynchronous networking |
| UDP sockets | Datagram communication between peers |
| JSON | Structured message serialization |
| `dataclasses` | Message data structure |
| Typer | Command-line interface foundation |
| `psutil` | CPU and RAM metrics |
| `unittest` | Unit and asynchronous test support |
| `pytest` | Test discovery and execution |
| Git and GitHub | Version control and collaboration |

## Project architecture

```text
meshweaver-p2p-task-broker/
├── cli/
│   ├── __init__.py
│   └── main.py
├── docs/
│   ├── networking.md
│   └── setup.md
├── gossip/
│   ├── metrics.py
│   ├── protocol.py
│   └── state.py
├── net/
│   ├── __init__.py
│   ├── messages.py
│   ├── peer_manager.py
│   └── udp_node.py
├── security/
│   └── __init__.py
├── tests/
│   ├── test_cli.py
│   ├── test_messages.py
│   ├── test_metrics.py
│   ├── test_peer_manager.py
│   ├── test_ping_pong.py
│   ├── test_protocol.py
│   ├── test_state.py
│   └── test_udp_node.py
├── .gitignore
├── README.md
└── requirements.txt
```

The tree above describes the project structure established during development. Some files may change as development continues.

### How the components fit together

1. The networking module starts UDP endpoints and handles incoming datagrams.
2. The message module encodes and decodes structured messages.
3. A node can send a PING message and receive a PONG response.
4. The peer manager tracks peer activity and timeout status.
5. The gossip modules provide node metrics, state, and information-merging behavior.
6. The CLI provides an entry point for future command-line workflows.

The `security/` package currently provides a package foundation; completed encryption, authentication, or signature verification should not be assumed.

## Installation and setup

### Prerequisites

- Git
- Python installed and available through the `python` command
- A terminal such as PowerShell on Windows

Python 3.14.8 was used to run the project's tests during development.

### 1. Clone the repository

```powershell
git clone https://github.com/harshNIU/meshweaver-p2p-task-broker.git
cd meshweaver-p2p-task-broker
```

### 2. Create a virtual environment

On Windows PowerShell:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, use the Python executable inside `.venv` directly instead of changing system execution policies.

### 3. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 4. Run the automated tests

```powershell
python -m pytest -v
```

This command discovers and runs the project's pytest-compatible and unittest-based tests.

### 5. Review networking documentation

The networking documentation is located at:

```text
docs/networking.md
```

The repository also contains `docs/setup.md` for additional setup notes.

## Testing

The following test command was run successfully during development:

```powershell
python -m pytest -v
```

**Recorded result:** 15 tests passed, with 2 deprecation warnings from Typer/Click.

The tests cover:

- Message encoding and decoding.
- UDP message exchange between two local nodes.
- PING/PONG communication.
- Peer activity and timeout behavior.
- CPU and RAM metrics.
- Gossip information merging.
- Default node state.
- CLI placeholder commands.

The recorded test result describes the state of the project when these tests were run. Run the tests again after making changes to verify the current state.

## Team contributions

MeshWeaver is developed collaboratively, with separate modules for networking, gossip, CLI, and security.

### Member 1 — Networking

Networking work in the `Shrinidhi-async-network` branch includes:

- Setting up the `net/` Python package.
- Implementing asynchronous UDP communication.
- Defining the structured message envelope.
- Implementing PING/PONG communication.
- Adding peer connection management and timeout tracking.
- Adding tests for networking and peer management.
- Documenting networking responsibilities.

### Collaborative components

The repository also contains gossip, metrics, node-state, CLI, and security package components. Contributions to these modules should be credited to the relevant team members based on the team's actual work and agreed responsibilities.

## Development status

MeshWeaver has working, tested foundations for local UDP communication, structured messages, PING/PONG responses, peer timeout tracking, node metrics, and gossip-related state handling.

Distributed task execution, secure communication, and a complete CLI task-submission workflow should be considered future work unless they are implemented and verified separately.

## License

Add the project's license here once the team has selected and confirmed the applicable license.