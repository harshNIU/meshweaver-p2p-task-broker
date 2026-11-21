py# MeshWeaver — P2P Task Broker

**MeshWeaver** is a Python-based peer-to-peer (P2P) task broker designed to support communication between distributed nodes, peer discovery, task routing, asynchronous task execution, and fault-tolerant coordination.

Instead of depending entirely on a central broker, MeshWeaver allows nodes to communicate and coordinate through networking protocols and distributed components.

This branch, `Shrinidhi-async-network`, focuses on asynchronous networking, UDP and TCP communication, peer management, heartbeat monitoring, task communication, and networking security.

---

## Features

### 1. Asynchronous Networking
- UDP-based peer-to-peer communication using Python `asyncio`.
- TCP communication for reliable message delivery.
- Structured JSON message envelopes.
- PING/PONG communication.
- Automatic peer discovery and peer-list exchange.
- Periodic heartbeat monitoring.
- Peer activity tracking and timeout cleanup.
- Asynchronous outgoing message queue.
- Large UDP message forwarding through TCP.

### 2. Peer Management and Discovery
- Peer information management.
- Node registry and peer tracking.
- ALIVE, SUSPECT, and DEAD peer states.
- Periodic heartbeat messages.
- Detection and cleanup of expired peers.
- Multi-node network communication.

### 3. Task Communication and Execution
- Task request and assignment messages.
- Task result and acknowledgement handling.
- Request-response communication.
- Correlation IDs for matching responses with requests.
- Request timeout handling.
- Task execution and result reporting.
- Retry and failover support through the task execution workflow.

### 4. Distributed Hash Table (DHT)
- Multi-node DHT network audit.
- Distributed data replication.
- Remote value retrieval.
- Peer connectivity validation.

### 5. Networking Security
- Shared-secret message authentication.
- Message signing and verification support.
- Optional TLS support for TCP connections.
- Authenticated task communication.
- Correlation ID validation.

### 6. Web Dashboard and Development Tools
- Optional browser-based dashboard.
- Runtime state monitoring.
- Four-node local launcher.
- CLI-based task submission.
- Automated tests for networking and other project components.

---

## Technologies Used

| Technology | Purpose |
|---|---|
| Python 3.14 | Main programming language |
| asyncio | Asynchronous programming |
| UDP | Peer communication and discovery |
| TCP | Reliable message transport |
| JSON | Message serialization |
| SSL/TLS | Optional secure TCP connections |
| Typer | Command-line interface |
| pytest | Automated testing |
| unittest | Unit and asynchronous tests |
| Git and GitHub | Version control and collaboration |

---

## Project Architecture

MeshWeaver consists of several major components:

1. **CLI:** Starts the application and accepts commands and configuration.
2. **Networking:** Handles UDP and TCP communication between nodes.
3. **Peer Management:** Maintains peer information and heartbeat status.
4. **DHT:** Supports distributed data storage and retrieval.
5. **Task Routing:** Coordinates task selection and distribution.
6. **Execution Engine:** Handles task execution and lifecycle tracking.
7. **Task Registry:** Maintains registered tasks and task information.
8. **Web Dashboard:** Provides a browser-based view of runtime information.
9. **Tests:** Verifies networking, routing, execution, and integration behaviour.

### Simplified Architecture

```text
                 MeshWeaver P2P Network
                          |
             +------------+------------+
             |                         |
         Node A                      Node B
             |                         |
       UDP Communication <----------> UDP
       TCP Communication <----------> TCP
             |                         |
       Peer Discovery              Peer Discovery
       Heartbeat                   Heartbeat
             |                         |
             +------------+------------+
                          |
                    Task Routing
                          |
                   Task Execution
                          |
                    Task Results

                 Optional Web Dashboard
```

---

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

### 3. Activate the virtual environment

```powershell
.\.venv\Scripts\Activate.ps1
```

### 4. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

---

## Running the Four-Node Dashboard Demo

The repository includes a PowerShell launcher for starting four local nodes and the optional web dashboard.

From the project root, run:

```powershell
.\start_four_nodes.ps1
```

The documented configuration uses UDP ports `8000–8003` and dashboard port `8765`.

Open the dashboard in a browser on the same computer:

```text
http://127.0.0.1:8765/
```

Keep the launcher terminal open while using the dashboard.

### Submitting Demo Tasks

Open a second PowerShell terminal in the project directory and activate the virtual environment.

**Addition:**

```powershell
python -m cli.main submit add --args "[2,3]" --host 127.0.0.1 --port 8000 --timeout 10
```

Expected calculation:

```text
2 + 3 = 5
```

**Multiplication:**

```powershell
python -m cli.main submit multiply --args "[4,5]" --host 127.0.0.1 --port 8000 --timeout 10
```

Expected calculation:

```text
4 * 5 = 20
```

The dashboard is an optional runtime interface. Its local task submission callback should not be confused with proof of remote distributed task execution.

### Stopping the Demo

Return to the launcher terminal and press:

```text
Ctrl + C
```

---

## Networking Demonstration

### 1. Ten-Node DHT Network Audit

Run:

```powershell
python -m scripts.spawn_nodes --nodes 10 --base-port 9000
```

Latest recorded live result:

| Metric | Result |
|---|---:|
| Status | PASS |
| Independent UDP processes | 10 |
| Ports | 9000–9009 |
| Peers discovered per node | 9 |
| DHT replicas | 10 |
| Remote value retrieval | Successful |
| Execution time | 8.916 seconds |
| Node shutdown | All 10 stopped cleanly |

### 2. Task Execution and Worker Failure Recovery

Run:

```powershell
pytest -v tests/test_smoke_5nodes.py
```

The test starts five independent worker processes and submits 20 slow-square tasks.

During execution, two workers are terminated to simulate worker failures.

**Latest recorded result:**

- Five worker processes started.
- Twenty tasks submitted.
- Two workers terminated during execution.
- Remaining workers handled recovery through retry and failover.
- All 20 tasks completed successfully.
- Returned results matched the expected square values.

```text
1 passed in 5.85s
```

These are separate demonstrations: the 10-node DHT audit and the five-worker task recovery test.

---

## Running the Complete Test Suite

Activate the virtual environment and run:

```powershell
pytest -q
```

### Latest Full Test Result

```text
313 passed in 25.51s
```

<details>
<summary>Validation summary</summary>

| Test / Demonstration | Result |
|---|---|
| Complete automated test suite | 313 passed |
| Ten-node DHT network audit | PASS |
| All-to-all peer discovery | PASS |
| DHT replication | PASS |
| Remote value retrieval | PASS |
| Twenty-task execution | PASS |
| Recovery after two worker failures | PASS |
| Pytest warnings in latest run | None |

</details>

---

## Networking Implementation Details

### UDP Communication

Implemented in `net/udp_node.py`.

The UDP networking module uses Python's `asyncio.DatagramProtocol`.

Main responsibilities:
- Sending and receiving UDP messages.
- Processing PING and PONG messages.
- Sharing peer information.
- Maintaining peer records.
- Handling incoming task requests.
- Managing outgoing messages through an asynchronous queue.
- Forwarding large messages through TCP.

### TCP Communication

Implemented in `net/tcp_node.py`.

Main responsibilities:
- TCP stream communication.
- Newline-delimited JSON messages.
- Task request and assignment handling.
- Task result and acknowledgement handling.
- Request timeouts.
- Correlation ID management.
- Authenticated message verification.

### Peer Heartbeat

The configured heartbeat interval is five seconds.

Heartbeat messages help identify active peers. Expired peer records are removed through the cleanup mechanism.

### Networking Security

The networking implementation supports:
- Shared-secret message authentication.
- Message signing and verification.
- Optional TLS connections.
- Request-response correlation validation.

---

## Repository Structure

```text
meshweaver-p2p-task-broker/
│
├── cli/
│   └── main.py
│
├── dht/
│   ├── ids.py
│   ├── lookup.py
│   ├── node.py
│   ├── routing_table.py
│   ├── rpc.py
│   └── store.py
│
├── docs/
│   ├── architecture.md
│   ├── demo_run.md
│   ├── networking.md
│   ├── network_audit.md
│   ├── setup.md
│   ├── tasks.md
│   └── usage.md
│
├── execution/
├── gossip/
│
├── net/
│   ├── messages.py
│   ├── peer_manager.py
│   ├── tcp_node.py
│   └── udp_node.py
│
├── routing/
├── security/
├── tasks/
├── tests/
├── web/
│
├── conftest.py
├── pytest.ini
├── requirements.txt
├── start_four_nodes.ps1
└── README.md
```

This is an overview of the project structure; actual files may vary between branches.

---

## Team Contributions

MeshWeaver is a collaborative project. Different members contribute to networking, task routing, execution, dashboard development, and other system components.

### Member 1 — Async Networking and Security

My contribution focuses on the networking module.

Work includes:

- UDP peer communication.
- TCP message transport.
- Structured message handling.
- PING/PONG communication.
- Peer discovery and peer-list exchange.
- Heartbeat monitoring.
- Peer timeout management and cleanup.
- Asynchronous message queue.
- Task request and response communication.
- Correlation IDs and request timeouts.
- Shared-secret authentication and TLS support.
- Networking testing and documentation.

### Collaborative Project Work

The complete repository also contains DHT, routing, task execution, gossip, metrics, and dashboard components developed collaboratively by the team.

Individual contributions should be credited according to the actual work completed by each member.

---

## Limitations and Notes

- The four-node launcher is a local demonstration, not proof of deployment across multiple physical computers.
- DHT and worker-recovery demonstrations are separate tests.
- Dashboard state reporting does not automatically prove remote task execution.
- Network timing and resource measurements may vary between runs.
- Optional TLS support requires appropriate SSL configuration.
- The project is intended as an academic and development project; production deployment requires additional operational and security validation.

---

## Development Workflow

Check the current branch and working tree:

```powershell
git status
```

Run the complete test suite:

```powershell
pytest -q
```

Check whitespace errors:

```powershell
git diff --check
```

Stage the README change:

```powershell
git add README.md
```

Commit:

```powershell
git commit -m "docs: update MeshWeaver project README"
```

Push your branch:

```powershell
git push origin Shrinidhi-async-network
```

---

## Documentation

Additional project documentation is available in the `docs/` directory:

- `architecture.md` — system architecture.
- `networking.md` — networking module and Member 1 contribution.
- `demo_run.md` — final demonstration results.
- `network_audit.md` — network audit information.
- `setup.md` — development setup.
- `tasks.md` — task-related documentation.
- `usage.md` — usage instructions.

---

## License

Add the license selected and approved by the project maintainers.

---

## Acknowledgement

This project was developed collaboratively as part of an internship project.

Through MeshWeaver, I gained practical experience with Python asynchronous programming, UDP and TCP communication, peer-to-peer networking, distributed systems, task execution, failure recovery, testing, debugging, and collaborative Git workflows.

**Project:** MeshWeaver — P2P Task Broker
**Branch:** `Shrinidhi-async-network`
**Role:** Member 1 — Async Networking and Security