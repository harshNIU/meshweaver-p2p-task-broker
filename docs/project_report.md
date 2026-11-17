MeshWeaver - Peer-to-Peer Task Broker



## 1. Abstract



MeshWeaver is a Peer-to-Peer (P2P) Task Broker designed to distribute computational tasks among multiple nodes in a network. It supports peer discovery, task routing, remote task execution, result handling, and fault recovery. The system uses UDP for peer communication and discovery and TCP for reliable task messaging. It also provides command-line and web dashboard interfaces for monitoring and interaction.



## 2. Introduction



In distributed computing, a single computer may not have enough resources to process every task efficiently. MeshWeaver addresses this challenge by allowing multiple nodes to communicate and share workloads.



Each node can participate in the network, discover other peers, and contribute computing resources. Tasks can be routed to available workers, and the system includes mechanisms to recover when a worker becomes unavailable.



## 3. Objectives



\- Develop a peer-to-peer task-sharing system.

\- Enable automatic peer discovery and communication.

\- Distribute tasks among available worker nodes.

\- Support remote task execution and result collection.

\- Provide retry and failover mechanisms.

\- Offer CLI and web dashboard interfaces.

\- Validate the system through automated tests and multi-node experiments.



## 4. System Architecture



MeshWeaver consists of the following major components:



## 4.1 Networking Layer

Handles communication between nodes using UDP and TCP.



## 4.2 Peer Discovery and DHT

Maintains information about network peers and supports distributed data storage and lookup.



## 4.3 Task Routing

Selects suitable worker nodes based on their availability and resource information.



4.4 Execution Engine

Registers functions, executes tasks, tracks task status, and collects results.



## 4.5 CLI and Web Dashboard

Provides commands to start nodes and interact with the system. The web dashboard exposes monitoring and task-submission endpoints.



## 5. Technologies Used



\- Python

\- UDP and TCP networking

\- asyncio

\- Typer for command-line interaction

\- Rich for terminal dashboard presentation

\- pytest for automated testing

\- Black for code formatting

\- Ruff for linting

\- HTTP and Server-Sent Events for web dashboard endpoints

\- Cloudpickle for trusted Python callable serialization



## 6. Task Execution Workflow



1\. A node starts and initializes its networking services.

2\. The node discovers other available peers.

3
