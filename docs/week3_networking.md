# Week 3 Networking: Routing and Failover

## 1. Introduction

MeshWeaver is a peer-to-peer task broker that allows nodes to communicate and distribute registered tasks across available workers. Its networking layer uses asynchronous TCP communication, structured JSON messages, peer monitoring, task routing, and retry mechanisms.

This document explains the networking architecture, message flow, task assignment, worker selection, task execution, and failover mechanisms implemented during Week 3.

## 2. Networking Architecture

The networking implementation consists of the following components:

- **Message Layer:** Defines a standard message format for communication between nodes.
- **TCP Communication Layer:** Sends and receives messages asynchronously.
- **Correlation Manager:** Matches responses with their corresponding requests using unique IDs.
- **Peer Manager:** Tracks peer activity, addresses, and connection timeouts.
- **Task Router:** Selects a suitable worker based on its health and resource usage.
- **Task Client:** Submits tasks and retries temporary communication failures.
- **Task Transport:** Transfers task requests and results between workers.
- **Security Layer:** Provides Ed25519 message-signing and verification utilities.

## 3. Message Format

MeshWeaver uses a standard `Message` structure for communication.

The message contains:

| Field | Description |
|---|---|
| type | Message type |
| sender_id | ID of the sending node |
| seq | Sequence number |
| payload | Message data |
| ts | Message timestamp |
| sig | Optional digital signature |
| correlation_id | Optional request-response identifier |

Messages are converted into JSON bytes before transmission and decoded after reception.

### Supported Message Types

- `PING` and `PONG` - Peer communication.
- `PEER_LIST` - Peer information exchange.
- `TASK_REQUEST` - Requests task execution.
- `TASK_RESULT` - Returns task execution results.
- `TASK_ASSIGN` - Assigns a task.
- `TASK_ACK` - Acknowledges task assignment.

## 4. TCP Communication

MeshWeaver uses Python's asynchronous networking capabilities for TCP communication.

The TCP layer provides:

1. Connection establishment between nodes.
2. Newline-delimited JSON message transmission.
3. Message decoding and validation.
4. Request handling and response generation.
5. Connection cleanup after communication.

The maximum TCP message size is 64 KB in `net/tcp_node.py`. Oversized or invalid messages are rejected.

### TCP Request Flow

1. The client establishes a TCP connection.
2. It generates a unique correlation ID.
3. The request message is sent to the server.
4. The server reads and processes the message.
5. The server sends the appropriate response.
6. The client checks the correlation ID and receives the response.
7. The connection is closed.

## 5. Task Assignment and Acknowledgement

The task assignment mechanism uses two message types:

- `TASK_ASSIGN`
- `TASK_ACK`

The client sends a task assignment containing the task object and task ID.

The server validates the task information and checks that the task ID matches the ID inside the task object.

If the assignment is valid, the server responds with an acknowledgement containing `accepted: true`. Otherwise, it returns a rejection with an error message.

The `assign_task()` function supports retries for temporary network failures using exponential backoff.

## 6. Task Routing and Worker Selection

The task router selects a suitable worker using the available node information.

The routing policy checks:

- The worker must be in the `ALIVE` state.
- The worker information must be fresh.
- CPU usage must be below 95%.
- RAM usage must be below 95%.
- Excluded workers must not be selected.

The default maximum age of worker information is 15 seconds.

Among eligible workers, the router selects the worker with the lowest CPU usage. RAM usage and node ID are used as tie-breakers.

If no eligible worker is available, the router raises `NoAvailableNodes`.

## 7. Task Execution and Result Transfer

The task execution flow uses `TASK_REQUEST` and `TASK_RESULT`.

1. The TaskClient obtains the latest worker information.
2. The router selects an eligible worker.
3. The task is serialized and sent through TCP.
4. The worker validates and deserializes the task.
5. The registered execution engine executes the task.
6. The worker creates a task result.
7. The result is converted into a JSON-compatible response.
8. The client receives and validates the result.

The task transport also supports optional SSL contexts for encrypted TCP communication when configured.

## 8. Retry and Failover Mechanism

MeshWeaver uses retry mechanisms to handle temporary communication failures.

The TaskClient:

- Selects a worker from the latest available snapshot.
- Sends the task to the selected worker.
- Detects communication failures and invalid responses.
- Excludes the failed worker from the next selection attempt.
- Attempts another eligible worker when retries remain.
- Returns a failed task result if all attempts are exhausted.

If no eligible worker is temporarily available, the client waits briefly and refreshes the worker selection.

The task assignment handshake also uses exponential backoff when temporary network errors occur.

These mechanisms improve task delivery reliability when workers become unavailable.

**Note:** A network failure can occur after a worker has already executed a task but before its result reaches the client. Therefore, retrying does not guarantee exactly-once execution for tasks with external side effects.

## 9. Peer Monitoring

The `PeerConnectionManager` maintains information about peer activity.

It supports:

- Recording peer activity.
- Updating peer network addresses.
- Retrieving known peer addresses.
- Identifying expired peers.
- Removing expired peers.
- Checking whether a peer is alive.

The default peer timeout is 30 seconds. Peers that exceed this timeout are considered expired by the manager.

## 10. Security Mechanisms

MeshWeaver includes an Ed25519 signing utility in `security/signing.py`.

It provides:

- Message signing using a private key.
- Signature verification using a public key.
- Canonical message serialization before signing.
- Public-key export in raw 32-byte format.

The signing utility supports message integrity and authenticity checks when integrated into the communication flow.

**Current implementation note:** The inspected TCP message handlers do not automatically sign outgoing messages or verify incoming signatures. Signature enforcement is therefore not claimed as an active part of the current TCP communication flow.

## 11. Conclusion

The Week 3 networking implementation provides asynchronous TCP communication, structured message exchange, worker selection, task submission, retry handling, peer monitoring, and task result transfer.

These components help MeshWeaver distribute tasks among available workers and recover from temporary communication failures. The existing signing utilities and optional SSL support provide a foundation for further security integration.
