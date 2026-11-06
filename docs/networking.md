# MeshWeaver Networking Module

## 1. Purpose

The networking module allows MeshWeaver peer nodes to communicate with each other without depending on a central broker.

It uses asynchronous networking so that multiple network operations can run without blocking the entire application.

## 2. Networking Architecture

MeshWeaver uses two communication protocols:

- UDP: Used for peer communication, heartbeat messages, and peer information exchange.
- TCP: Used for task requests, task assignments, task results, and reliable message delivery.

## 3. UDP Communication

The UDP implementation is available in `net/udp_node.py`.

The main class is `UDPProtocol`, which extends Python's `asyncio.DatagramProtocol`.

Its responsibilities include:

- Sending and receiving UDP messages.
- Handling incoming peer messages.
- Maintaining information about connected peers.
- Processing PING and PONG messages.
- Sharing and merging peer lists.
- Handling task requests received through UDP.
- Managing large messages by forwarding them through TCP.

## 4. Peer Management and Heartbeat

MeshWeaver maintains peer information using the peer manager and node registry.

The heartbeat mechanism sends PING messages periodically. The configured heartbeat interval is 5 seconds.

Peer information can be exchanged through PEER_LIST messages.

The cleanup mechanism checks expired peers and removes outdated peer information.

Node states include:

- ALIVE: The peer is active.
- SUSPECT: The peer may be unavailable.
- DEAD: The peer is considered unavailable.

## 5. Asynchronous Message Queue

The UDP module uses an asynchronous send queue.

The queue worker processes outgoing messages in the background.

This helps prevent network sending operations from blocking other asynchronous tasks.

The default send queue size is 100 messages.

## 6. TCP Communication

The TCP implementation is available in `net/tcp_node.py`.

Its main responsibilities include:

- Reading and writing messages through TCP streams.
- Handling task requests and task assignments.
- Sending task results and acknowledgements.
- Managing request timeouts.
- Matching responses with their original requests.

TCP messages use newline-delimited JSON.

The maximum TCP message size is 64 KB.

## 7. Request and Response Handling

The `send_tcp_message()` function sends a TCP request and waits for its response.

It uses a correlation ID to match a response with the correct request.

The function also:

- Supports request timeouts.
- Signs authenticated task messages.
- Verifies authenticated responses.
- Supports optional TLS connections.
- Rejects responses with mismatched correlation IDs.

The default request timeout is 5 seconds.

## 8. Security Support

The networking module supports message authentication using a shared secret.

Authenticated messages are signed before sending and verified after receiving.

TLS support is also available for TCP connections through an SSL context.

These mechanisms help protect task communication and verify message authenticity.

## 9. TCP Task Communication Flow

A simplified task communication flow is:

1. A peer creates a task request.
2. The request is sent to another peer.
3. The receiving peer validates the message.
4. The task is executed by the execution engine.
5. The receiver creates a task result.
6. The result is sent back to the requesting peer.
7. The sender matches the response using its correlation ID.

## 10. Technologies Used

- Python
- asyncio
- UDP
- TCP
- JSON
- SSL/TLS
- Shared-secret message authentication

## 11. Testing and Validation

The networking implementation was checked using:

- Two-node PING/PONG communication.
- Peer information exchange through PEER_LIST messages.
- Multi-node network testing.
- DHT network audit and remote value lookup.
- TCP message handling and task communication tests.

A 10-node network test successfully started 10 independent nodes.

The network audit also verified peer connectivity, store replicas, and remote value lookup.

## 12. Member 1 Contribution

My contribution focused on the networking module.

The work included:

- UDP peer communication.
- TCP message transport.
- Asynchronous message handling.
- Heartbeat and peer cleanup mechanisms.
- Task request and response communication.
- Networking security and TLS integration.
- Testing and documentation of networking functionality.

