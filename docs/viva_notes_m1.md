# MeshWeaver Mid Viva Notes — Member 1

## 1. What is MeshWeaver?

MeshWeaver is a peer-to-peer task broker project. Its networking module allows peer nodes to communicate and exchange messages.

## 2. What is my role?

My role is Member 1 — Async Networking and Security. My current networking work includes UDP communication, PING/PONG messaging, peer activity tracking, and TCP message exchange.

## 3. What is asynchronous programming?

Asynchronous programming allows a program to wait for operations such as network communication without unnecessarily blocking other work.

In Python, the asyncio library provides support for asynchronous networking.

## 4. Why do we use UDP?

UDP is a connectionless transport protocol. It is useful for lightweight control messages between peer nodes.

In my implementation, UDP is used for structured messages, PING/PONG communication, and plain-text messages.

UDP does not guarantee that every packet will arrive.

## 5. Why do we use TCP?

TCP provides a reliable, ordered byte stream between connected endpoints.

In my implementation, TCP is used to send and receive structured messages. The server returns an ACK response after processing each valid message.

## 6. What is the difference between UDP and TCP?

UDP sends independent datagrams without establishing a connection first. TCP establishes a connection and provides a reliable, ordered stream of bytes.

Our networking module uses UDP for control communication and TCP for message exchange that benefits from a reliable stream.

## 7. What are PING and PONG?

PING is a message used to check communication with another peer. PONG is the response.

In `net/udp_node.py`, when a structured PING message arrives, the receiving node sends a PONG message back to the sender.

## 8. What is a message envelope?

A message envelope is a structured representation of a message. Our Message class contains fields such as type, sender_id, seq, payload, timestamp, and signature placeholder.

The message is encoded as JSON bytes for transmission.

## 9. What is a sequence number?

A sequence number identifies a message within a sender's message sequence.

Our UDP implementation tracks structured messages using the combination of sender_id and seq. It ignores a message if that key has already been received.

## 10. How do you prevent duplicate messages?

The UDPProtocol class maintains a set called seen_messages.

For each structured message, it checks the pair (sender_id, seq). If that pair has already been seen, the message is ignored.

Using both fields means different senders can use the same sequence number.

## 11. How does peer timeout detection work?

PeerConnectionManager records the last activity time for each peer using Python's monotonic clock.

When the elapsed time reaches or exceeds timeout_seconds, the peer is considered expired. The manager can return expired peer IDs and remove them.

## 12. What is asyncio.DatagramProtocol?

It is a Python interface for handling UDP datagram events asynchronously.

Our UDPProtocol class implements callbacks such as connection_made, datagram_received, error_received, and connection_lost.

## 13. How does our TCP implementation work?

The client connects to a TCP server, sends a JSON message followed by a newline, and waits for the server's response.

The server reads the message, records it, and sends an ACK containing the original message type and sequence number.

## 14. Why do we use newline-delimited JSON over TCP?

TCP is a byte stream, so it does not automatically preserve application message boundaries.

Our implementation adds a newline after each JSON message and uses readline() to read one message at a time.

## 15. How do we handle invalid TCP messages?

The read_message function converts malformed messages into a ValueError. The server handles that error and closes the affected connection.

Our tests also check that a valid client can connect after a malformed message from another client.

## 16. How did I test my implementation?

I ran:

- `python -m unittest discover -s tests -v`
- `python -m black --check .`
- `python -m ruff check .`

The reported results were 36 tests passed, 32 files unchanged by the Black check, and no Ruff issues.

The tests cover bootstrap behavior, PING/PONG communication, peer timeouts, UDP message handling, and TCP message handling.

## 17. What are the current limitations?

- UDP does not guarantee packet delivery.
- The shown TCP implementation does not yet implement request correlation IDs, application-level timeouts, or retry/backoff logic.
- The message signature field is only a placeholder in the current message format; this code does not perform cryptographic signing or verification.
- Peer timeout detection is implemented, but a complete distributed failure-detection and recovery mechanism requires additional work.
- The current networking tests do not by themselves demonstrate a complete 10-node audit.

## 18. What is the next development work?

The planned work includes request/response correlation IDs and timeouts, TCP task assignment and acknowledgements, Ed25519 key management, message signing and verification, and further integration testing.

## 19. How should I explain my contribution?

I implemented and tested core asynchronous networking components for MeshWeaver. My work includes UDP message exchange, automatic PING/PONG responses, duplicate structured-message filtering, peer activity and timeout management, and TCP message exchange with acknowledgements. I also ran the project's unit and integration tests and code-quality checks.

## 20. Final viva tip

Explain each feature in this order:

1. What problem does it solve?
2. Which file implements it?
3. How does it work?
4. Which test verifies it?
5. What limitation remains?
