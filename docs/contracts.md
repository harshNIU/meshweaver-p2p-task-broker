\# MeshWeaver Shared Data Contracts



\## 1. Purpose



This document defines the shared data contracts used across MeshWeaver modules. The message envelope and node information structure should remain consistent across networking, gossip, CLI, execution, and testing modules.



\## 2. Message Envelope Contract



\*\*Source:\*\* `net/messages.py`



The `Message` dataclass is the standard envelope for communication between nodes.



\### Fields



| Field | Type | Default | Description |

|---|---|---|---|

| `type` | `str` | Required | Message type, such as `PING` or `PONG` |

| `sender\_id` | `str` | Required | Identifier of the sending node |

| `seq` | `int` | Required | Message sequence number |

| `payload` | `dict` | Required | Message-specific data |

| `ts` | `float` | `0.0` | Message timestamp |

| `sig` | `str` | `""` | Reserved signature field |



\### Encoding and decoding



\- `Message.encode()` converts the message into UTF-8 JSON bytes.

\- If `ts` is zero or otherwise falsy, encoding generates a timestamp using `time.time()`.

\- `Message.decode(raw)` converts UTF-8 JSON bytes back into a `Message` object.

\- Encoding and decoding should preserve the message fields and values, except that a zero timestamp is replaced during encoding.



\### Supported message types



The currently defined message constants are `PING` and `PONG`. Additional types may be added as the project evolves.



\### Signature status



The `sig` field is currently a placeholder. Cryptographic signing and signature verification are not implemented by this contract.



\## 3. Node Information Contract



\*\*Source:\*\* `gossip/state.py`



The `NodeInfo` dataclass stores a peer's identity, network address, resource usage, last-seen timestamp, and current state.



\### Fields



| Field | Type | Default | Description |

|---|---|---|---|

| `node\_id` | `str` | Required | Unique node identifier |

| `host` | `str` | Required | Hostname or IP address |

| `port` | `int` | Required | Node communication port |

| `cpu` | `float` | `0.0` | CPU usage information |

| `ram` | `float` | `0.0` | RAM usage information |

| `last\_seen` | `float` | `0.0` | Timestamp of last observed activity |

| `state` | `NodeState` | `"ALIVE"` | Current node status |



\### Supported node states



\- `ALIVE` — the node is considered active.

\- `SUSPECT` — the node may be unavailable.

\- `DEAD` — the node is considered unavailable.



The default state for a newly created `NodeInfo` is `ALIVE`.



\## 4. Compatibility Rules



1\. Modules must use the agreed field names and expected data types.

2\. Changes to required fields, defaults, serialization, or supported states must be coordinated with dependent modules.

3\. Message encoding and decoding must remain compatible with the agreed envelope structure.

4\. The `sig` field must not be treated as proof of authenticity until signing and verification are implemented.

5\. Contract tests must be run after changes to either shared data structure.



\## 5. Contract Tests



\*\*File:\*\* `tests/test\_contracts.py`



The contract tests verify:



\- Message envelope field names.

\- Default timestamp and signature values.

\- Message encoding and decoding.

\- Valid JSON serialization.

\- All three supported node states.

\- The expected fields in `NodeInfo`.



These contracts are frozen for current integration work. Future changes should be coordinated and tested before adoption.

