# Member 2 validation — 24 October 2026

Validated locally on macOS, Python 3.12, using the repository's pinned test,
CLI and lint dependencies. These are measured results, not target results.
GitHub Actions has also been configured to run the tests and demos; this report
does not claim a remote CI pass.

| Check | Observed result |
|---|---|
| `python -m pytest -q` | 110 passed in 2.59 seconds |
| `python -m scripts.spawn_nodes --nodes 10 --base-port 9000` | PASS; 10 independent Python processes, ports 9000–9009 |
| Discovered peers per process | 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 |
| Replicated network-state value | 10 copies; remote process read succeeded |
| Process audit elapsed time | 0.851 seconds (single local run, not a statistical benchmark) |
| `python -m scripts.network_audit` | PASS; 10 UDP nodes plus a new reader with no local value |
| Remote FIND_VALUE | Newly joined reader retrieved the stored object |
| `python -m scripts.routing_demo` | PASS; result 328350, first submission 1 attempt, after stopping lowest-CPU worker 2 attempts |
| Black and Ruff | Passed for cli, tests, dht, tasks and scripts |
| `pip check` | No broken requirements |

## What the tests cover

- SHA-1 identities, invalid IDs, XOR distances, self-exclusion, bucket boundaries.
- Full bucket LRU policy, retention of live peers, replacement of dead peers.
- Single-seed discovery, multi-hop lookups and alpha=3 concurrency bounds.
- Stale-bucket refresh, dead contact removal and rejoining at a new address.
- Malformed datagrams, mismatched RPC responses, timeouts and shutdown cleanup.
- JSON value expiry, null values, copying, capacity and size limits.
- CPU/RAM/ID tie-breaking; unhealthy, stale and overloaded worker exclusion.
- Real TCP execution, concurrent duplicate coalescing, failed-worker retry,
  stable task IDs, timeout handling, retry limits and application errors.
- Existing networking, executor, CLI, gossip model and contract regressions.

Only one seed address was supplied to each joining process. IDs and peer
addresses were learned through DHT queries. The process controller did not
supply peer lists. The single-process audit additionally verifies FIND_VALUE
from a node that joined after storage and therefore has no cached value.

The routing demo uses synthetic load snapshots and the existing named-function
executor. It is not evidence of live gossip convergence, arbitrary-function
cloudpickle transfer, heartbeat detection, signed traffic or TLS deployment.
See [the review guide](member2_review.md) for integration boundaries.
