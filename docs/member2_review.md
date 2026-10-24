# Harsh — DHT (Kademlia) and Task Routing

This branch supplies Member 2's discovery, network-state storage, routing and
submission code. It builds on the team's existing `main` interfaces without
changing the `Message`, `NodeInfo`, `Task` or `TaskResult` fields.

## Run before your review

Python 3.11 or newer is required. From the repository root:

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS / Linux instead:
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
python -m scripts.spawn_nodes --nodes 10 --base-port 9000
python -m scripts.network_audit --nodes 10
python -m scripts.routing_demo
```

Use only the activation command for your operating system. If ports 9000–9009
are occupied, pass `--base-port 0` to allocate available ports automatically.
The demos terminate and clean up their sockets/processes after printing PASS.

For a visible live mesh, open three terminals:

```bash
python -m scripts.dht_node --port 9000
python -m scripts.dht_node --port 9001 --bootstrap 127.0.0.1:9000
python -m scripts.dht_node --port 9002 --bootstrap 127.0.0.1:9000
```

Each prints its own ID and discovered peers every five seconds. Stop one with
Ctrl+C; the remaining nodes remove its contact after failed probes. For a LAN
use `--host <your-LAN-IP>` and give the other machines that address as bootstrap.
Wildcard binding requires `--advertise-host <your-LAN-IP>`. Allow UDP through
the firewall. Multi-device behavior has not been verified in this audit.

## Five-minute presentation

1. Explain the problem: a central task broker is a single point of failure.
2. Show `dht/ids.py`: SHA-1 gives a 160-bit placement ID; XOR measures distance.
   SHA-1 here is an addressing function, not an authentication mechanism.
3. Show `dht/routing_table.py`: 160 buckets, at most 20 contacts per bucket.
   Self is excluded, repeated contacts move to the most recently seen end, and
   a full bucket probes its oldest contact before replacing a dead peer.
4. Run the ten-process audit: every process starts with only one seed address,
   discovers all nine other peers, and participates in replicated STORE/GET.
   The parent starts/stops processes and checks results; it does not distribute
   peer lists. Discovery occurs over UDP between child processes.
5. Show `dht/lookup.py`: at most alpha=3 concurrent FIND_NODE requests per
   lookup. Query the nearest unvisited candidates, merge replies, stop when the
   nearest k candidates are exhausted. Timeouts evict unresponsive contacts.
6. Run the routing demo: `sum_squares(100)` returns 328350. The lowest-CPU
   worker then stops; the next submission succeeds at another worker on attempt
   two. This demonstrates transport-failure retry, not heartbeat detection.
7. Show the tests and explain the boundaries listed below honestly.

## Files and responsibilities

| File | Responsibility |
|---|---|
| `dht/ids.py` | SHA-1 identifiers, validation, XOR distance, bucket index |
| `dht/routing_table.py` | Contacts, bounded LRU buckets, nearest-neighbour API |
| `dht/rpc.py` | DHT wire types and packet-size limit |
| `dht/lookup.py` | Iterative alpha-parallel discovery |
| `dht/node.py` | UDP integration, joining, RPC correlation, refresh, shutdown |
| `dht/store.py` | Bounded JSON values, TTL expiry, null-safe lookups |
| `tasks/router.py` | ALIVE/fresh/load filtering; CPU, RAM, ID ordering |
| `tasks/submit.py` | Submission, timeout, bounded retry, failed-worker exclusion |
| `tasks/transport.py` | TCP review adapter to the existing registered executor |
| `tests/test_dht.py`, `tests/test_router.py` | Unit and real-socket integration tests |
| `scripts/` | Independent-process audit, socket audit, live nodes, routing demo |

## Integration APIs for teammates

```python
from dht import DHTNode

node = await DHTNode.start(host="127.0.0.1", port=9001)
try:
    await node.join(("127.0.0.1", 9000))
    neighbours = node.nearest_neighbours(n=3)  # Contact(node_id, host, port)
    peers = await node.lookup(node.node_id)
    copies = await node.put("cluster-config", {"version": 1}, ttl=300)
    config = await node.get("cluster-config")  # raises KeyError if missing
finally:
    await node.close()
```

DHT IDs are normalized 40-character hexadecimal strings; integer IDs are also
accepted by the ID helpers. A Contact contains discovery information only.
Gossip should use the same IDs and obtain CPU/RAM from actual measurements.
Do not convert a newly discovered contact into a zero-load scheduling candidate:
`select_node` requires a positive, recent `NodeInfo.last_seen` timestamp.

```python
from tasks import TaskClient, select_node
from tasks.transport import send_task

# Supply your gossip table here, including local load if local work is allowed.
client = TaskClient(lambda: gossip_snapshot, send_task, max_age=15)
result = await client.submit_task("sum_squares", 100, max_retries=2)
```

`gossip_snapshot` is a mapping of IDs to the existing `gossip.state.NodeInfo` or
an iterable of NodeInfo. The TCP task endpoint must be reachable at its host
and port. TCP and UDP can share a numeric port, but a TaskServer must actually
be listening there; DHTNode alone does not start a TCP worker. Register task
names with the team's ExecutionEngine, then pass it to `TaskServer(engine)`.

The router skips SUSPECT, DEAD, stale, future-dated, invalid and overloaded
entries. Defaults: CPU <95%, RAM <95%, sample age <=15 seconds. Lowest CPU wins,
then lowest RAM, then lexicographically smallest node ID. The snapshot is read
again before every attempt. Failures exclude that worker for this submission;
`max_retries=2` permits at most three attempts, stopping sooner if peers run out.
Application errors return FAILED without retry. All attempts retain the same
task ID. Worker-local caching coalesces concurrent duplicate requests.

For Member 3: update `NodeInfo.state` on heartbeat events and remove DEAD IDs
with `node.routing.remove(node_id)`. For Member 1: DHTNode subclasses UDPProtocol
and intercepts only DHT message types. TCP adapter functions accept SSL contexts;
message signing/verification should be added at the transport boundary.
For Member 4: TaskClient accepts a replacement asynchronous `send(peer, task)`
function, so the final task channel can replace this TCP adapter.

## Wire protocol

The existing Message envelope is unchanged. DHT_PING avoids conflicting with
the original PING/PONG sequence handling. FIND_NODE, STORE, FIND_VALUE and
DHT_PING carry a random `request_id`; DHT_RESPONSE echoes both the ID and RPC
type. Responses must match the pending request's source address and expected
node ID. This prevents accidental/mismatched completion; it is not cryptographic
authentication. Malformed/oversized control datagrams are ignored.

FIND_NODE requests use `target_id`; replies contain `nodes` contact dictionaries.
STORE uses hashed `key`, JSON `value` and `ttl`; FIND_VALUE replies distinguish
`found: true, value: null` from a missing key. `put`/`get` hash the public string
key internally. Values are limited to 4096 encoded bytes, at most 1024 local
records, with TTL in (0, 86400] seconds. Storage is in memory and unversioned;
concurrent writes do not provide strong consistency or transactions.

## Review boundaries and follow-up integration

- This implements Member 2's modules, not the entire team's final product.
- The current shared executor accepts registered task names and JSON arguments.
  Sending arbitrary functions/closures with cloudpickle remains an executor
  integration task. No claim is made that the handbook's arbitrary-function
  serialization check has passed.
- Routing tests use supplied load snapshots. Live gossip dissemination and
  heartbeat-based interruption of in-flight work belong to Member 3 integration.
  The TCP demo proves retry on connection failure; timeouts also trigger retry.
- TCP execution is offloaded to threads, keeping the event loop responsive.
  A timed-out Python function may still run; shutdown drains active functions.
  Use finite, idempotent tasks. This does not provide exactly-once execution,
  global de-duplication or process isolation.
- TLS contexts are accepted, but this demo has no certificates, peer signature
  verification or replay protection. Run it only on a trusted test network.
- DHT control traffic is IPv4 UDP. No NAT traversal, persistence, automatic
  re-publication of expiring values, or large-scale production claim is made.
- The audits cover ten localhost processes and an additional remote-value
  reader in the single-process socket audit. No Wi-Fi or partition benchmark
  is claimed. Routing tables are bounded; all-to-all membership is only expected
  for these small meshes, not for arbitrary large clusters.
