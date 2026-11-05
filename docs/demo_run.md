# MeshWeaver Final Demo Run

**Date:** 5 November 2026
**Member:** M1 - Async Networking and Security

## 1. Ten-Node DHT Network Audit

**Command:** `python -m scripts.spawn_nodes --nodes 10 --base-port 9000`

**Result:** PASS

- Independent UDP processes started: 10
- Ports used: 9000-9009
- Peers discovered by each node: 9
- DHT replicas: 10
- Remote value retrieval: Successful
- Audit execution time: 5.736 seconds
- All 10 nodes stopped cleanly.

## 2. Task Execution and Worker Failure Recovery

**Command:** `pytest -v tests/test_smoke_5nodes.py`

**Result:** PASS

- Started 5 independent task worker processes.
- Submitted 20 slow_square tasks.
- Terminated 2 workers while tasks were running.
- Remaining workers handled the tasks through retry and failover.
- All 20 tasks completed successfully.
- All returned results matched the expected square values.
- Latest verification: 1 passed in 4.56 seconds.

## 3. Final Demo Summary

| Test | Result |
|---|---|
| 10-node DHT network audit | PASS |
| All-to-all peer discovery | PASS |
| DHT replication and remote retrieval | PASS |
| 20-task execution | PASS |
| Recovery after 2 worker failures | PASS |

**Overall:** Both demonstration runs passed.

**Note:** The 10-node DHT audit and 5-worker task failover test were separate runs, not one combined 10-worker demonstration.

Non-blocking Python 3.14 deprecation warnings and a pytest configuration warning appeared during testing.
