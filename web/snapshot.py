"""Build dashboard snapshots from the running MeshWeaver process."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any


def _utc_timestamp(epoch_seconds: float | None = None) -> str:
    if epoch_seconds is None:
        epoch_seconds = time.time()
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()


def build_snapshot(
    *,
    protocol: Any,
    transport: Any,
    node_id: str,
    host: str,
    port: int,
    task_registry: Any = None,
) -> dict[str, Any]:
    """Return a snapshot from the live UDP protocol and task registry."""
    import psutil

    cpu = psutil.cpu_percent(interval=0.1)
    ram = psutil.virtual_memory().percent
    now_monotonic = time.monotonic()
    now_epoch = time.time()

    nodes: list[dict[str, Any]] = [{
        "node_id": node_id,
        "host": host,
        "port": port,
        "cpu": cpu,
        "ram": ram,
        "last_seen": _utc_timestamp(now_epoch),
        "state": "ALIVE",
    }]

    peer_addresses: dict[str, tuple[str, int]] = {}
    for message, address in list(getattr(protocol, "received_envelopes", [])):
        if isinstance(address, tuple) and len(address) >= 2:
            peer_addresses[message.sender_id] = (str(address[0]), int(address[1]))

    peer_manager = getattr(protocol, "peer_manager", None)
    peer_times = dict(getattr(peer_manager, "peers", {}))
    for peer_id, address in peer_addresses.items():
        last_seen_monotonic = peer_times.get(peer_id)
        if last_seen_monotonic is None:
            state, last_seen = "SUSPECT", None
        else:
            age = max(0.0, now_monotonic - last_seen_monotonic)
            timeout = float(getattr(peer_manager, "timeout_seconds", 10.0))
            state = "ALIVE" if age < timeout else "SUSPECT"
            last_seen = _utc_timestamp(now_epoch - age)
        nodes.append({
            "node_id": peer_id,
            "host": address[0],
            "port": address[1],
            "cpu": None,
            "ram": None,
            "last_seen": last_seen,
            "state": state,
        })

    events = []
    for message, _address in list(getattr(protocol, "received_envelopes", []))[-50:]:
        events.append({
            "timestamp": _utc_timestamp(getattr(message, "ts", 0) or None),
            "level": "INFO",
            "message": f"Received {message.type} from {message.sender_id} (seq={message.seq})",
        })

    tasks = []
    if task_registry is not None:
        tasks = task_registry.snapshot_tasks()
        events.extend(task_registry.snapshot_events())
    events = events[-100:]

    return {
        "snapshot_at": _utc_timestamp(),
        "mode": "live",
        "nodes": nodes,
        "tasks": tasks,
        "events": events,
    }
