"""Build a live dashboard snapshot from actual MeshWeaver runtime state."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any


def _iso_timestamp(value: float | int | None) -> str | None:
    """Convert a Unix timestamp to an ISO-8601 UTC timestamp."""
    if value is None:
        return None

    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except (OverflowError, OSError, TypeError, ValueError):
        return None


def _resource_usage() -> tuple[float, float]:
    """Return local CPU and RAM usage percentages when psutil is available."""
    try:
        import psutil

        cpu = float(psutil.cpu_percent(interval=None))
        ram = float(psutil.virtual_memory().percent)
        return round(cpu, 1), round(ram, 1)
    except (ImportError, OSError, RuntimeError, AttributeError, ValueError):
        return 0.0, 0.0


def _normalise_task(task: dict[str, Any]) -> dict[str, Any]:
    """Keep task data compatible with the dashboard's expected field names."""
    item = dict(task)

    item["state"] = str(item.get("state", item.get("status", "PENDING"))).upper()

    item["node"] = item.get("node", item.get("assigned_to"))
    item["attempts"] = item.get("attempts", item.get("attempt", 0))

    return item


def build_snapshot(
    *,
    protocol: Any,
    transport: Any,
    node_id: str,
    host: str,
    port: int,
    task_registry: Any = None,
) -> dict[str, Any]:
    """Create a JSON-serialisable snapshot from the running UDP node."""
    now = time.time()
    now_monotonic = time.monotonic()

    cpu, ram = _resource_usage()

    # The UDP protocol maintains the actual node registry.
    registry = getattr(protocol, "node_registry", {}) or {}
    nodes_by_id: dict[str, dict[str, Any]] = {}

    peer_manager = getattr(protocol, "peer_manager", None)
    active_peers = getattr(peer_manager, "peers", {}) or {}
    timeout = float(getattr(peer_manager, "timeout_seconds", 10.0))

    # Include registered nodes, preserving their real addresses and timestamps.
    for registered_id, info in list(registry.items()):
        info_id = str(getattr(info, "node_id", registered_id))
        last_seen = getattr(info, "last_seen", None)

        try:
            last_seen_value = float(last_seen) if last_seen is not None else 0.0
        except (TypeError, ValueError):
            last_seen_value = 0.0

        if info_id == node_id:
            state = "ALIVE"
        else:
            # Peer activity is monotonic time; NodeInfo.last_seen is wall time.
            peer_activity = active_peers.get(info_id)

            if peer_activity is not None:
                age = max(0.0, now_monotonic - float(peer_activity))
            elif last_seen_value > 0:
                age = max(0.0, now - last_seen_value)
            else:
                age = timeout + 1.0

            if age < timeout * 0.5:
                state = "ALIVE"
            elif age < timeout:
                state = "SUSPECT"
            else:
                state = "DEAD"

        nodes_by_id[info_id] = {
            "node_id": info_id,
            "host": str(getattr(info, "host", host)),
            "port": int(getattr(info, "port", port)),
            "cpu": (
                cpu if info_id == node_id else float(getattr(info, "cpu", 0.0) or 0.0)
            ),
            "ram": (
                ram if info_id == node_id else float(getattr(info, "ram", 0.0) or 0.0)
            ),
            "state": state,
            "last_seen": (
                _iso_timestamp(last_seen_value) if last_seen_value > 0 else None
            ),
        }

    # Ensure the local node is represented even before registry initialisation.
    if node_id not in nodes_by_id:
        local_host = host
        local_port = port

        if transport is not None:
            try:
                address = transport.get_extra_info("sockname")
                if address and len(address) >= 2:
                    local_host = str(address[0])
                    local_port = int(address[1])
            except (AttributeError, OSError, TypeError, ValueError):
                pass

        nodes_by_id[node_id] = {
            "node_id": node_id,
            "host": local_host,
            "port": local_port,
            "cpu": cpu,
            "ram": ram,
            "state": "ALIVE",
            "last_seen": _iso_timestamp(now),
        }
    else:
        # Refresh local resource measurements without changing the real address.
        nodes_by_id[node_id]["cpu"] = cpu
        nodes_by_id[node_id]["ram"] = ram
        nodes_by_id[node_id]["state"] = "ALIVE"

    tasks: list[dict[str, Any]] = []
    events: list[dict[str, str]] = []

    if task_registry is not None:
        snapshot_tasks = getattr(task_registry, "snapshot_tasks", None)
        if callable(snapshot_tasks):
            try:
                tasks = [
                    _normalise_task(task)
                    for task in snapshot_tasks()
                    if isinstance(task, dict)
                ]
            except (RuntimeError, TypeError, ValueError):
                tasks = []

        snapshot_events = getattr(task_registry, "snapshot_events", None)
        if callable(snapshot_events):
            try:
                for event in snapshot_events():
                    if not isinstance(event, dict):
                        continue

                    events.append(
                        {
                            "timestamp": str(
                                event.get("timestamp", _iso_timestamp(now))
                            ),
                            "level": str(event.get("level", "INFO")),
                            "message": str(event.get("message", "")),
                        }
                    )
            except (RuntimeError, TypeError, ValueError):
                pass

    # Include actual messages received by the UDP protocol in the event log.
    received = getattr(protocol, "received_envelopes", []) or []
    for message, address in list(received)[-50:]:
        sender_id = str(getattr(message, "sender_id", "unknown"))
        message_type = str(getattr(message, "type", "MESSAGE"))

        events.append(
            {
                "timestamp": _iso_timestamp(now) or "",
                "level": "INFO",
                "message": (
                    f"Received {message_type} from {sender_id} "
                    f"({address[0]}:{address[1]})"
                    if isinstance(address, (tuple, list)) and len(address) >= 2
                    else f"Received {message_type} from {sender_id}"
                ),
            }
        )

    events = events[-100:]

    return {
        "snapshot_at": _iso_timestamp(now),
        "mode": "live",
        "self": node_id,
        "nodes": list(nodes_by_id.values()),
        "tasks": tasks,
        "events": events,
    }
