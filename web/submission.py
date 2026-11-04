"""Authenticated remote task submission for the MeshWeaver dashboard."""

import asyncio
import concurrent.futures
from dataclasses import replace

from execution.models import Task, TaskResult, TaskStatus
from tasks.submit import TaskClient
from tasks.transport import send_task

ALLOWED_TASKS = frozenset({"add", "multiply"})


def create_dashboard_submitter(
    *,
    loop,
    protocol,
    task_registry,
    local_node_id,
    secret,
):
    """Create a synchronous HTTP callback for authenticated remote TCP work."""

    def available_workers():
        """Return live remote peers with known TCP endpoints."""
        registry = getattr(protocol, "node_registry", {}) or {}
        peer_manager = getattr(protocol, "peer_manager", None)

        if peer_manager is None:
            return []

        peers = []

        for peer_id, peer in list(registry.items()):
            peer_id = getattr(peer, "node_id", peer_id)

            if not peer_id or peer_id == local_node_id:
                continue

            is_alive = getattr(peer_manager, "is_peer_alive", None)

            if callable(is_alive):
                if not is_alive(peer_id):
                    continue
            elif getattr(peer, "state", None) != "ALIVE":
                continue

            address = peer_manager.get_peer_tcp_address(peer_id)

            if not address or len(address) < 2:
                continue

            tcp_host, tcp_port = address

            if (
                not isinstance(tcp_host, str)
                or not tcp_host
                or not isinstance(tcp_port, int)
                or isinstance(tcp_port, bool)
                or not 1 <= tcp_port <= 65535
            ):
                continue

            try:
                remote_peer = replace(
                    peer,
                    host=tcp_host,
                    port=tcp_port,
                )
            except (TypeError, ValueError):
                continue

            peers.append(remote_peer)

        return peers

    def submit_demo(payload):
        """Submit an allowlisted task to an authenticated remote worker."""
        if not isinstance(payload, dict):
            raise TypeError("The submission payload must be a JSON object.")

        task_name = payload.get("name")
        args = payload.get("args", [])
        kwargs = payload.get("kwargs", {})

        if not isinstance(task_name, str) or task_name not in ALLOWED_TASKS:
            raise ValueError("Only add and multiply are allowed.")

        if not isinstance(args, list):
            raise TypeError("'args' must be a JSON array.")

        if not isinstance(kwargs, dict):
            raise TypeError("'kwargs' must be a JSON object.")

        if not all(isinstance(key, str) for key in kwargs):
            raise TypeError("'kwargs' keys must be strings.")

        task = Task(
            task_name,
            args=tuple(args),
            kwargs=kwargs,
            max_retries=2,
            timeout_seconds=5.0,
        )

        task_registry.mark_pending(task)

        responding_node = None

        async def authenticated_send(peer, submitted_task):
            nonlocal responding_node

            result = await send_task(
                peer,
                submitted_task,
                secret=secret,
                sender_id=local_node_id,
            )

            # Only record a worker after it has returned a valid response.
            responding_node = peer.node_id
            return result

        client = TaskClient(
            snapshot=available_workers,
            send=authenticated_send,
            timeout=5.0,
        )

        future = asyncio.run_coroutine_threadsafe(
            client.submit(task),
            loop,
        )

        try:
            result = future.result(timeout=18.0)

        except concurrent.futures.TimeoutError as exc:
            future.cancel()

            failed_result = TaskResult(
                task.task_id,
                TaskStatus.FAILED,
                error="Timed out waiting for the remote worker.",
            )
            task_registry.mark_result(task, failed_result)

            raise RuntimeError("Remote task submission timed out.") from exc

        except Exception:
            future.cancel()
            raise

        if responding_node is None:
            task_registry.mark_result(task, result)
            raise RuntimeError(
                result.error or "No authenticated remote worker responded."
            )

        task_registry.mark_assigned(task, responding_node)
        task_registry.mark_result(task, result)

        return {
            "task_id": task.task_id,
            "state": result.status.value.upper(),
            "node": responding_node,
            "attempts": result.attempts,
        }

    return submit_demo
