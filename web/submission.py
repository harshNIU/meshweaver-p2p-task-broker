
"""Authenticated remote task submission for the MeshWeaver dashboard."""

import asyncio
import concurrent.futures
from dataclasses import replace

from execution.models import Task, TaskResult, TaskStatus
from tasks.lifecycle import TaskState
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
    recovery_coordinator=None,
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
                or not tcp_host.strip()
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

        active_node = None
        registered = False
        received_valid_result = False
        attempt_count = 0

        async def authenticated_send(peer, submitted_task):
            nonlocal active_node
            nonlocal registered
            nonlocal received_valid_result
            nonlocal attempt_count

            peer_id = peer.node_id
            attempt_count += 1

            if recovery_coordinator is not None:
                lifecycle = recovery_coordinator.lifecycle

                if not registered:
                    recovery_coordinator.register(submitted_task, peer_id)
                    registered = True

                elif active_node != peer_id:
                    lifecycle.reassign_running(
                        submitted_task.task_id,
                        active_node,
                        peer_id,
                    )
                    lifecycle.transition(
                        submitted_task.task_id,
                        TaskState.RUNNING,
                    )

            active_node = peer_id

            result = await send_task(
                peer,
                submitted_task,
                secret=secret,
                sender_id=local_node_id,
            )

            if (
                isinstance(result, TaskResult)
                and result.task_id == submitted_task.task_id
                and result.status in (
                    TaskStatus.COMPLETED,
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                )
            ):
                received_valid_result = True

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

            error_message = (
                "Timed out waiting for the remote worker; "
                "execution may have occurred."
            )

            if active_node is not None:
                task_registry.mark_assigned(task, active_node)
                task_registry.mark_outcome_uncertain(task, error_message)

                return {
                    "task_id": task.task_id,
                    "state": "OUTCOME_UNCERTAIN",
                    "node": active_node,
                    "attempts": attempt_count,
                    "error": error_message,
                }

            failed_result = TaskResult(
                task.task_id,
                TaskStatus.FAILED,
                error="Timed out before contacting a remote worker.",
            )
            task_registry.mark_result(task, failed_result)

            raise RuntimeError(
                "Remote task submission timed out before contacting a worker."
            ) from exc

        except Exception:
            future.cancel()
            raise

        if active_node is None:
            task_registry.mark_result(task, result)
            raise RuntimeError(
                result.error or "No authenticated remote worker responded."
            )

        if not received_valid_result:
            error_message = (
                result.error
                or "No authenticated result was received; "
                "remote execution may have occurred."
            )

            task_registry.mark_assigned(task, active_node)
            task_registry.mark_outcome_uncertain(task, error_message)

            return {
                "task_id": task.task_id,
                "state": "OUTCOME_UNCERTAIN",
                "node": active_node,
                "attempts": result.attempts or attempt_count,
                "error": error_message,
            }

        if recovery_coordinator is not None and registered:
            lifecycle = recovery_coordinator.lifecycle
            record = lifecycle.get(task.task_id)

            if record is not None and record.state == TaskState.RUNNING:
                if result.status == TaskStatus.COMPLETED:
                    lifecycle.transition(task.task_id, TaskState.DONE)
                elif result.status in (
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                ):
                    lifecycle.transition(task.task_id, TaskState.FAILED)

        task_registry.mark_assigned(task, active_node)
        task_registry.mark_result(task, result)

        return {
            "task_id": task.task_id,
            "state": result.status.value.upper(),
            "node": active_node,
            "attempts": result.attempts,
        }

    return submit_demo
