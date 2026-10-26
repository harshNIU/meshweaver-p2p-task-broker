"""Mark tasks as failed when their executing node is declared dead."""

from tasks.lifecycle import TaskLifecycle, TaskState


def mark_running_tasks_failed(
    lifecycle: TaskLifecycle,
    dead_node_id: str,
) -> list[str]:
    """Fail tasks currently running on a dead node.

    Returns the IDs of tasks successfully transitioned to FAILED.
    Repeated calls are safe because terminal tasks are not transitioned again.
    """
    if not isinstance(dead_node_id, str) or not dead_node_id.strip():
        raise ValueError("dead_node_id must be a non-empty string")

    failed_task_ids: list[str] = []

    for item in lifecycle.snapshot():
        task_id = item["task_id"]
        node_id = item["node"]
        state = item["state"]

        if node_id != dead_node_id or state != TaskState.RUNNING.value:
            continue

        try:
            lifecycle.transition(str(task_id), TaskState.FAILED)
        except ValueError:
            # Another worker may have completed or failed this task after
            # the snapshot was taken. Do not overwrite its terminal state.
            continue

        failed_task_ids.append(str(task_id))

    return failed_task_ids
