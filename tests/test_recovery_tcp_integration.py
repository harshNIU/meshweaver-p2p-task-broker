"""Integration tests for task recovery over real TCP workers."""

import time
import unittest

from execution.engine import ExecutionEngine
from execution.models import Task, TaskResult, TaskStatus
from execution.serializer import task_to_dict
from gossip.state import NodeInfo
from net.messages import TASK_REQUEST, TASK_RESULT, Message
from net.tcp_node import send_tcp_message, start_tcp_server
from tasks.lifecycle import TaskLifecycle, TaskState
from tasks.recovery import TaskRecoveryCoordinator


class TCPRecoveryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.executions = []
        self.dead_engine = ExecutionEngine(max_workers=2)
        self.replacement_engine = ExecutionEngine(max_workers=2)

        self.replacement_engine.register(
            "calculate", lambda a, b: self._calculate(a, b)
        )

        self.dead_server, self.dead_messages = await start_tcp_server(
            execution_engine=self.dead_engine
        )
        self.replacement_server, self.replacement_messages = await start_tcp_server(
            execution_engine=self.replacement_engine
        )

        dead_address = self.dead_server.sockets[0].getsockname()
        replacement_address = self.replacement_server.sockets[0].getsockname()

        self.dead_node = NodeInfo(
            "worker-dead",
            dead_address[0],
            dead_address[1],
            cpu=10,
            ram=10,
            last_seen=time.time(),
        )
        self.replacement_node = NodeInfo(
            "worker-replacement",
            replacement_address[0],
            replacement_address[1],
            cpu=20,
            ram=20,
            last_seen=time.time(),
        )

        self.lifecycle = TaskLifecycle()

        async def submit_over_tcp(node, task):
            request = Message(
                type=TASK_REQUEST,
                sender_id="recovery-coordinator",
                seq=1,
                payload={"task": task_to_dict(task)},
            )
            response = await send_tcp_message(
                node.host, node.port, request, timeout=2.0
            )

            if response.type != TASK_RESULT:
                raise ValueError(f"Expected TASK_RESULT, received {response.type}")

            payload = response.payload
            try:
                return TaskResult(
                    task_id=payload["task_id"],
                    status=TaskStatus(payload["status"]),
                    attempts=payload.get("attempts", 0),
                    value=payload.get("result"),
                    error=payload.get("error"),
                    duration_seconds=payload.get("duration_seconds", 0.0),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("Invalid task result from TCP worker") from exc

        self.coordinator = TaskRecoveryCoordinator(
            self.lifecycle,
            snapshot=lambda: [self.dead_node, self.replacement_node],
            submit=submit_over_tcp,
        )

    def _calculate(self, a, b):
        self.executions.append((a, b))
        return a + b

    async def asyncTearDown(self):
        self.dead_server.close()
        self.replacement_server.close()
        await self.dead_server.wait_closed()
        await self.replacement_server.wait_closed()
        self.dead_engine.shutdown()
        self.replacement_engine.shutdown()

    async def test_recovery_executes_task_on_replacement_worker(self):
        task = Task(
            task_name="calculate",
            args=(20, 22),
            task_id="tcp-recovery-001",
        )
        self.coordinator.register(task, self.dead_node.node_id)

        results = await self.coordinator.recover_node(self.dead_node.node_id)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].task_id, task.task_id)
        self.assertEqual(results[0].status, TaskStatus.COMPLETED)
        self.assertEqual(results[0].value, 42)

        record = self.lifecycle.get(task.task_id)
        self.assertEqual(record.node, self.replacement_node.node_id)
        self.assertEqual(record.state, TaskState.DONE)
        self.assertEqual(self.executions, [(20, 22)])
        self.assertEqual(len(self.replacement_messages), 1)
        self.assertEqual(self.replacement_messages[0].type, TASK_REQUEST)

    async def test_repeated_recovery_does_not_execute_again(self):
        task = Task(
            task_name="calculate",
            args=(3, 4),
            task_id="tcp-recovery-002",
        )
        self.coordinator.register(task, self.dead_node.node_id)

        first = await self.coordinator.recover_node(self.dead_node.node_id)
        second = await self.coordinator.recover_node(self.dead_node.node_id)

        self.assertEqual(len(first), 1)
        self.assertEqual(first[0].value, 7)
        self.assertEqual(second, [])
        self.assertEqual(self.executions, [(3, 4)])
        self.assertEqual(len(self.replacement_messages), 1)


if __name__ == "__main__":
    unittest.main()
