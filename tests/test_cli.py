import asyncio

from typer.testing import CliRunner

from cli.main import app
from net.messages import TASK_REQUEST, TASK_RESULT, Message

runner = CliRunner()


class FakeTransport:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeSocket:
    def getsockname(self):
        return ("127.0.0.1", 9002)


class FakeTCPServer:
    def __init__(self):
        self.sockets = [FakeSocket()]
        self.closed = False
        self.waited = False

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.waited = True


class FakeProtocol:
    def __init__(self, response=None):
        self.transport = None
        self.received_envelopes = []
        self.sent_messages = []
        self.response = response
        self.tcp_endpoint = None

    def set_tcp_endpoint(self, host, port):
        self.tcp_endpoint = (host, port)

    def send_message(self, message, address):
        self.sent_messages.append((message, address))

        if self.response is not None:
            request = message

            async def deliver_response():
                await asyncio.sleep(0.02)
                result = Message(
                    type=TASK_RESULT,
                    sender_id="node-server",
                    seq=request.seq,
                    payload={
                        "task_id": request.payload["task"]["task_id"],
                        **self.response,
                    },
                )
                self.received_envelopes.append((result, address))

            asyncio.get_running_loop().create_task(deliver_response())


def install_fake_node(monkeypatch, protocol, transport):
    calls = {}

    async def fake_start_udp_node(host, port, node_id, execution_engine=None):
        calls.update(
            host=host,
            port=port,
            node_id=node_id,
            execution_engine=execution_engine,
        )
        return transport, protocol

    monkeypatch.setattr("cli.main.start_udp_node", fake_start_udp_node)
    return calls


def test_start_command(monkeypatch):
    calls = {}
    transport = FakeTransport()
    protocol = FakeProtocol()
    tcp_server = FakeTCPServer()

    async def fake_start_udp_node(host, port, node_id, execution_engine=None):
        calls.update(
            host=host,
            port=port,
            node_id=node_id,
            execution_engine=execution_engine,
        )
        return transport, protocol

    async def fake_start_tcp_server(
        host="127.0.0.1",
        port=0,
        execution_engine=None,
    ):
        calls["tcp_host"] = host
        calls["tcp_port_requested"] = port
        calls["tcp_execution_engine"] = execution_engine
        return tcp_server, []

    async def finish_wait(self):
        return None

    monkeypatch.setattr("cli.main.start_udp_node", fake_start_udp_node)
    monkeypatch.setattr("cli.main.start_tcp_server", fake_start_tcp_server)
    monkeypatch.setattr("cli.main.asyncio.Event.wait", finish_wait)

    result = runner.invoke(app, ["start", "--port", "8123"])

    assert result.exit_code == 0, result.output
    assert calls["host"] == "127.0.0.1"
    assert calls["port"] == 8123
    assert calls["node_id"] == "node-8123"
    assert calls["execution_engine"] is not None

    assert calls["tcp_host"] == "127.0.0.1"
    assert calls["tcp_port_requested"] == 0
    assert calls["tcp_execution_engine"] is calls["execution_engine"]

    assert protocol.tcp_endpoint == ("127.0.0.1", 9002)
    assert transport.closed
    assert tcp_server.closed
    assert tcp_server.waited


def test_start_rejects_invalid_port():
    result = runner.invoke(app, ["start", "--port", "70000"])

    assert result.exit_code == 1
    assert "port must be between 1 and 65535" in result.stdout


def test_submit_sends_task_and_displays_result(monkeypatch):
    transport = FakeTransport()
    protocol = FakeProtocol(
        response={
            "status": "completed",
            "value": 5,
            "attempts": 1,
            "error": None,
            "duration_seconds": 0.001,
        }
    )
    calls = install_fake_node(monkeypatch, protocol, transport)

    result = runner.invoke(
        app,
        [
            "submit",
            "add",
            "--args",
            "[2, 3]",
            "--kwargs",
            '{"round": true}',
            "--host",
            "127.0.0.1",
            "--port",
            "8123",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Submitted task 'add'" in result.output
    assert "Status: completed" in result.output
    assert "Result: 5" in result.output
    assert len(protocol.sent_messages) == 1

    request, address = protocol.sent_messages[0]
    assert request.type == TASK_REQUEST
    assert request.payload["task"]["task_name"] == "add"
    assert request.payload["task"]["args"] == [2, 3]
    assert request.payload["task"]["kwargs"] == {"round": True}
    assert address == ("127.0.0.1", 8123)
    assert calls["port"] == 0
    assert transport.closed


def test_submit_reports_remote_execution_failure(monkeypatch):
    transport = FakeTransport()
    protocol = FakeProtocol(
        response={
            "status": "failed",
            "value": None,
            "attempts": 0,
            "error": "Unknown task: missing",
            "duration_seconds": 0.0,
        }
    )
    install_fake_node(monkeypatch, protocol, transport)

    result = runner.invoke(app, ["submit", "missing"])

    assert result.exit_code == 1
    assert "Status: failed" in result.output
    assert "Unknown task: missing" in result.output
    assert transport.closed


def test_submit_rejects_non_array_args():
    result = runner.invoke(app, ["submit", "add", "--args", '{"x": 1}'])

    assert result.exit_code == 1
    assert "--args must be a JSON array" in result.output


def test_submit_rejects_non_object_kwargs():
    result = runner.invoke(app, ["submit", "add", "--kwargs", "[1, 2]"])

    assert result.exit_code == 1
    assert "--kwargs must be a JSON object" in result.output


def test_submit_rejects_invalid_port():
    result = runner.invoke(app, ["submit", "add", "--port", "70000"])

    assert result.exit_code == 1
    assert "--port must be between 1 and 65535" in result.output


def test_submit_rejects_invalid_timeout():
    result = runner.invoke(app, ["submit", "add", "--timeout", "0"])

    assert result.exit_code == 1
    assert "--timeout must be a finite positive number" in result.output


def test_submit_times_out(monkeypatch):
    transport = FakeTransport()
    protocol = FakeProtocol(response=None)
    install_fake_node(monkeypatch, protocol, transport)

    result = runner.invoke(
        app,
        ["submit", "add", "--timeout", "0.05"],
    )

    assert result.exit_code == 1
    assert "Timed out" in result.output
    assert transport.closed
