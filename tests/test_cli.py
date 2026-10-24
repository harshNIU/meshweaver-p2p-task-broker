from typer.testing import CliRunner

from cli.main import app

runner = CliRunner()


def test_start_command(monkeypatch):
    calls = {}

    class FakeTransport:
        closed = False

        def close(self):
            self.closed = True

    transport = FakeTransport()

    async def fake_start_udp_node(host, port, node_id):
        calls.update(host=host, port=port, node_id=node_id)
        return transport, object()

    async def finish_wait(self):
        return None

    monkeypatch.setattr("cli.main.start_udp_node", fake_start_udp_node)
    monkeypatch.setattr("cli.main.asyncio.Event.wait", finish_wait)

    result = runner.invoke(app, ["start", "--port", "8123"])

    assert result.exit_code == 0
    assert calls == {
        "host": "127.0.0.1",
        "port": 8123,
        "node_id": "node-8123",
    }
    assert transport.closed


def test_start_rejects_invalid_port():
    result = runner.invoke(app, ["start", "--port", "70000"])

    assert result.exit_code == 1
    assert "port must be between 1 and 65535" in result.stdout


def test_submit_command():
    result = runner.invoke(
        app,
        [
            "submit",
            "add",
            "--args",
            "[2, 3]",
            "--kwargs",
            '{"round": true}',
        ],
    )

    assert result.exit_code == 0
    assert "Task request created successfully." in result.stdout
    assert "Task name: add" in result.stdout
    assert '"args": [2, 3]' in result.stdout
    assert "Remote execution is not implemented yet" in result.stdout


def test_submit_rejects_non_array_args():
    result = runner.invoke(app, ["submit", "add", "--args", '{"x": 1}'])

    assert result.exit_code == 1
    assert "--args must be a JSON array" in result.stdout


def test_submit_rejects_non_object_kwargs():
    result = runner.invoke(app, ["submit", "add", "--kwargs", "[1, 2]"])

    assert result.exit_code == 1
    assert "--kwargs must be a JSON object" in result.stdout
