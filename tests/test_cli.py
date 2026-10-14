from typer.testing import CliRunner

from cli.main import app

runner = CliRunner()


def test_start_command():
    result = runner.invoke(app, ["start"])

    assert result.exit_code == 0
    assert "Start command is not implemented yet." in result.stdout


def test_submit_command():
    result = runner.invoke(app, ["submit"])

    assert result.exit_code == 0
    assert "Submit command is not implemented yet." in result.stdout
