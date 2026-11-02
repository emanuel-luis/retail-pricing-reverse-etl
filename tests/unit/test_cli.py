from typer.testing import CliRunner

from retail_pricing import __version__
from retail_pricing.cli import app


def test_version_prints_the_package_version() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_data_commands_are_registered() -> None:
    result = CliRunner().invoke(app, ["data", "--help"])
    assert result.exit_code == 0
    for command in ("generate", "build", "all"):
        assert command in result.stdout
