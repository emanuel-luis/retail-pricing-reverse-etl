"""Typer entry point. Each command imports its implementation when it runs."""

from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

from retail_pricing import __version__

if TYPE_CHECKING:
    from retail_pricing.settings import DataSettings

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="Retail pricing ELT and reverse ETL: data, OLTP, NiFi, ClickHouse and dbt.",
)
data_app = typer.Typer(no_args_is_help=True, help="Generate TPC-DS data and build the OLTP seed.")
app.add_typer(data_app, name="data")


def _register_subcommands() -> None:
    """Sub-apps live next to their implementations; their modules import nothing heavy."""
    from retail_pricing.clickhouse.cli import app as ch_app
    from retail_pricing.control.cli import app as control_app
    from retail_pricing.dbt_runner.cli import app as dbt_runner_app
    from retail_pricing.nifi.cli import app as nifi_app
    from retail_pricing.oltp.cli import app as oltp_app
    from retail_pricing.s3.cli import app as s3_app
    from retail_pricing.workload.cli import app as workload_app

    app.add_typer(oltp_app, name="oltp")
    app.add_typer(ch_app, name="ch")
    app.add_typer(s3_app, name="s3")
    app.add_typer(control_app, name="control")
    app.add_typer(workload_app, name="workload")
    app.add_typer(dbt_runner_app, name="dbt-runner")
    app.add_typer(nifi_app, name="nifi")


_register_subcommands()


@app.callback()
def main(
    log_level: Annotated[str, typer.Option(help="Log level for the JSON logs on stderr.")] = "INFO",
) -> None:
    """Configure JSON logging for every command."""
    from retail_pricing.logging import configure_logging

    configure_logging(log_level.upper())


SfOption = Annotated[float, typer.Option("--sf", help="TPC-DS scale factor, e.g. 0.01, 1 or 10.")]
DataDirOption = Annotated[
    Path | None, typer.Option("--data-dir", help="Override DATA_DIR (default: data).")
]


def _data_settings(data_dir: Path | None) -> "DataSettings":
    from retail_pricing.settings import DataSettings

    settings = DataSettings()
    return settings if data_dir is None else settings.model_copy(update={"dir": data_dir})


@data_app.command("generate")
def data_generate(sf: SfOption, data_dir: DataDirOption = None) -> None:
    """Generate TPC-DS with DuckDB and copy the needed tables to Parquet."""
    from retail_pricing.data.generate import generate

    typer.echo(generate(sf, _data_settings(data_dir)))


@data_app.command("build")
def data_build(sf: SfOption, data_dir: DataDirOption = None) -> None:
    """Build the OLTP-shaped seed Parquet from the generated TPC-DS Parquet."""
    from retail_pricing.data.build import build

    typer.echo(build(sf, _data_settings(data_dir)))


@data_app.command("all")
def data_all(sf: SfOption, data_dir: DataDirOption = None) -> None:
    """Generate (skipped when the manifest matches), then build."""
    from retail_pricing.data.build import build
    from retail_pricing.data.generate import generate

    settings = _data_settings(data_dir)
    generate(sf, settings)
    typer.echo(build(sf, settings))


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@app.command()
def health(
    url: Annotated[str, typer.Option(help="HTTP URL that must answer 200 within the timeout.")],
    timeout: Annotated[float, typer.Option(help="Seconds before giving up.")] = 3.0,
) -> None:
    """Exit 0 when URL answers HTTP 200 (container healthchecks; no curl in the image)."""
    import urllib.request

    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            ok = resp.status == 200
    except OSError:
        ok = False
    raise typer.Exit(0 if ok else 1)
