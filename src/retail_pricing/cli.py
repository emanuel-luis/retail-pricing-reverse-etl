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
