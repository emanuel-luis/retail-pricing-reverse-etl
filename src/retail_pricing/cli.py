"""Typer entry point. Each command imports its implementation when it runs."""

from typing import Annotated

import typer

from retail_pricing import __version__

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


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)
