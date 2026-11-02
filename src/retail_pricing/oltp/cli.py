"""`retail-pricing oltp ...` commands. Implementations are imported inside each command."""

from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(no_args_is_help=True, help="OLTP Postgres: create, load and inspect.")


@app.command("load")
def load_cmd(
    sf: Annotated[float, typer.Option("--sf", help="Scale factor of the seed to load.")],
    data_dir: Annotated[
        Path | None, typer.Option("--data-dir", help="Override DATA_DIR (default: data).")
    ] = None,
    replace: Annotated[
        bool, typer.Option("--replace", help="Drop the OLTP schemas first.")
    ] = False,
) -> None:
    """Create the schema from the contract, COPY the seed, then constraints, triggers, roles."""
    from retail_pricing.oltp.load import LoadError, load
    from retail_pricing.settings import DataSettings, OltpSettings

    try:
        rows = load(sf, data_dir or DataSettings().dir, OltpSettings(), replace=replace)
    except LoadError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    typer.echo(f"loaded {rows} rows")


@app.command("status")
def status_cmd() -> None:
    """Row count per OLTP table ('-' when the table does not exist)."""
    from retail_pricing.oltp.load import status
    from retail_pricing.settings import OltpSettings

    for table, rows in status(OltpSettings()).items():
        typer.echo(f"{table:40} {'-' if rows is None else rows}")
