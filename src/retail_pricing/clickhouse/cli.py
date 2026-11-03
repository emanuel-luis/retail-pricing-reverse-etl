"""`retail-pricing ch ...` commands. Implementations are imported inside each command."""

from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(no_args_is_help=True, help="ClickHouse: raw tables and the dev raw load.")


@app.command("init")
def init_command() -> None:
    """Create the databases and the raw tables (safe to run twice)."""
    from retail_pricing.clickhouse.init import init
    from retail_pricing.settings import ClickHouseSettings

    init(ClickHouseSettings())


@app.command("load-raw")
def load_raw_command(
    sf: Annotated[float, typer.Option("--sf", help="Scale factor of the seed to load.")],
    cycle: Annotated[int, typer.Option("--cycle", help="Value of _cycle_id for these rows.")],
    data_dir: Annotated[
        Path | None, typer.Option("--data-dir", help="Override DATA_DIR (manifest lookup only).")
    ] = None,
) -> None:
    """Load the OLTP seed Parquet from ./data (mounted into the container) as one cycle."""
    from retail_pricing.clickhouse.load_raw import LoadRawError, load_raw
    from retail_pricing.settings import ClickHouseSettings, DataSettings

    try:
        rows = load_raw(sf, cycle, data_dir or DataSettings().dir, ClickHouseSettings())
    except LoadRawError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    typer.echo(f"loaded {rows} rows as cycle {cycle}")
