"""`retail-pricing control ...` commands. Implementations are imported inside each command."""

import typer

app = typer.Typer(no_args_is_help=True, help="Control/audit Postgres: registry and audit.")


@app.command("init")
def init_command() -> None:
    """Create roles, schemas, functions, views and the registry seed (safe to run twice)."""
    from retail_pricing.control.init import init
    from retail_pricing.settings import ControlSettings

    init(ControlSettings())


@app.command("status")
def status_command(
    limit: int = typer.Option(5, help="How many of the latest cycles to show."),
) -> None:
    """Print the latest cycles and the runs of each."""
    from retail_pricing.control.init import status
    from retail_pricing.settings import ControlSettings

    typer.echo("\n".join(status(ControlSettings(), limit)))


@app.command("schedule")
def schedule_command(
    every: str | None = typer.Option(
        None,
        help="Start a cycle when the last one started longer ago than this, e.g. '10 minutes'.",
    ),
    off: bool = typer.Option(
        False, "--off", help="Stop the scheduled cycles (HTTP triggers only)."
    ),
) -> None:
    """Set or clear the cycle schedule in the job registry (NiFi polls it every minute)."""
    from retail_pricing.control.init import set_cycle_schedule
    from retail_pricing.settings import ControlSettings

    if off == (every is not None):
        raise typer.BadParameter("give exactly one of --every or --off")
    shown = set_cycle_schedule(ControlSettings(), None if off else every)
    typer.echo(f"cycle schedule: {shown or 'off'}")
