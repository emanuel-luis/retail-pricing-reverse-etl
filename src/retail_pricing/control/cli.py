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
