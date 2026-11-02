"""`retail-pricing workload ...` commands. Implementations are imported inside each command."""

from typing import Annotated

import typer

app = typer.Typer(no_args_is_help=True, help="Concurrent OLTP workload and scripted edits.")


@app.command("run")
def run_cmd(
    duration: Annotated[
        float | None,
        typer.Option(help="Stop after this many seconds (default: until SIGTERM or SIGINT)."),
    ] = None,
) -> None:
    """Sales, returns, receipts, costs, manual price edits and label printing (WORKLOAD_*)."""
    from retail_pricing.settings import OltpSettings, WorkloadSettings
    from retail_pricing.workload.run import run

    summary = run(WorkloadSettings(), OltpSettings(), duration)
    raise typer.Exit(1 if summary["errors"] else 0)


@app.command("edit")
def edit_cmd(
    count: Annotated[int, typer.Option("--count", help="Zone prices to edit.")],
    kvi: Annotated[bool, typer.Option("--kvi", help="Edit KVI products first.")] = False,
    seed: Annotated[int, typer.Option("--seed", help="Seed of the edit choices.")] = 7,
) -> None:
    """Scripted manual zone price edits, e.g. to force apply conflicts in a demo."""
    from retail_pricing.settings import OltpSettings
    from retail_pricing.workload.run import edit

    typer.echo(f"edited {edit(count, kvi, seed, OltpSettings())} zone prices")
