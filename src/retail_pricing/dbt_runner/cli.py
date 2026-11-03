"""`retail-pricing dbt-runner ...` commands."""

import typer

app = typer.Typer(no_args_is_help=True, help="HTTP service that runs dbt build for NiFi.")


@app.command("serve")
def serve_cmd() -> None:
    """Listen on DBT_RUNNER_HOST:DBT_RUNNER_PORT until SIGTERM or SIGINT."""
    from retail_pricing.dbt_runner.server import serve
    from retail_pricing.settings import DbtRunnerSettings

    serve(DbtRunnerSettings())


@app.command("trigger")
def trigger_cmd(
    cycle: int = typer.Option(..., min=1, help="Cycle id passed to dbt as var cycle_id."),
    as_of: str = typer.Option(..., help="Business date YYYY-MM-DD passed as var as_of."),
    select: str | None = typer.Option(None, help="Optional dbt --select tokens."),
    url: str | None = typer.Option(None, help="Runner base URL (default from DBT_RUNNER_*)."),
    timeout: float = typer.Option(1800.0, help="Seconds to wait for the build."),
) -> None:
    """Start `dbt build` for a cycle on the running dbt runner and wait; exit 1 unless it succeeds."""
    import json

    from retail_pricing.dbt_runner.client import RunnerError, trigger_and_wait
    from retail_pricing.settings import DbtRunnerSettings

    s = DbtRunnerSettings()
    base = url or f"http://{s.host}:{s.port}"
    try:
        out = trigger_and_wait(base, cycle, as_of, select, timeout_s=timeout)
    except RunnerError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    job = out.job
    typer.echo(
        json.dumps(
            {k: job.get(k) for k in ("job_id", "cycle_id", "status", "duration_s", "counts")},
            default=str,
        )
    )
    for node in out.failed_nodes:
        typer.echo(f"{node['status']}: {node['unique_id']}: {(node.get('message') or '')[:300]}")
    if job["status"] != "succeeded":
        raise typer.Exit(1)
