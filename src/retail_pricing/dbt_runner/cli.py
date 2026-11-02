"""`retail-pricing dbt-runner ...` commands."""

import typer

app = typer.Typer(no_args_is_help=True, help="HTTP service that runs dbt build for NiFi.")


@app.command("serve")
def serve_cmd() -> None:
    """Listen on DBT_RUNNER_HOST:DBT_RUNNER_PORT until SIGTERM or SIGINT."""
    from retail_pricing.dbt_runner.server import serve
    from retail_pricing.settings import DbtRunnerSettings

    serve(DbtRunnerSettings())
