"""`retail-pricing cycle ...`: trigger a cycle through NiFi and follow it in the audit DB (PLAN 11).

Exit codes of `trigger --wait` and `wait`: 0 succeeded, 1 failed (or refused), 2 timeout.
"""

import time
from collections.abc import Callable
from typing import Annotated, Any

import typer

app = typer.Typer(no_args_is_help=True, help="Trigger and follow pricing cycles.")

POLL_S = 5.0
SUCCEEDED, FAILED, TIMEOUT = 0, 1, 2

Snapshot = dict[str, Any]


def post_trigger(mode: str, rerun: int | None, timeout_s: float = 90.0) -> tuple[int, Any]:
    """POST /cycles to the NiFi trigger; returns (HTTP status, JSON body)."""
    import httpx

    from retail_pricing.settings import NifiSettings

    s = NifiSettings()
    resp = httpx.post(
        f"http://{s.host}:{s.trigger_port}/cycles",
        json={"mode": mode, "rerun_cycle_id": rerun},
        timeout=timeout_s,
    )
    try:
        body = resp.json()
    except ValueError:
        body = {"error": resp.text}
    return resp.status_code, body


def snapshot(cycle_id: int | None) -> Snapshot | None:
    """The cycle (latest when None), its stage runs of the current attempt and failed steps."""
    import psycopg
    from psycopg.rows import dict_row

    from retail_pricing.settings import ControlSettings

    s = ControlSettings()
    with psycopg.connect(s.conninfo(s.owner_user, s.owner_password), row_factory=dict_row) as c:
        cycle = c.execute(
            "SELECT cycle_id, mode, status, failed_stage, error, attempt, started_at, ended_at"
            " FROM audit.cycle WHERE cycle_id = coalesce(%s, (SELECT max(cycle_id) FROM"
            " audit.cycle))",
            (cycle_id,),
        ).fetchone()
        if cycle is None:
            return None
        runs = c.execute(
            "SELECT job_id, status, rows_out, extract(epoch FROM coalesce(ended_at, now())"
            " - started_at)::float AS seconds FROM audit.job_run"
            " WHERE cycle_id = %s AND started_at >= %s ORDER BY started_at",
            (cycle["cycle_id"], cycle["started_at"]),
        ).fetchall()
        failed = c.execute(
            "SELECT r.job_id, s.item, s.error FROM audit.step s JOIN audit.job_run r USING"
            " (run_id) WHERE r.cycle_id = %s AND r.started_at >= %s AND s.status = 'failed'"
            " ORDER BY s.started_at",
            (cycle["cycle_id"], cycle["started_at"]),
        ).fetchall()
    return {**cycle, "runs": runs, "failed_steps": failed}


def _runs_line(snap: Snapshot) -> str:
    parts = [
        f"{r['job_id']} {r['status']} {r['seconds']:.0f}s"
        + (f" {r['rows_out']} rows" if r["rows_out"] is not None else "")
        for r in snap["runs"]
    ]
    return f"cycle {snap['cycle_id']} {snap['status']}: " + (", ".join(parts) or "starting")


def wait_cycle(
    cycle_id: int,
    timeout_s: float,
    fetch: Callable[[int], Snapshot | None] = snapshot,
    echo: Callable[[str], None] = typer.echo,
    every_s: float = POLL_S,
) -> int:
    """Poll until the cycle ends, printing each change of its stage runs; returns the exit code."""
    deadline = time.monotonic() + timeout_s
    last = ""
    while True:
        snap = fetch(cycle_id)
        if snap is not None:
            line = _runs_line(snap)
            if line != last:
                echo(line)
                last = line
            if snap["status"] == "succeeded":
                return SUCCEEDED
            if snap["status"] == "failed":
                echo(f"failed stage {snap['failed_stage']}: {snap['error']}")
                for f in snap["failed_steps"]:
                    echo(f"  {f['job_id']} {f['item']}: {f['error']}")
                return FAILED
        if time.monotonic() >= deadline:
            echo(f"cycle {cycle_id} still running after {timeout_s:.0f} s")
            return TIMEOUT
        time.sleep(every_s)


def wait_stage(
    cycle_id: int | None,
    job: str,
    timeout_s: float,
    fetch: Callable[[int | None], Snapshot | None] = snapshot,
    every_s: float = 1.0,
) -> int:
    """Poll until the cycle's current attempt has started a run of `job`: 0 once it has, 1 when
    the cycle ended without one, 2 on timeout. The previous stages are done by then."""
    deadline = time.monotonic() + timeout_s
    while True:
        snap = fetch(cycle_id)
        if snap is not None:
            if any(r["job_id"] == job for r in snap["runs"]):
                return SUCCEEDED
            if snap["status"] != "running":
                return FAILED
        if time.monotonic() >= deadline:
            return TIMEOUT
        time.sleep(every_s)


TimeoutOption = Annotated[float, typer.Option("--timeout", help="Seconds to wait.")]


@app.command("trigger")
def trigger_cmd(
    mode: Annotated[str, typer.Option(help="full or incremental.")],
    rerun: Annotated[int | None, typer.Option(help="Rerun this failed cycle id.")] = None,
    wait: Annotated[bool, typer.Option("--wait", help="Wait until the cycle ends.")] = False,
    timeout: TimeoutOption = 3600.0,
) -> None:
    """Start a cycle (POST /cycles on the NiFi trigger port)."""
    if mode not in ("full", "incremental"):
        raise typer.BadParameter("mode must be full or incremental")
    status, body = post_trigger(mode, rerun)
    if status == 409:
        typer.echo(f"busy: cycle {body.get('cycle_id')} is running", err=True)
        raise typer.Exit(FAILED)
    if status != 202:
        typer.echo(f"refused (HTTP {status}): {body.get('error', body)}", err=True)
        raise typer.Exit(FAILED)
    cycle_id = int(body["cycle_id"])
    typer.echo(f"cycle {cycle_id} {body['status']}")
    if wait:
        raise typer.Exit(wait_cycle(cycle_id, timeout))


@app.command("wait")
def wait_cmd(
    cycle: Annotated[int, typer.Option(help="Cycle id.")],
    timeout: TimeoutOption = 3600.0,
) -> None:
    """Wait for a running cycle to end."""
    raise typer.Exit(wait_cycle(cycle, timeout))


@app.command("wait-stage")
def wait_stage_cmd(
    job: Annotated[str, typer.Argument(help="Job id of the stage, e.g. transform.")],
    cycle: Annotated[int | None, typer.Option(help="Cycle id (default: the latest).")] = None,
    timeout: TimeoutOption = 3600.0,
) -> None:
    """Wait until a running cycle reaches a stage (its run of the job has started)."""
    code = wait_stage(cycle, job, timeout)
    typer.echo({SUCCEEDED: f"{job} started", FAILED: "cycle ended first"}.get(code, "timeout"))
    raise typer.Exit(code)


@app.command("status")
def status_cmd(
    cycle: Annotated[int | None, typer.Option(help="Cycle id (default: the latest).")] = None,
) -> None:
    """Print a cycle's status and its stage runs."""
    snap = snapshot(cycle)
    if snap is None:
        typer.echo("no cycle yet")
        return
    typer.echo(_runs_line(snap))
    if snap["error"]:
        typer.echo(f"failed stage {snap['failed_stage']}: {snap['error']}")
