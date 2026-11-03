"""`retail-pricing report --run-dir runs/<utc>-sf<SF>`: timings.json and timings.md (PLAN 11).

`build` turns rows (lists of dicts) into the report dict and `render_md` into Markdown; `load`
is the only database access. Sources: steps.json (written by scripts/demo.sh), the audit
database, and stats.jsonl (scripts/sample-stats.sh) when it exists.
"""

import json
import re
from pathlib import Path
from typing import Annotated, Any

import typer
from pydantic import Field

from retail_pricing.settings import _Base

Rows = list[dict[str, Any]]

TPC_NOTICE = "not TPC-DS benchmark results and not comparable to TPC Benchmark results"
TOP_MODELS = 10
_MIB = {"b": 1 / 2**20, "kib": 1 / 1024, "mib": 1.0, "gib": 1024.0}
_MB = {"kb": 1e3 / 2**20, "mb": 1e6 / 2**20, "gb": 1e9 / 2**20}


class GrafanaSettings(_Base):
    """Monitoring arrives in PR 6; without GRAFANA_PORT there is no URL to print."""

    port: int | None = Field(default=None, validation_alias="GRAFANA_PORT")


def grafana_url(port: int | None) -> str | None:
    return None if port is None else f"http://127.0.0.1:{port}"


def parse_steps(text: str) -> list[dict[str, Any]]:
    """steps.json: a list of {"step": str, "ms": int, "ok": bool}; returns step, seconds, ok."""
    doc = json.loads(text)
    if not isinstance(doc, list):
        raise TypeError("steps.json must hold a list of steps")
    out = []
    for s in doc:
        if not isinstance(s, dict) or "step" not in s or "ms" not in s:
            raise ValueError(f"bad step entry: {s!r}")
        out.append(
            {"step": str(s["step"]), "seconds": round(s["ms"] / 1000, 1), "ok": s.get("ok", True)}
        )
    return out


def parse_mem_mib(usage: str) -> float | None:
    """'123.4MiB / 15.6GiB' (docker stats MemUsage) -> 123.4; None when unparseable."""
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", usage)
    if not m:
        return None
    unit = m.group(2).lower()
    factor = _MIB.get(unit, _MB.get(unit))
    return None if factor is None else round(float(m.group(1)) * factor, 1)


def peak_memory(stats_text: str) -> dict[str, float]:
    """Peak MemUsage in MiB per container from stats.jsonl lines."""
    peak: dict[str, float] = {}
    for line in stats_text.splitlines():
        try:
            d = json.loads(line)
            mib = parse_mem_mib(d["MemUsage"])
            name = d["Name"]
        except (ValueError, KeyError, TypeError):
            continue
        if mib is not None and mib > peak.get(name, -1.0):
            peak[name] = mib
    return dict(sorted(peak.items(), key=lambda kv: -kv[1]))


def _rate(amount: float | None, seconds: float | None) -> float | None:
    if amount is None or not seconds or seconds <= 0:
        return None
    return round(amount / seconds, 2)


def build(
    steps: list[dict[str, Any]],
    stages: Rows,
    tables: Rows,
    dbt_nodes: Rows,
    reverse: Rows,
    reconcile: Rows,
    peak_mem: dict[str, float] | None,
    run_dir: str = "",
) -> dict[str, Any]:
    tbl = [
        {
            "cycle_id": t["cycle_id"],
            "stage": t["job_id"],
            "table": t["item"],
            "rows": t["rows"],
            "mb": None if t["bytes"] is None else round(t["bytes"] / 1e6, 2),
            "seconds": round(t["seconds"], 2),
            "rows_per_s": _rate(t["rows"], t["seconds"]),
            "mb_per_s": _rate(None if t["bytes"] is None else t["bytes"] / 1e6, t["seconds"]),
        }
        for t in tables
    ]
    models = sorted(
        (n for n in dbt_nodes if n["unique_id"].startswith("model.")),
        key=lambda n: -(n["execution_time_s"] or 0),
    )[:TOP_MODELS]
    return {
        "run_dir": run_dir,
        "notice": TPC_NOTICE,
        "host_steps": steps,
        "host_total_seconds": round(sum(s["seconds"] for s in steps), 1),
        "stages": [
            {
                "cycle_id": s["cycle_id"],
                "stage": s["job_id"],
                "status": s["status"],
                "seconds": round(s["duration_s"], 2),
            }
            for s in stages
        ],
        "tables": tbl,
        "dbt_top_models": [
            {
                "cycle_id": n["cycle_id"],
                "model": n["unique_id"].rsplit(".", 1)[-1],
                "seconds": round(n["execution_time_s"] or 0, 2),
                "rows": n["rows_affected"],
                "status": n["status"],
            }
            for n in models
        ],
        "reverse": [
            {
                **{k: r[k] for k in ("cycle_id", "target", "batches", "sent", "applied")},
                **{k: r[k] for k in ("unchanged", "conflicts", "rejects")},
                "seconds": None if r["duration_s"] is None else round(r["duration_s"], 2),
                "rows_per_s": _rate(r["sent"], r["duration_s"]),
            }
            for r in reverse
        ],
        "reconcile": [
            {
                "cycle_id": r["cycle_id"],
                "target": r["target"],
                "checks": r["checks"],
                "passed": r["passed"],
            }
            for r in reconcile
        ],
        "peak_memory_mib": peak_mem,
    }


def _table(head: list[str], rows: list[list[Any]]) -> list[str]:
    def cell(v: Any) -> str:
        return "" if v is None else str(v)

    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    out += ["| " + " | ".join(cell(v) for v in r) + " |" for r in rows]
    return out + [""]


def render_md(rep: dict[str, Any], grafana: str | None = None) -> str:
    L = [f"# Run timings {rep['run_dir']}".rstrip(), "", f"_These timings are {TPC_NOTICE}._", ""]
    L += ["## Host steps", ""]
    L += _table(
        ["step", "seconds", "ok"], [[s["step"], s["seconds"], s["ok"]] for s in rep["host_steps"]]
    )
    L += [f"Total: {rep['host_total_seconds']} s", ""]
    L += ["## Stage durations per cycle", ""]
    L += _table(
        ["cycle", "stage", "status", "seconds"],
        [[s["cycle_id"], s["stage"], s["status"], s["seconds"]] for s in rep["stages"]],
    )
    L += ["## Extract and load per table", ""]
    L += _table(
        ["cycle", "stage", "table", "rows", "MB", "seconds", "rows/s", "MB/s"],
        [
            [t["cycle_id"], t["stage"], t["table"], t["rows"], t["mb"], t["seconds"]]
            + [t["rows_per_s"], t["mb_per_s"]]
            for t in rep["tables"]
        ],
    )
    L += [f"## Top {TOP_MODELS} dbt models by time", ""]
    L += _table(
        ["cycle", "model", "seconds", "rows", "status"],
        [
            [n["cycle_id"], n["model"], n["seconds"], n["rows"], n["status"]]
            for n in rep["dbt_top_models"]
        ],
    )
    L += ["## Reverse per target", ""]
    L += _table(
        ["cycle", "target", "batches", "sent", "applied", "unchanged", "conflicts", "rejects"]
        + ["seconds", "rows/s"],
        [
            [r["cycle_id"], r["target"], r["batches"], r["sent"], r["applied"], r["unchanged"]]
            + [r["conflicts"], r["rejects"], r["seconds"], r["rows_per_s"]]
            for r in rep["reverse"]
        ],
    )
    L += ["## Reconcile", ""]
    L += _table(
        ["cycle", "target", "checks", "passed"],
        [[r["cycle_id"], r["target"], r["checks"], r["passed"]] for r in rep["reconcile"]],
    )
    if rep["peak_memory_mib"]:
        L += ["## Peak memory per container", ""]
        L += _table(["container", "MiB"], [[k, v] for k, v in rep["peak_memory_mib"].items()])
    if grafana:
        L += [f"Grafana: {grafana}", ""]
    return "\n".join(L)


def load() -> tuple[Rows, Rows, Rows, Rows, Rows]:
    """stages, extract/load steps, dbt nodes, reverse totals and reconcile from the control DB."""
    import psycopg
    from psycopg.rows import dict_row

    from retail_pricing.settings import ControlSettings

    s = ControlSettings()
    with psycopg.connect(s.conninfo(s.owner_user, s.owner_password), row_factory=dict_row) as c:

        def q(sql: str) -> Rows:
            return c.execute(sql).fetchall()

        stages = q(
            "SELECT cycle_id, job_id, status, duration_s FROM audit.v_cycle_stage_duration"
            " ORDER BY cycle_id, stage_order"
        )
        tables = q(
            "SELECT r.cycle_id, r.job_id, s.item, s.rows, s.bytes,"
            " extract(epoch FROM s.ended_at - s.started_at)::float AS seconds"
            " FROM audit.step s JOIN audit.v_latest_run r USING (run_id)"
            " WHERE r.job_id IN ('extract', 'load') AND s.status = 'succeeded'"
            " AND s.item IS NOT NULL ORDER BY r.cycle_id, r.job_id, s.item"
        )
        nodes = q(
            "SELECT cycle_id, unique_id, status, execution_time_s, rows_affected"
            " FROM audit.dbt_node_result"
        )
        reverse = q(
            "SELECT t.cycle_id, t.target, t.batches, t.sent, t.applied, t.unchanged,"
            " t.conflicts, t.rejects, d.duration_s FROM audit.v_reverse_totals t"
            " LEFT JOIN audit.v_cycle_stage_duration d"
            " ON d.cycle_id = t.cycle_id AND d.job_id = 'reverse.' || t.target"
            " ORDER BY t.cycle_id, t.target"
        )
        reconcile = q(
            "SELECT cycle_id, target, count(*) AS checks, count(*) FILTER (WHERE passed) AS passed"
            " FROM audit.reconcile_result GROUP BY cycle_id, target ORDER BY cycle_id, target"
        )
    return stages, tables, nodes, reverse, reconcile


def write_report(run_dir: Path, rep: dict[str, Any], grafana: str | None) -> None:
    (run_dir / "timings.json").write_text(
        json.dumps(rep, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    (run_dir / "timings.md").write_text(render_md(rep, grafana), encoding="utf-8", newline="\n")


def report_cmd(
    run_dir: Annotated[Path, typer.Option(help="Run folder, e.g. runs/20260101T000000Z-sf1.")],
) -> None:
    """Write timings.json and timings.md from steps.json, the audit DB and stats.jsonl."""
    steps_file, stats_file = run_dir / "steps.json", run_dir / "stats.jsonl"
    if not steps_file.is_file():
        typer.echo(f"{steps_file} not found: run scripts/demo.sh first", err=True)
        raise typer.Exit(1)
    steps = parse_steps(steps_file.read_text(encoding="utf-8"))
    mem = peak_memory(stats_file.read_text(encoding="utf-8")) if stats_file.is_file() else None
    rep = build(steps, *load(), mem, run_dir.name)
    url = grafana_url(GrafanaSettings().port)
    write_report(run_dir, rep, url)
    typer.echo(f"wrote {run_dir / 'timings.json'} and {run_dir / 'timings.md'}")
    if url:
        typer.echo(f"Grafana: {url}")
