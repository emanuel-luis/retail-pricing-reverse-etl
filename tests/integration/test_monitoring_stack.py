"""The monitoring profile against the running stack (marker `integration`).

Needs the monitoring profile up next to the core services, the OLTP loaded, `control init` and
`ch init`, and at least one dbt build so the ClickHouse pricing and marts tables exist (CI runs
it after `make demo`). Targets whose compose service is not running are skipped, so the module
also runs against a partial stack. Grafana allows anonymous viewing; the API calls log in as the
admin from `.env`.
"""

import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from dotenv import dotenv_values

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
ENV = {**dotenv_values(ROOT / ".env"), **os.environ}
PROMETHEUS = f"http://127.0.0.1:{ENV.get('PROMETHEUS_PORT', '29090')}"
GRAFANA = f"http://127.0.0.1:{ENV.get('GRAFANA_PORT', '23000')}"
AUTH = (ENV.get("GRAFANA_ADMIN_USER") or "admin", ENV.get("GRAFANA_ADMIN_PASSWORD") or "")
DASHBOARDS = {
    p.stem: json.loads(p.read_text(encoding="utf-8"))
    for p in sorted((ROOT / "config" / "grafana" / "dashboards").glob("*.json"))
}
RULES = {
    "CycleFailed",
    "SlaBreach",
    "ReconcileFailed",
    "DqErrors",
    "ConflictRateHigh",
    "WatermarkLag",
    "TargetDown",
    "NiFiBackpressure",
    "ClickHouseMemoryHigh",
}
# Scrape job of each target's compose service; metric name prefix of each job.
SERVICE = {"postgres-oltp": "pg-exporter-oltp", "postgres-control": "pg-exporter-control"}
PREFIX = {
    "nifi_": "nifi",
    "ClickHouse": "clickhouse",
    "pg_": "postgres-oltp",
    "workload_": "workload",
    "dbt_runner_": "dbt-runner",
}
# Counters prometheus_client only exports once a labelled child exists (the first error).
LAZY = {"workload_errors_total"}


def running_services() -> set[str]:
    out = subprocess.run(
        ["docker", "compose", "ps", "--services", "--status", "running"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout
    return set(out.split())


@pytest.fixture(autouse=True, scope="module")
def _needs_monitoring() -> None:
    """After `make up` (core only) this module skips instead of failing on connection errors."""
    try:
        running = running_services()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        # no Docker, or compose cannot read the project (an .env without the monitoring keys)
        pytest.skip("docker compose cannot list the services")
    if not {"prometheus", "grafana"} <= running:
        pytest.skip("the monitoring profile is not running (make monitoring)")


def prom(path: str, **params: str) -> Any:
    resp = httpx.get(f"{PROMETHEUS}/api/v1/{path}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()["data"]


def grafana(method: str, path: str, **kw: Any) -> Any:
    resp = httpx.request(method, f"{GRAFANA}{path}", auth=AUTH, timeout=60, **kw)
    resp.raise_for_status()
    return resp.json()


def targets() -> dict[str, str]:
    return {t["labels"]["job"]: t["health"] for t in prom("targets")["activeTargets"]}


def test_targets_of_running_services_are_up() -> None:
    running = running_services()
    expected = {
        job
        for job in targets()
        if (SERVICE.get(job, job) if job != "prometheus" else "prometheus") in running
    }
    assert {"prometheus", "clickhouse", "postgres-oltp", "postgres-control"} <= expected
    # A target scraped for the first time (NiFi every 30 s) needs up to one interval.
    deadline = time.monotonic() + 90
    while True:
        health = targets()
        down = {job for job in expected if health.get(job) != "up"}
        if not down or time.monotonic() > deadline:
            break
        time.sleep(5)
    assert not down


def test_dashboard_metrics_exist_for_jobs_that_are_up() -> None:
    names = set(prom("label/__name__/values"))
    up = {job for job, h in targets().items() if h == "up"}
    missing = []
    for board in DASHBOARDS.values():
        for panel in board["panels"]:
            for t in panel["targets"]:
                for name in re.findall(
                    r"\b(?:nifi_|ClickHouse|pg_|workload_|dbt_runner_)\w+", t.get("expr", "")
                ):
                    job = next(j for p, j in PREFIX.items() if name.startswith(p))
                    if job in up and name not in LAZY and name not in names:
                        missing.append(name)
    assert not missing


def test_grafana_lists_the_dashboards_and_alert_rules() -> None:
    found = {d["uid"] for d in grafana("GET", "/api/search", params={"type": "dash-db"})}
    assert found >= {"pipeline-overview", "pricing-outcomes", "platform"}
    rules = grafana("GET", "/api/v1/provisioning/alert-rules")
    assert {r["title"] for r in rules} == RULES


@pytest.mark.parametrize("uid", ["prometheus", "control", "clickhouse"])
def test_datasource_health(uid: str) -> None:
    assert grafana("GET", f"/api/datasources/uid/{uid}/health")["status"] == "OK"


@pytest.mark.parametrize("name", sorted(DASHBOARDS))
def test_every_panel_query_runs(name: str) -> None:
    errors = []
    for panel in DASHBOARDS[name]["panels"]:
        queries = [{**t, "intervalMs": 15000, "maxDataPoints": 500} for t in panel["targets"]]
        resp = httpx.post(
            f"{GRAFANA}/api/ds/query",
            auth=AUTH,
            timeout=60,
            json={"queries": queries, "from": "now-6h", "to": "now"},
        )
        results = resp.json().get("results", {})
        bad = {k: v.get("error") for k, v in results.items() if v.get("error")}
        if resp.status_code != 200 or bad:
            errors.append((panel["title"], resp.status_code, bad or resp.text[:300]))
    assert not errors


def test_alert_rules_evaluate_without_errors() -> None:
    # Rules evaluate every minute; a fresh Grafana may not have run them yet.
    deadline = time.monotonic() + 90
    while True:
        groups = grafana("GET", "/api/prometheus/grafana/api/v1/rules")["data"]["groups"]
        rules = {r["name"]: r for g in groups for r in g["rules"]}
        pending = [n for n, r in rules.items() if r.get("health") in (None, "", "unknown")]
        if not pending or time.monotonic() > deadline:
            break
        time.sleep(5)
    assert set(rules) == RULES
    failing = {n: r.get("lastError") for n, r in rules.items() if r["health"] != "ok"}
    assert not failing
