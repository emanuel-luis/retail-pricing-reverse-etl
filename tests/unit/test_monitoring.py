"""Monitoring config as files: Prometheus scrape jobs, Grafana datasources, dashboards, alerts."""

import json
from pathlib import Path
from typing import Any, Self

import pytest
import yaml
from typer.testing import CliRunner

from retail_pricing.cli import app

ROOT = Path(__file__).resolve().parents[2]
GRAFANA = ROOT / "config" / "grafana"
DASHBOARDS = sorted((GRAFANA / "dashboards").glob("*.json"))
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
MONITORING = ["prometheus", "grafana", "pg-exporter-oltp", "pg-exporter-control", "nifi-token"]


def load_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


COMPOSE = load_yaml(ROOT / "compose.yaml")["services"]
DATASOURCES = load_yaml(GRAFANA / "provisioning" / "datasources" / "datasources.yaml")
DS_UIDS = {d["uid"] for d in DATASOURCES["datasources"]}
ALERTS = load_yaml(GRAFANA / "provisioning" / "alerting" / "alerts.yaml")
PROMETHEUS = load_yaml(ROOT / "config" / "prometheus" / "prometheus.yml")


def container_ports(service: dict[str, Any]) -> set[int]:
    ports = {int(p.rsplit(":", 1)[-1]) for p in service.get("ports", [])}
    return ports | {int(p) for p in service.get("expose", [])}


def test_three_dashboards_with_unique_uids() -> None:
    uids = [json.loads(p.read_text(encoding="utf-8"))["uid"] for p in DASHBOARDS]
    assert sorted(uids) == ["pipeline-overview", "platform", "pricing-outcomes"]


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.stem)
def test_every_panel_queries_a_provisioned_datasource(path: Path) -> None:
    board = json.loads(path.read_text(encoding="utf-8"))
    panels = board["panels"]
    assert panels
    assert len({p["id"] for p in panels}) == len(panels)
    for p in panels:
        assert p["datasource"]["uid"] in DS_UIDS, p["title"]
        assert p["targets"], p["title"]
        for t in p["targets"]:
            assert t["datasource"]["uid"] == p["datasource"]["uid"], p["title"]
            assert t.get("rawSql") or t.get("expr"), p["title"]


def test_datasources_take_passwords_from_the_environment() -> None:
    assert {"prometheus", "control", "clickhouse"} == DS_UIDS
    for d in DATASOURCES["datasources"]:
        for value in d.get("secureJsonData", {}).values():
            assert value.startswith("$"), d["uid"]


def test_every_scrape_target_is_a_compose_service_port() -> None:
    jobs = {j["job_name"]: j for j in PROMETHEUS["scrape_configs"]}
    assert set(jobs) == {
        "nifi",
        "clickhouse",
        "postgres-oltp",
        "postgres-control",
        "dbt-runner",
        "workload",
        "prometheus",
    }
    for name, job in jobs.items():
        for target in (t for sc in job["static_configs"] for t in sc["targets"]):
            host, port = target.rsplit(":", 1)
            service = "prometheus" if host == "localhost" else host
            assert service in COMPOSE, (name, target)
            assert int(port) in container_ports(COMPOSE[service]), (name, target)


def test_nifi_is_scraped_with_the_sidecar_token() -> None:
    nifi = next(j for j in PROMETHEUS["scrape_configs"] if j["job_name"] == "nifi")
    token_dir = str(Path(nifi["authorization"]["credentials_file"]).parent.as_posix())
    assert f"nifi_token:{token_dir}:ro" in COMPOSE["prometheus"]["volumes"]
    assert f"nifi_token:{token_dir}" in COMPOSE["nifi-token"]["volumes"]
    # Only NiFi skips the certificate check.
    skips = [j["job_name"] for j in PROMETHEUS["scrape_configs"] if "tls_config" in j]
    assert skips == ["nifi"]


@pytest.mark.parametrize("name", MONITORING)
def test_monitoring_services_follow_the_compose_rules(name: str) -> None:
    s = COMPOSE[name]
    assert s["profiles"] == ["monitoring"]
    assert s["mem_limit"]
    assert s["healthcheck"]["test"]
    assert "@sha256:" in s["image"] or s.get("pull_policy") == "never"
    assert all(p.startswith("127.0.0.1:") for p in s.get("ports", []))


def test_alert_rules_cover_the_plan_and_query_provisioned_datasources() -> None:
    rules = [r for g in ALERTS["groups"] for r in g["rules"]]
    assert {r["title"] for r in rules} == RULES
    assert len(rules) == len(RULES)
    for r in rules:
        refs = {d["refId"] for d in r["data"]}
        assert r["condition"] in refs, r["title"]
        for d in r["data"]:
            assert d["datasourceUid"] in DS_UIDS | {"__expr__"}, r["title"]
    receivers = {c["name"] for c in ALERTS["contactPoints"]}
    assert {p["receiver"] for p in ALERTS["policies"]} <= receivers


def test_token_interval_rewrites_the_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tokens = iter(["first", "second"])

    class FakeClient:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *exc: object) -> None:
            pass

        def fetch_token(self, wait_s: float) -> str:
            return next(tokens)

    class Stop(Exception):
        pass

    seen: list[str] = []

    def fake_sleep(s: float) -> None:
        assert s == 60
        seen.append((tmp_path / "token").read_text(encoding="utf-8"))
        if len(seen) == 2:
            raise Stop

    monkeypatch.setattr("retail_pricing.nifi.client.connect", lambda _s: FakeClient())
    monkeypatch.setattr("time.sleep", fake_sleep)
    result = CliRunner().invoke(
        app, ["nifi", "token", "--out", str(tmp_path / "token"), "--interval", "60"]
    )
    assert isinstance(result.exception, Stop)
    assert seen == ["first", "second"]
    assert not (tmp_path / "token.tmp").exists()
