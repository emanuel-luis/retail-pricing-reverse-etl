"""The workload runs against the loaded OLTP as role `workload` without errors."""

import socket

import psycopg
import pytest

from retail_pricing.settings import OltpSettings, WorkloadSettings
from retail_pricing.workload.run import edit, run

pytestmark = pytest.mark.sql


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_workload_runs_20_seconds_without_errors(oltp_owner: psycopg.Connection) -> None:
    def count(q: str) -> int:
        row = oltp_owner.execute(q.encode()).fetchone()
        assert row is not None
        return int(row[0])

    sales = count("SELECT count(*) FROM sales.sale")
    manual = count("SELECT count(*) FROM pricing.price_history WHERE source = 'manual'")
    settings = WorkloadSettings(metrics_port=_free_port(), price_edits_per_second=2.0)
    summary = run(settings, OltpSettings(), duration=20)
    assert summary["errors"] == 0, summary
    assert summary["sale"] > 100
    assert count("SELECT count(*) FROM sales.sale") - sales == summary["sale"]
    assert count("SELECT count(*) FROM pricing.price_history WHERE source = 'manual'") > manual


def test_scripted_edits_hit_kvi_first(oltp_owner: psycopg.Connection) -> None:
    assert edit(3, True, 7, OltpSettings()) == 3
    rows = oltp_owner.execute(
        "SELECT z.updated_by, k.product_id IS NOT NULL FROM pricing.zone_price z"
        " LEFT JOIN rules.kvi k USING (product_id) ORDER BY z.updated_at DESC LIMIT 3"
    ).fetchall()
    assert rows == [("store-ops", True)] * 3
