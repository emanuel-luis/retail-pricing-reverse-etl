"""Each role created by `oltp load` can do what it must and nothing more (every write rolled back)."""

from collections.abc import Iterator

import psycopg
import pytest
from pydantic import SecretStr

from retail_pricing.settings import OltpSettings

pytestmark = pytest.mark.sql

ALLOWED = {
    "workload": [
        (
            "INSERT INTO sales.sale (channel, store_id, business_date, sold_at, total_amount)"
            " VALUES ('STORE', 1, DATE '2025-12-05', now(), 1)"
        ),
        "UPDATE inventory.store_stock SET on_hand = on_hand WHERE store_id = 1",
        "UPDATE pricing.zone_price SET price = price WHERE product_id = 1",
        "UPDATE pricing.store_price_override SET price = price WHERE valid_to IS NULL",
        (
            "INSERT INTO pricing.supplier_cost (product_id, valid_from, unit_cost)"
            " VALUES (1, DATE '2030-01-01', 1)"
        ),
        "UPDATE pricing.shelf_label_queue SET status = 'printed' WHERE false",
        "UPDATE ops.business_clock SET business_date = business_date",
        "SELECT count(*) FROM rules.kvi",
    ],
    "nifi": [
        "SELECT count(*) FROM sales.sale_line",
        "SELECT count(*) FROM pricing.zone_price",
        (
            "INSERT INTO pricing_apply.zone_price_stage"
            " (cycle_id, product_id, zone_id, batch_no, new_price, expected_version)"
            " VALUES (-1, 1, 1, 1, 1, 1)"
        ),
        "SELECT * FROM pricing_apply.apply_batch(-1, 'zone_price', 1)",
        "SELECT * FROM pricing_apply.reconcile_actual(-1, 'zone_price')",
        "SELECT count(*) FROM pricing_apply.batch",
    ],
    "exporter": ["SELECT count(*) FROM pg_stat_activity", "SELECT * FROM pg_stat_database"],
}

DENIED = {
    "workload": [
        "UPDATE rules.kvi SET kvi_tier = kvi_tier",
        "UPDATE pricing.zone_price SET last_cycle_id = 1",
        (
            "INSERT INTO pricing.store_price_override (store_id, product_id, price, valid_from)"
            " VALUES (1, 1, 1, DATE '2030-01-01')"
        ),
        "UPDATE pricing.price_history SET price = price",
        "DELETE FROM sales.sale WHERE false",
        "SELECT count(*) FROM pricing_apply.batch",
        "SELECT * FROM pricing_apply.apply_batch(-1, 'zone_price', 1)",
    ],
    "nifi": [
        "UPDATE pricing.zone_price SET price = price",
        "INSERT INTO pricing_apply.batch (cycle_id, target, batch_no) VALUES (-1, 'x', 1)",
        "UPDATE pricing_apply.zone_price_stage SET status = 'x'",
        "SELECT pricing_apply._apply_zone_price(-1, 1)",
        (
            "INSERT INTO sales.sale (channel, business_date, sold_at, total_amount)"
            " VALUES ('WEB', DATE '2025-12-05', now(), 1)"
        ),
    ],
    "exporter": ["SELECT count(*) FROM master.product"],
}


def _creds(s: OltpSettings, role: str) -> tuple[str, SecretStr]:
    user, password = {
        "workload": (s.workload_user, s.workload_password),
        "nifi": (s.nifi_user, s.nifi_password),
        "exporter": (s.exporter_user, s.exporter_password),
    }[role]
    return user, password


@pytest.fixture(params=list(ALLOWED))
def role_conn(
    request: pytest.FixtureRequest, oltp_settings: OltpSettings
) -> Iterator[tuple[str, psycopg.Connection]]:
    role: str = request.param
    with psycopg.connect(oltp_settings.conninfo(*_creds(oltp_settings, role))) as conn:
        yield role, conn


def test_role_can_do_what_it_must(role_conn: tuple[str, psycopg.Connection]) -> None:
    role, conn = role_conn
    for stmt in ALLOWED[role]:
        with conn.transaction(force_rollback=True):
            conn.execute(stmt.encode())


def test_role_cannot_do_anything_else(role_conn: tuple[str, psycopg.Connection]) -> None:
    role, conn = role_conn
    for stmt in DENIED[role]:
        with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
            conn.execute(stmt.encode())


def test_only_the_engine_role_keeps_last_cycle_id(oltp_owner: psycopg.Connection) -> None:
    """app.actor is a free setting: claiming 'pricing-engine' without the role changes nothing."""
    key = "WHERE product_id = 1 AND zone_id = 1"
    with oltp_owner.transaction(force_rollback=True):
        oltp_owner.execute("SET LOCAL ROLE pricing_engine")
        row = oltp_owner.execute(
            f"UPDATE pricing.zone_price SET price = price + 0.01, last_cycle_id = 999 {key}"
            " RETURNING last_cycle_id, updated_by"
        ).fetchone()
        assert row == (999, "pricing-engine")
        oltp_owner.execute("RESET ROLE")
        oltp_owner.execute("SET LOCAL ROLE workload")
        oltp_owner.execute("SET LOCAL app.actor = 'pricing-engine'")
        row = oltp_owner.execute(
            f"UPDATE pricing.zone_price SET price = price + 0.01 {key}"
            " RETURNING last_cycle_id, updated_by"
        ).fetchone()
        assert row == (None, "unknown")
        source = oltp_owner.execute(
            f"SELECT source FROM pricing.price_history {key} AND valid_to IS NULL"
        ).fetchone()
        assert source == ("manual",)
