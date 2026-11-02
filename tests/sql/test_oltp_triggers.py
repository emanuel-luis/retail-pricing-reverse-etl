"""Row versioning, pricing ownership and zone price history triggers."""

from decimal import Decimal

import psycopg
import pytest

pytestmark = pytest.mark.sql

KEY = (1, 1)  # product 1, zone 1


def _zone_price(conn: psycopg.Connection) -> tuple[Decimal, int, str, int | None]:
    row = conn.execute(
        "SELECT price, version, updated_by, last_cycle_id FROM pricing.zone_price"
        " WHERE product_id = %s AND zone_id = %s",
        KEY,
    ).fetchone()
    assert row is not None
    return row


def _edit(conn: psycopg.Connection, actor: str, price: Decimal, cycle: int | None = None) -> None:
    with conn.transaction():
        # The engine is a role, not an actor name: only pricing_engine keeps last_cycle_id.
        if actor == "pricing-engine":
            conn.execute("SET LOCAL ROLE pricing_engine")
        conn.execute("SELECT set_config('app.actor', %s, true)", (actor,))
        conn.execute(
            "UPDATE pricing.zone_price SET price = %s, last_cycle_id = %s"
            " WHERE product_id = %s AND zone_id = %s",
            (price, cycle, *KEY),
        )


def _open_history(conn: psycopg.Connection) -> list[tuple[Decimal, str, int | None]]:
    return conn.execute(
        "SELECT price, source, cycle_id FROM pricing.price_history"
        " WHERE product_id = %s AND zone_id = %s AND valid_to IS NULL",
        KEY,
    ).fetchall()


def test_touch_row_bumps_version_and_updated_at(oltp_owner: psycopg.Connection) -> None:
    before = oltp_owner.execute(
        "SELECT version, updated_at FROM master.category WHERE category_id = 1"
    ).fetchone()
    oltp_owner.execute(
        "UPDATE master.category SET category_name = category_name WHERE category_id = 1"
    )
    after = oltp_owner.execute(
        "SELECT version, updated_at FROM master.category WHERE category_id = 1"
    ).fetchone()
    assert before is not None and after is not None
    assert after[0] == before[0] + 1
    assert after[1] > before[1]


def test_engine_keeps_cycle_and_manual_edit_clears_it(oltp_owner: psycopg.Connection) -> None:
    price, version, _, _ = _zone_price(oltp_owner)
    _edit(oltp_owner, "pricing-engine", price + Decimal("1.00"), cycle=42)
    p, v, by, cycle = _zone_price(oltp_owner)
    assert (p, v, by, cycle) == (price + 1, version + 1, "pricing-engine", 42)
    assert _open_history(oltp_owner) == [(price + 1, "engine", 42)]

    _edit(oltp_owner, "store-ops", price, cycle=99)
    p, v, by, cycle = _zone_price(oltp_owner)
    assert (p, v, by, cycle) == (price, version + 2, "store-ops", None)
    assert _open_history(oltp_owner) == [(price, "manual", None)]


def test_no_history_row_when_the_price_is_unchanged(oltp_owner: psycopg.Connection) -> None:
    price, _, _, _ = _zone_price(oltp_owner)
    n = "SELECT count(*) FROM pricing.price_history WHERE product_id = %s AND zone_id = %s"
    before = oltp_owner.execute(n, KEY).fetchone()
    _edit(oltp_owner, "store-ops", price)
    assert oltp_owner.execute(n, KEY).fetchone() == before


def test_unset_actor_is_unknown(oltp_owner: psycopg.Connection) -> None:
    oltp_owner.execute(
        "UPDATE pricing.zone_price SET price = price WHERE product_id = %s AND zone_id = %s", KEY
    )
    assert _zone_price(oltp_owner)[2] == "unknown"
