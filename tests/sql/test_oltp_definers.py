"""SECURITY DEFINER functions run as least-privilege NOLOGIN roles, never as a superuser."""

import psycopg
import pytest

pytestmark = pytest.mark.sql


def test_no_definer_function_is_owned_by_a_superuser(oltp_owner: psycopg.Connection) -> None:
    rows = oltp_owner.execute(
        "SELECT p.oid::regprocedure::text, r.rolname, r.rolsuper, p.proconfig"
        "  FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner"
        "  JOIN pg_namespace n ON n.oid = p.pronamespace"
        " WHERE p.prosecdef AND n.nspname IN ('pricing', 'pricing_apply', 'ops')"
        " ORDER BY 1"
    ).fetchall()
    assert [(name, owner) for name, owner, _, _ in rows] == [
        ("pricing.zone_price_history()", "price_history_writer"),
        ("pricing_apply.apply_batch(bigint,text,integer)", "pricing_engine"),
        ("pricing_apply.reconcile_actual(bigint,text)", "pricing_engine"),
    ]
    for _, _, superuser, config in rows:
        assert not superuser
        assert "search_path=pg_catalog, pg_temp" in config


@pytest.mark.parametrize(
    "stmt",
    [
        "DROP TABLE pricing.zone_price",
        "TRUNCATE pricing.price_change",
        "SELECT count(*) FROM master.customer",
        "DELETE FROM pricing.zone_price WHERE false",
        "UPDATE pricing.zone_price SET updated_by = 'x' WHERE false",
        (
            "INSERT INTO pricing.zone_price (product_id, zone_id, price, effective_from)"
            " VALUES (-1, -1, 1, DATE '2025-12-05')"
        ),
    ],
)
def test_pricing_engine_has_no_broad_rights(oltp_owner: psycopg.Connection, stmt: str) -> None:
    with (
        pytest.raises(psycopg.errors.InsufficientPrivilege),
        oltp_owner.transaction(),
    ):
        oltp_owner.execute("SET LOCAL ROLE pricing_engine")
        oltp_owner.execute(stmt.encode())


def test_price_history_writer_cannot_touch_prices(oltp_owner: psycopg.Connection) -> None:
    with pytest.raises(psycopg.errors.InsufficientPrivilege), oltp_owner.transaction():
        oltp_owner.execute("SET LOCAL ROLE price_history_writer")
        oltp_owner.execute("UPDATE pricing.zone_price SET price = price WHERE false")
