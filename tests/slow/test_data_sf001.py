"""SF0.01 end to end: generate and build twice, then check counts, shapes, FKs and rules."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from retail_pricing.data import synth
from retail_pricing.data.build import build
from retail_pricing.data.contract import TABLES, primary_key
from retail_pricing.data.generate import KNOWN_COUNTS, generate
from retail_pricing.data.manifest import read_manifest
from retail_pricing.settings import DataSettings

pytestmark = pytest.mark.slow
SF = 0.01


@dataclass
class Run:
    tpcds: dict[str, Any]
    oltp: dict[str, Any]
    con: duckdb.DuckDBPyConnection


def _run(root: Path) -> Run:
    settings = DataSettings(dir=root)
    src = generate(SF, settings)
    out = build(SF, settings)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for table in TABLES:
        schema, name = table.split(".")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        path = (out / f"{schema}__{name}.parquet").as_posix()
        con.execute(
            f"CREATE VIEW {table} AS SELECT * "
            f"FROM read_parquet('{path}', hive_partitioning = false)"
        )
    return Run(
        read_manifest(src / "manifest.json") or {}, read_manifest(out / "manifest.json") or {}, con
    )


@pytest.fixture(scope="module")
def runs(tmp_path_factory: pytest.TempPathFactory) -> tuple[Run, Run]:
    return _run(tmp_path_factory.mktemp("a")), _run(tmp_path_factory.mktemp("b"))


@pytest.fixture(scope="module")
def con(runs: tuple[Run, Run]) -> duckdb.DuckDBPyConnection:
    return runs[0].con


def _one(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    row = con.execute(sql).fetchone()
    assert row is not None
    return row[0]


def test_tpcds_counts(runs: tuple[Run, Run]) -> None:
    counts = {t: e["rows"] for t, e in runs[0].tpcds["tables"].items()}
    assert counts == KNOWN_COUNTS[SF]


def test_parquet_schemas_match_the_contract(con: duckdb.DuckDBPyConnection) -> None:
    for table, cols in TABLES.items():
        got = [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()]
        assert got == [(c.name, c.duckdb_type) for c in cols], table
        for c in cols:
            if not c.nullable:
                assert _one(con, f"SELECT count(*) FROM {table} WHERE {c.name} IS NULL") == 0
        pk = ", ".join(primary_key(table))
        dupes = f"SELECT count(*) FROM (SELECT {pk} FROM {table} GROUP BY ALL HAVING count(*) > 1)"
        assert _one(con, dupes) == 0, table


def test_foreign_keys_hold(con: duckdb.DuckDBPyConnection) -> None:
    for table, cols in TABLES.items():
        for c in cols:
            if c.fk is None:
                continue
            parent, column = c.fk.rsplit(".", 1)
            orphans = _one(
                con,
                f"SELECT count(*) FROM {table} WHERE {c.name} IS NOT NULL "
                f"AND {c.name} NOT IN (SELECT {column} FROM {parent})",
            )
            assert orphans == 0, f"{table}.{c.name} -> {c.fk}"


def test_two_runs_give_identical_content(runs: tuple[Run, Run]) -> None:
    a, b = runs
    for key in ("tpcds", "oltp"):
        ha = {t: e["content_hash"] for t, e in getattr(a, key)["tables"].items()}
        hb = {t: e["content_hash"] for t, e in getattr(b, key)["tables"].items()}
        assert ha == hb, key
    assert a.oltp["dropped"] == b.oltp["dropped"]
    assert a.oltp["adjusted"] == b.oltp["adjusted"]
    assert a.oltp["anchor"] == synth.ANCHOR.isoformat()


def test_zones_stores_and_products(con: duckdb.DuckDBPyConnection) -> None:
    stores = _one(con, "SELECT count(*) FROM master.store")
    zones = con.execute("SELECT zone_id, channel FROM master.price_zone ORDER BY 1").fetchall()
    z = synth.zone_count(stores)
    assert zones == [(i, "STORE") for i in range(1, z + 1)] + [(z + 1, "WEB")]
    for store_id, zone_id in con.execute("SELECT store_id, zone_id FROM master.store").fetchall():
        assert zone_id == synth.store_zone(store_id, z)
    for size, units in con.execute("SELECT size, pack_units FROM master.product").fetchall():
        assert units == synth.pack_units(size)


def test_sales_window_and_totals(con: duckdb.DuckDBPyConnection) -> None:
    window = con.execute("SELECT min(business_date), max(business_date) FROM sales.sale")
    lo, hi = window.fetchone() or (None, None)
    assert (lo, hi) == (date(2020, 12, 4), synth.ANCHOR)
    mismatched = _one(
        con,
        "SELECT count(*) FROM sales.sale s JOIN (SELECT sale_id, sum(net_amount) t "
        "FROM sales.sale_line GROUP BY 1) l USING (sale_id) WHERE s.total_amount <> l.t",
    )
    assert mismatched == 0
    assert _one(con, "SELECT max(business_date) FROM sales.sale_return") <= synth.ANCHOR


def test_prices_are_on_the_grid_and_ladders_hold(con: duckdb.DuckDBPyConnection) -> None:
    bands = con.execute(
        "SELECT min_price, max_price, ending FROM rules.price_ending_band ORDER BY band_id"
    ).fetchall()
    grid = synth.Grid(synth.Band(*b) for b in bands)
    for (p,) in con.execute(
        "SELECT price FROM pricing.zone_price UNION ALL SELECT promo_price FROM pricing.promo_price"
    ).fetchall():
        assert grid.round(p) == p

    members: dict[int, list[tuple[int, int, Decimal]]] = defaultdict(list)
    kinds = {}
    for gid, kind, pid, rank, pack in con.execute(
        "SELECT g.ladder_group_id, g.ladder_type, m.product_id, m.rank, p.pack_units "
        "FROM rules.ladder_member m JOIN rules.ladder_group g USING (ladder_group_id) "
        "JOIN master.product p USING (product_id)"
    ).fetchall():
        members[gid].append((pid, rank, pack))
        kinds[gid] = kind
    assert members and all(len(m) >= 2 for m in members.values())
    assert _one(con, "SELECT count(*) FROM rules.ladder_group") == len(members)
    prices: dict[int, dict[int, Decimal]] = defaultdict(dict)
    for zone, pid, price in con.execute(
        "SELECT zone_id, product_id, price FROM pricing.zone_price"
    ).fetchall():
        prices[zone][pid] = price
    for zone_prices in prices.values():
        for gid, m in members.items():
            assert synth.ladder_holds(synth.Ladder(kinds[gid], tuple(m)), zone_prices), gid


def test_pricing_rules(con: duckdb.DuckDBPyConnection) -> None:
    not_below = _one(
        con,
        "SELECT count(*) FROM pricing.promo_price pp JOIN pricing.zone_price zp "
        "USING (product_id, zone_id) WHERE pp.promo_price >= zp.price",
    )
    assert not_below == 0
    open_rows = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE h.price <> zp.price) FROM pricing.price_history h "
        "JOIN pricing.zone_price zp USING (product_id, zone_id) WHERE h.valid_to IS NULL"
    ).fetchone()
    assert open_rows == (_one(con, "SELECT count(*) FROM pricing.zone_price"), 0)

    products = _one(con, "SELECT count(*) FROM master.product")
    assert _one(con, "SELECT count(*) FROM rules.kvi") == synth.kvi_count(products)
    assert _one(con, "SELECT count(*) FROM pricing.supplier_cost") == 24 * products

    breached = {
        pid
        for (pid,) in con.execute(
            """
            SELECT c.product_id
            FROM pricing.supplier_cost c
            JOIN master.product p USING (product_id)
            JOIN master.product_class pc USING (class_id)
            JOIN rules.category_rule r USING (category_id)
            JOIN (SELECT product_id, min(price) AS lo FROM pricing.zone_price GROUP BY 1) z
              USING (product_id)
            WHERE c.valid_from = (SELECT max(valid_from) FROM pricing.supplier_cost)
              AND c.unit_cost > (1 - r.min_margin) * z.lo
            """
        ).fetchall()
    }
    flagged = {
        pid
        for (pid,) in con.execute("SELECT product_id FROM master.product").fetchall()
        if synth.u(pid, "cost_below_min") < 0.03
    }
    assert breached == flagged


def test_store_stock_is_the_sold_assortment(con: duckdb.DuckDBPyConnection) -> None:
    diff = _one(
        con,
        """
        SELECT count(*) FROM (
            (SELECT store_id, product_id FROM inventory.store_stock)
            EXCEPT
            (SELECT DISTINCT h.store_id, l.product_id
             FROM sales.sale_line l JOIN sales.sale h USING (sale_id)
             WHERE h.channel = 'STORE' AND h.business_date > DATE '2025-12-04' - 365))
        """,
    )
    assert diff == 0
    assert _one(con, "SELECT count(*) FROM inventory.store_stock") == _one(
        con, "SELECT count(*) FROM inventory.stock_movement"
    )
