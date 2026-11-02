"""`data build`: the OLTP-shaped seed from TPC-DS Parquet, one DuckDB SQL file per table.

Each table is created empty from the contract (NOT NULL and defaults included), filled by
`sql/duckdb/NN_<schema>__<table>.sql` with `INSERT ... BY NAME`, and exported to
`<dir>/oltp/sf=<sf>/<schema>__<table>.parquet` in contract column order.
"""

import logging
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict
from decimal import Decimal
from importlib.resources import files
from pathlib import Path
from typing import Any

import duckdb

from retail_pricing.data import synth
from retail_pricing.data.contract import TABLES, primary_key
from retail_pricing.data.generate import TABLES as TPCDS_TABLES
from retail_pricing.data.generate import tpcds_dir
from retail_pricing.data.manifest import (
    connect,
    parquet_hash,
    read_manifest,
    sf_label,
    sha256_file,
    write_manifest,
)
from retail_pricing.settings import DataSettings

log = logging.getLogger(__name__)

SEED_DEFAULTS = {
    "version": "1",
    "updated_at": f"TIMESTAMPTZ '{synth.ANCHOR.isoformat()} 00:00:00+00'",
    "updated_by": "'seed'",
}


def oltp_dir(data_dir: Path, sf: float) -> Path:
    return data_dir / "oltp" / f"sf={sf_label(sf)}"


def sql_files() -> list[tuple[str | None, str]]:
    """(table or None for the prelude, SQL text) in file order; checked against the contract."""
    root = files("retail_pricing") / "sql" / "duckdb"
    out: list[tuple[str | None, str]] = []
    for entry in sorted(root.iterdir(), key=lambda e: e.name):
        if not entry.name.endswith(".sql"):
            continue
        stem = entry.name.removesuffix(".sql").split("_", 1)[1]
        table = None if stem == "prelude" else stem.replace("__", ".")
        out.append((table, entry.read_text(encoding="utf-8")))
    tables = [t for t, _ in out if t is not None]
    if tables != list(TABLES):
        raise RuntimeError(f"sql/duckdb files {tables} do not follow the contract order")
    return out


def create_macros(con: duckdb.DuckDBPyConnection) -> None:
    """The DuckDB side of synth.u() and synth.shift_date(); unit tests check the parity."""
    con.execute(
        f"""
        CREATE OR REPLACE MACRO u(key, salt) AS
            (md5_number(salt || ':' || CAST(key AS VARCHAR)) % 1000000) / 1e6;
        CREATE OR REPLACE MACRO shift_date(d) AS d + {synth.SHIFT_DAYS};
        CREATE OR REPLACE MACRO anchor() AS DATE '{synth.ANCHOR.isoformat()}';
        """
    )


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    for schema in sorted({t.split(".")[0] for t in TABLES}):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for table, cols in TABLES.items():
        defs = []
        for c in cols:
            d = f"{c.name} {c.duckdb_type}"
            if not c.nullable:
                d += " NOT NULL"
            if c.name in SEED_DEFAULTS:
                d += f" DEFAULT {SEED_DEFAULTS[c.name]}"
            defs.append(d)
        con.execute(f"CREATE TABLE {table} ({', '.join(defs)})")


def grid_lookup(grid: synth.Grid, max_cents: int) -> tuple[list[str | None], list[str | None]]:
    """For every cent value 0..max_cents: Grid.floor and Grid.round, as strings (None if none).

    Same results as calling the Grid per value (unit-tested), but a sweep over the grid points.
    """
    points: list[int] = []
    p = grid.ceil(0)
    while p is not None:
        points.append(int(p * 100))
        if points[-1] > max_cents:
            break
        p = grid.next_above(p)
    floors: list[str | None] = []
    rounds: list[str | None] = []
    for c in range(max_cents + 1):
        i = bisect_right(points, c)
        f = points[i - 1] if i else None
        up = points[bisect_left(points, c)] if c <= points[-1] else None
        r = up if f is None or (up is not None and up - c <= c - f) else f
        floors.append(None if f is None else f"{f / 100:.2f}")
        rounds.append(None if r is None else f"{r / 100:.2f}")
    return floors, rounds


def install_grid(con: duckdb.DuckDBPyConnection) -> synth.Grid:
    """Cents lookup of the ending grid (from rules.price_ending_band) plus two SQL macros.

    grid_round(v) rounds v to cents (half up), then to the nearest grid price (tie up);
    grid_floor(v) truncates v to cents, then takes the largest grid price <= it.
    """
    bands = con.execute(
        "SELECT min_price, max_price, ending FROM rules.price_ending_band ORDER BY band_id"
    ).fetchall()
    grid = synth.Grid(synth.Band(*b) for b in bands)
    top = con.execute("SELECT max(i_current_price) FROM tpcds.item").fetchone()
    max_cents = int(((top[0] if top and top[0] else Decimal(0)) * 2 + 100) * 100)
    cents = list(range(max_cents + 1))
    floors, rounds = grid_lookup(grid, max_cents)
    con.execute(
        """
        CREATE TABLE build.grid AS SELECT unnest($1) AS cents,
            unnest($2)::DECIMAL(10,2) AS floor_price, unnest($3)::DECIMAL(10,2) AS round_price
        """,
        [cents, floors, rounds],
    )
    con.execute(
        """
        CREATE OR REPLACE MACRO grid_round(v) AS
            (SELECT g.round_price FROM build.grid g WHERE g.cents = CAST(round(v * 100) AS BIGINT));
        CREATE OR REPLACE MACRO grid_floor(v) AS
            (SELECT g.floor_price FROM build.grid g WHERE g.cents = CAST(floor(v * 100) AS BIGINT));
        """
    )
    return grid


def repair_ladders(con: duckdb.DuckDBPyConnection, grid: synth.Grid) -> int:
    """Raise seed zone prices until every PACK and TIER ladder holds; returns rows changed."""
    members: dict[int, list[tuple[int, int, Decimal]]] = defaultdict(list)
    kinds: dict[int, str] = {}
    for gid, kind, pid, rank, pack in con.execute(
        """
        SELECT g.ladder_group_id, g.ladder_type, m.product_id, m.rank, p.pack_units
        FROM rules.ladder_member m JOIN rules.ladder_group g USING (ladder_group_id)
        JOIN master.product p USING (product_id) ORDER BY 1, 3
        """
    ).fetchall():
        members[gid].append((pid, rank, pack))
        kinds[gid] = kind
    ladders = [synth.Ladder(kinds[g], tuple(m)) for g, m in sorted(members.items())]
    by_zone: dict[int, dict[int, Decimal]] = defaultdict(dict)
    for zone, pid, price in con.execute(
        """
        SELECT zone_id, product_id, price FROM pricing.zone_price
        WHERE product_id IN (SELECT product_id FROM rules.ladder_member)
        """
    ).fetchall():
        by_zone[zone][pid] = price
    zs: list[int] = []
    ps: list[int] = []
    new: list[str] = []
    for zone, prices in sorted(by_zone.items()):
        fixed = synth.repair_zone(prices, ladders, grid)
        for pid in sorted(fixed):
            if fixed[pid] != prices[pid]:
                zs.append(zone)
                ps.append(pid)
                new.append(str(fixed[pid]))
    if new:
        con.execute(
            """
            UPDATE pricing.zone_price z SET price = r.price
            FROM (SELECT unnest($1) AS zone_id, unnest($2) AS product_id,
                         unnest($3)::DECIMAL(10,2) AS price) r
            WHERE z.zone_id = r.zone_id AND z.product_id = r.product_id
            """,
            [zs, ps, new],
        )
    return len(new)


def build(sf: float, settings: DataSettings) -> Path:
    """Build `<dir>/oltp/sf=<sf>/` from the TPC-DS Parquet of the same SF."""
    src = tpcds_dir(settings.dir, sf)
    if read_manifest(src / "manifest.json") is None:
        raise FileNotFoundError(f"no TPC-DS manifest in {src}; run `data generate --sf {sf:g}`")
    out = oltp_dir(settings.dir, sf)
    out.mkdir(parents=True, exist_ok=True)
    db = out / "seed.duckdb"  # not build.duckdb: the catalog name would clash with schema build
    for stale in (db, db.with_suffix(".duckdb.wal"), out / "manifest.json"):
        stale.unlink(missing_ok=True)

    started = time.perf_counter()
    adjusted: dict[str, int] = {}
    con = connect(db, settings.duckdb_memory_limit, settings.duckdb_threads)
    try:
        con.execute("CREATE SCHEMA tpcds; CREATE SCHEMA build;")
        for t in TPCDS_TABLES:
            pq = (src / f"{t}.parquet").as_posix()
            con.execute(
                f"CREATE VIEW tpcds.{t} AS SELECT * "
                f"FROM read_parquet('{pq}', hive_partitioning = false)"
            )
        create_macros(con)
        # TPC-DS leaves i_current_price and i_wholesale_cost NULL on some item versions (none at
        # SF0.01, some from SF1 on). Fill them deterministically so every price and cost
        # downstream is set: a price in 0.99..49.99 and a cost at 45..75% of it. A NULL record
        # start date becomes 1997-10-27, the earliest start TPC-DS assigns.
        item_pq = (src / "item.parquet").as_posix()
        con.execute(
            "CREATE OR REPLACE VIEW tpcds.item AS SELECT * REPLACE ("
            " CAST(coalesce(i_current_price, round(0.99 + 49 * u(i_item_sk::VARCHAR, 'fill_price'),"
            " 2)) AS DECIMAL(7,2)) AS i_current_price,"
            " CAST(coalesce(i_wholesale_cost, round(coalesce(i_current_price, round(0.99 + 49 *"
            " u(i_item_sk::VARCHAR, 'fill_price'), 2)) * (0.45 + 0.3 * u(i_item_sk::VARCHAR,"
            " 'fill_cost')), 2)) AS DECIMAL(7,2)) AS i_wholesale_cost,"
            " coalesce(i_rec_start_date, DATE '1997-10-27') AS i_rec_start_date)"
            f" FROM read_parquet('{item_pq}', hive_partitioning = false)"
        )
        con.execute("CREATE TABLE build.dropped (table_name VARCHAR, rule VARCHAR, rows BIGINT)")
        con.execute(
            "CREATE TABLE build.pack_units AS "
            "SELECT unnest($1) AS size, unnest($2)::DECIMAL(5,2) AS units",
            [list(synth.PACK_UNITS), [str(v) for v in synth.PACK_UNITS.values()]],
        )
        create_tables(con)
        grid: synth.Grid | None = None
        for table, sql in sql_files():
            t0 = time.perf_counter()
            if any(not ln.strip().startswith("--") for ln in sql.splitlines() if ln.strip()):
                con.execute(sql)
            if table == "rules.price_ending_band":
                grid = install_grid(con)
            elif table == "pricing.zone_price":
                assert grid is not None
                adjusted["pricing.zone_price.ladder_repair"] = repair_ladders(con, grid)
            log.info(
                "build step",
                extra={"step": table or "prelude", "s": round(time.perf_counter() - t0, 3)},
            )

        for table in TABLES:
            path = out / f"{table.replace('.', '__')}.parquet"
            order = ", ".join(primary_key(table))
            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order}) TO '{path.as_posix()}' "
                "(FORMAT parquet, COMPRESSION zstd)"
            )
        dropped: dict[str, dict[str, int]] = defaultdict(dict)
        for table, rule, rows in con.execute(
            "SELECT table_name, rule, sum(rows)::BIGINT FROM build.dropped "
            "GROUP BY ALL ORDER BY ALL"
        ).fetchall():
            dropped[table][rule] = rows
    finally:
        con.close()
    db.unlink(missing_ok=True)

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    tables: dict[str, dict[str, Any]] = {}
    for table in TABLES:
        path = out / f"{table.replace('.', '__')}.parquet"
        rows, digest = parquet_hash(con, path)
        tables[table] = {"rows": rows, "sha256": sha256_file(path), "content_hash": digest}
    con.close()
    write_manifest(
        out / "manifest.json",
        {
            "sf": sf,
            "anchor": synth.ANCHOR.isoformat(),
            "shift_days": synth.SHIFT_DAYS,
            "duckdb": duckdb.__version__,
            "tables": tables,
            "dropped": dropped,
            "adjusted": adjusted,
        },
    )
    log.info(
        "oltp seed built", extra={"sf": sf, "total_s": round(time.perf_counter() - started, 2)}
    )
    return out
