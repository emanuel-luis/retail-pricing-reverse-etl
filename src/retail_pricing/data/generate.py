"""`data generate`: TPC-DS through DuckDB's tpcds extension, copied to zstd Parquet."""

import logging
import time
from pathlib import Path
from typing import Any

import duckdb

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

TABLES = (
    "store_sales",
    "store_returns",
    "web_sales",
    "web_returns",
    "inventory",
    "item",
    "customer",
    "customer_address",
    "store",
    "warehouse",
    "promotion",
    "date_dim",
)

KNOWN_COUNTS: dict[float, dict[str, int]] = {
    0.01: {
        "store_sales": 28_810,
        "store_returns": 2_829,
        "web_sales": 7_212,
        "web_returns": 685,
        "inventory": 23_490,
        "item": 180,
        "customer": 1_000,
        "customer_address": 500,
        "store": 1,
        "warehouse": 1,
        "promotion": 3,
        "date_dim": 73_049,
    },
    1.0: {
        "store_sales": 2_880_404,
        "store_returns": 287_514,
        "web_sales": 719_384,
        "web_returns": 71_763,
        "inventory": 11_745_000,
        "item": 18_000,
        "customer": 100_000,
        "customer_address": 50_000,
        "store": 12,
        "warehouse": 5,
        "promotion": 300,
        "date_dim": 73_049,
    },
    10.0: {
        "store_sales": 28_800_991,
        "store_returns": 2_875_432,
        "web_sales": 7_197_566,
        "web_returns": 719_217,
        "inventory": 133_110_000,
        "item": 102_000,
        "customer": 500_000,
        "customer_address": 250_000,
        "store": 102,
        "warehouse": 10,
        "promotion": 500,
        "date_dim": 73_049,
    },
}


def tpcds_dir(data_dir: Path, sf: float) -> Path:
    return data_dir / "tpcds" / f"sf={sf_label(sf)}"


def _is_current(out: Path, sf: float) -> bool:
    manifest = read_manifest(out / "manifest.json")
    if manifest is None or manifest.get("sf") != sf or manifest.get("duckdb") != duckdb.__version__:
        return False
    con = duckdb.connect()
    for table in TABLES:
        path = out / f"{table}.parquet"
        entry = manifest["tables"].get(table)
        if entry is None or not path.exists():
            return False
        row = con.execute(f"SELECT count(*) FROM read_parquet('{path.as_posix()}')").fetchone()
        if row is None or row[0] != entry["rows"]:
            return False
    return True


def check_counts(sf: float, counts: dict[str, int]) -> None:
    known = KNOWN_COUNTS.get(sf)
    if known is None:
        return
    wrong = {t: (counts.get(t), n) for t, n in known.items() if counts.get(t) != n}
    if wrong:
        raise RuntimeError(f"TPC-DS SF{sf_label(sf)} counts differ (got, expected): {wrong}")


def generate(sf: float, settings: DataSettings) -> Path:
    """Generate TPC-DS at `sf` into `<dir>/tpcds/sf=<sf>/`; skip when the manifest matches."""
    out = tpcds_dir(settings.dir, sf)
    if _is_current(out, sf):
        log.info("tpcds manifest matches, skipping", extra={"sf": sf, "dir": str(out)})
        return out
    out.mkdir(parents=True, exist_ok=True)
    db = out / "tpcds.duckdb"
    for stale in (db, db.with_suffix(".duckdb.wal"), out / "manifest.json"):
        stale.unlink(missing_ok=True)

    started = time.perf_counter()
    con = connect(db, settings.duckdb_memory_limit, settings.duckdb_threads)
    try:
        con.execute("INSTALL tpcds; LOAD tpcds;")
        con.execute(f"CALL dsdgen(sf={float(sf)})")
        gen_s = time.perf_counter() - started
        ext = con.execute(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'tpcds'"
        ).fetchone()
        tables: dict[str, dict[str, Any]] = {}
        for table in TABLES:
            path = out / f"{table}.parquet"
            con.execute(f"COPY {table} TO '{path.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
        con.close()
        db.unlink()
        con = duckdb.connect()
        for table in TABLES:
            path = out / f"{table}.parquet"
            rows, digest = parquet_hash(con, path)
            tables[table] = {"rows": rows, "sha256": sha256_file(path), "content_hash": digest}
    finally:
        con.close()

    check_counts(sf, {t: e["rows"] for t, e in tables.items()})
    write_manifest(
        out / "manifest.json",
        {
            "sf": sf,
            "duckdb": duckdb.__version__,
            "tpcds_extension": ext[0] if ext else None,
            "tables": tables,
        },
    )
    log.info(
        "tpcds generated",
        extra={
            "sf": sf,
            "dsdgen_s": round(gen_s, 2),
            "total_s": round(time.perf_counter() - started, 2),
        },
    )
    return out
