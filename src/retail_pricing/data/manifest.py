"""Manifest helpers: file sha256 (corruption) and an order-independent content hash."""

import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb

_MOD = 2**64


def content_hash(con: duckdb.DuckDBPyConnection, relation: str) -> tuple[int, str]:
    """Row count and sum(md5_lower64(row as text)) mod 2^64 as hex.

    The sum runs in HUGEINT (exact; at most rows * 2^64), so the order of rows never matters.
    `relation` is a table name or a table function such as read_parquet('...').
    """
    con.execute("SET TimeZone = 'UTC'")  # timestamptz renders in the session time zone
    row = con.execute(
        f"SELECT count(*), sum(md5_number_lower(CAST(t AS VARCHAR)))::HUGEINT % {_MOD}::HUGEINT "
        f"FROM {relation} t"
    ).fetchone()
    assert row is not None
    return int(row[0]), f"{int(row[1] or 0):016x}"


def parquet_hash(con: duckdb.DuckDBPyConnection, path: Path) -> tuple[int, str]:
    # hive_partitioning off: the sf=<N> directory must not become a column
    return content_hash(con, f"read_parquet('{path.as_posix()}', hive_partitioning = false)")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def write_manifest(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def sf_label(sf: float) -> str:
    """Directory label: 0.01 -> '0.01', 1.0 -> '1'."""
    return f"{sf:g}"


def connect(path: Path | str, memory_limit: str, threads: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute(f"SET memory_limit = '{memory_limit}'; SET threads = {int(threads)};")
    con.execute("SET TimeZone = 'UTC'")
    return con
