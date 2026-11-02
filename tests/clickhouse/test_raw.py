import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from ch_helpers import SF, reset_raw
from typer.testing import CliRunner

from retail_pricing.cli import app
from retail_pricing.clickhouse import ddl
from retail_pricing.clickhouse.client import Client
from retail_pricing.clickhouse.init import init
from retail_pricing.clickhouse.load_raw import load_raw
from retail_pricing.data.contract import TABLES, Column, primary_key
from retail_pricing.settings import ClickHouseSettings

pytestmark = pytest.mark.clickhouse


def _manifest(data_dir: Path) -> dict[str, dict[str, object]]:
    path = data_dir / "oltp" / "sf=0.01" / "manifest.json"
    tables: dict[str, dict[str, object]] = json.loads(path.read_text(encoding="utf-8"))["tables"]
    return tables


def test_init_twice_is_a_no_op(ch_settings: ClickHouseSettings, admin: Client) -> None:
    def snapshot() -> list[list[str | None]]:
        return admin.rows(
            "SELECT database, name, create_table_query FROM system.tables "
            "WHERE database IN ('raw', 'stg', 'int', 'pricing', 'reverse', 'marts') ORDER BY 1, 2"
        )

    init(ch_settings)
    first = snapshot()
    result = CliRunner().invoke(app, ["ch", "init"])
    assert result.exit_code == 0, result.output
    assert snapshot() == first
    assert len([r for r in first if r[0] == "raw"]) == len(TABLES)
    dbs = {r[0] for r in admin.rows("SELECT name FROM system.databases")}
    assert {"raw", "stg", "int", "pricing", "reverse", "marts"} <= dbs


def test_columns_follow_the_contract_plus_meta(admin: Client) -> None:
    for table, cols in TABLES.items():
        rows = admin.rows(
            "SELECT name, type FROM system.columns "
            f"WHERE database = 'raw' AND table = '{ddl.raw_name(table).removeprefix('raw.')}' "
            "ORDER BY position"
        )
        expected = [(c.name, ddl.column_type(c).replace(", ", ",")) for c in cols]
        expected += [(n, t.replace(", ", ",")) for n, t in ddl.META_COLUMNS]
        assert [(n, (t or "").replace(", ", ",")) for n, t in rows] == expected, table


def test_load_matches_the_manifest(loaded: None, admin: Client, data_dir: Path) -> None:
    for table, info in _manifest(data_dir).items():
        raw = ddl.raw_name(table)
        assert int(admin.scalar(f"SELECT count() FROM {raw}") or 0) == info["rows"], table
        assert admin.scalar(f"SELECT uniqExact(_cycle_id) FROM {raw}") in ("0", "1")


def test_second_cycle_collapses_under_final(
    ch_settings: ClickHouseSettings, admin: Client, data_dir: Path
) -> None:
    reset_raw(ch_settings, data_dir, 1)
    assert load_raw(SF, 2, data_dir, ch_settings) > 0
    for table, info in _manifest(data_dir).items():
        raw = ddl.raw_name(table)
        assert int(admin.scalar(f"SELECT count() FROM {raw} FINAL") or 0) == info["rows"], table
        if info["rows"]:  # equal versions: the last insert wins
            assert admin.scalar(f"SELECT max(_cycle_id) FROM {raw} FINAL") == "2", table
            assert admin.scalar(f"SELECT min(_cycle_id) FROM {raw} FINAL") == "2", table


def test_load_raw_cli_refuses_a_missing_seed(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app, ["ch", "load-raw", "--sf", "0.01", "--cycle", "9", "--data-dir", str(tmp_path)]
    )
    assert result.exit_code == 1


def _duckdb_expr(c: Column) -> str:
    if c.type == "timestamptz":
        return f"strftime({c.name} AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%g')"
    return f"CAST({c.name} AS VARCHAR)"


def _norm(
    cols: tuple[Column, ...], row: list[str | None] | tuple[str | None, ...]
) -> tuple[object, ...]:
    # ClickHouse prints Decimal(5,2) 2 as "2" where DuckDB says "2.00": compare as numbers
    return tuple(
        Decimal(v) if v is not None and c.type.startswith("numeric") else v
        for c, v in zip(cols, row, strict=True)
    )


def test_values_round_trip_against_the_parquet(loaded: None, admin: Client, data_dir: Path) -> None:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    checked = 0
    for table, cols in TABLES.items():
        path = (data_dir / "oltp" / "sf=0.01" / f"{table.replace('.', '__')}.parquet").as_posix()
        order = ", ".join(primary_key(table))
        want = con.execute(
            f"SELECT {', '.join(_duckdb_expr(c) for c in cols)} "
            f"FROM read_parquet('{path}', hive_partitioning = false) ORDER BY {order} LIMIT 25"
        ).fetchall()
        got = admin.rows(
            f"SELECT {', '.join(c.name for c in cols)} FROM {ddl.raw_name(table)} FINAL "
            f"ORDER BY {order} LIMIT 25"
        )
        assert [_norm(cols, r) for r in got] == [_norm(cols, r) for r in want], table
        checked += len(want)
    assert checked > 400
    kinds = {c.type.split("(")[0] for cols in TABLES.values() for c in cols if c.nullable}
    assert {"numeric", "text", "date"} <= kinds  # nullable columns of several types were compared
