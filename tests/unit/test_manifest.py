from pathlib import Path

import duckdb

from retail_pricing.data.manifest import content_hash, parquet_hash, sf_label, sha256_file

ROWS = [(1, "a", "2025-12-04 10:00:00+00"), (2, None, None), (3, "c", "2020-12-04 00:00:00+00")]


def _table(rows: list[tuple[int, str | None, str | None]]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE t (id BIGINT, name VARCHAR, seen_at TIMESTAMPTZ)")
    if rows:
        con.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
    return con


def test_content_hash_ignores_row_order_and_detects_changes() -> None:
    rows, digest = content_hash(_table(ROWS), "t")
    assert rows == 3 and len(digest) == 16
    assert content_hash(_table(ROWS[::-1]), "t") == (3, digest)
    changed = [ROWS[0], (2, "b", None), ROWS[2]]
    assert content_hash(_table(changed), "t")[1] != digest
    assert content_hash(_table([]), "t") == (0, "0" * 16)


def test_content_hash_ignores_the_session_time_zone() -> None:
    con = _table(ROWS)
    expected = content_hash(con, "t")
    con.execute("SET TimeZone = 'America/Sao_Paulo'")
    assert content_hash(con, "t") == expected


def test_parquet_hash_matches_the_table(tmp_path: Path) -> None:
    con = _table(ROWS)
    path = tmp_path / "t.parquet"
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    assert parquet_hash(con, path) == content_hash(con, "t")
    assert len(sha256_file(path)) == 64


def test_sf_label() -> None:
    assert [sf_label(x) for x in (0.01, 1.0, 10.0)] == ["0.01", "1", "10"]
