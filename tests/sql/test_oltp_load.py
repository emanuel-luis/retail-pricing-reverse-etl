"""The loaded seed round-trips the Parquet exactly (run `oltp load --sf 0.01` first)."""

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from retail_pricing.data.build import oltp_dir
from retail_pricing.data.contract import TABLES
from retail_pricing.oltp.load import LoadError, copy_table, load
from retail_pricing.settings import DataSettings, OltpSettings

pytestmark = pytest.mark.sql

SF = 0.01


@pytest.mark.parametrize("table", list(TABLES))
def test_table_round_trips_the_parquet(oltp_owner: psycopg.Connection, table: str) -> None:
    """COPY into a temp twin of the table (the loaded one is mutated by other tests)."""
    path = oltp_dir(DataSettings().dir, SF) / f"{table.replace('.', '__')}.parquet"
    t = pq.read_table(path)
    oltp_owner.execute(f"CREATE TEMP TABLE twin (LIKE {table})".encode())
    copy_table(oltp_owner, table, path, into="twin")
    exprs, expected = ["count(*)"], [t.num_rows]
    for c in TABLES[table]:
        col = t.column(c.name)
        exprs.append(f"count(*) - count({c.name})")
        expected.append(col.null_count)
        if c.type.startswith("numeric") or c.type in ("int", "bigint", "smallint"):
            exprs.append(f"sum({c.name})")
            expected.append(pc.sum(col).as_py())
        elif c.type == "text":
            exprs.append(f"sum(octet_length({c.name}))")
            expected.append(pc.sum(pc.binary_length(col)).as_py())
        elif c.type in ("date", "timestamptz"):
            exprs += [f"min({c.name})", f"max({c.name})"]
            expected += [pc.min(col).as_py(), pc.max(col).as_py()]
        elif c.type == "boolean":
            exprs.append(f"count(*) FILTER (WHERE {c.name})")
            expected.append(pc.sum(col.cast(pa.int64())).as_py() or 0)
    row = oltp_owner.execute(f"SELECT {', '.join(exprs)} FROM twin".encode()).fetchone()
    assert row is not None
    if t.num_rows == 0:  # sums of nothing are NULL on both sides
        assert row[0] == 0
        return
    assert list(row) == expected


def test_load_refuses_a_loaded_database(oltp_settings: OltpSettings) -> None:
    with pytest.raises(LoadError, match="--replace"):
        load(SF, DataSettings().dir, oltp_settings)


def test_identity_sequences_continue_after_the_seed(oltp_owner: psycopg.Connection) -> None:
    with oltp_owner.transaction(force_rollback=True):
        row = oltp_owner.execute(
            "INSERT INTO master.category (category_name) VALUES ('x') RETURNING category_id"
        ).fetchone()
        top = oltp_owner.execute("SELECT max(category_id) FROM master.category").fetchone()
    assert row is not None and top is not None and row[0] == top[0]
