import re

import pytest

from retail_pricing.clickhouse import ddl
from retail_pricing.data.contract import EXTRACT_TABLES, Column, primary_key


def _col(type_: str, name: str = "x", *, nullable: bool = False) -> Column:
    return Column(name, type_, nullable=nullable)


@pytest.mark.parametrize(
    ("col", "expected"),
    [
        (_col("bigint"), "Int64"),
        (_col("int"), "Int32"),
        (_col("smallint"), "Int16"),
        (_col("numeric(10,2)"), "Decimal(10,2)"),
        (_col("text"), "String"),
        (_col("text", "channel"), "LowCardinality(String)"),
        (_col("text", "reason_code", nullable=True), "LowCardinality(Nullable(String))"),
        (_col("date"), "Date32"),
        (_col("timestamptz"), "DateTime64(3, 'UTC')"),
        (_col("boolean"), "Bool"),
        (_col("int", nullable=True), "Nullable(Int32)"),
        (_col("numeric(5,2)", nullable=True), "Nullable(Decimal(5,2))"),
        (_col("date", nullable=True), "Nullable(Date32)"),
        (_col("bigint", "version"), "UInt64"),
    ],
)
def test_type_mapping(col: Column, expected: str) -> None:
    assert ddl.column_type(col) == expected


def test_every_contract_column_is_rendered_in_order_with_engine_and_order_key() -> None:
    stmts = ddl.tables()
    assert len(stmts) == len(EXTRACT_TABLES)
    for table, stmt in zip(EXTRACT_TABLES, stmts, strict=True):
        m = re.fullmatch(
            r"CREATE TABLE IF NOT EXISTS (\S+) \(\n(.*)\n\)\n"
            r"ENGINE = ReplacingMergeTree\(version\)\nORDER BY \((.*)\)",
            stmt,
            re.DOTALL,
        )
        assert m, stmt
        assert m.group(1) == "raw." + table.replace(".", "__")
        lines = [ln.strip().rstrip(",") for ln in m.group(2).splitlines()]
        names = [ln.split()[0] for ln in lines]
        assert names == [c.name for c in EXTRACT_TABLES[table]] + ["_cycle_id", "_loaded_at"]
        for ln, c in zip(lines, EXTRACT_TABLES[table], strict=False):
            assert ln == f"{c.name} {ddl.column_type(c)}"
        assert lines[-2:] == ["_cycle_id UInt64", "_loaded_at DateTime64(3, 'UTC')"]
        assert m.group(3) == ", ".join(primary_key(table))
        assert "PARTITION" not in stmt
