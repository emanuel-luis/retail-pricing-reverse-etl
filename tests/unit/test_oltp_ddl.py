import re

from retail_pricing.data.contract import TABLES
from retail_pricing.oltp import ddl


def test_every_contract_column_is_rendered_in_order() -> None:
    created = {
        m.group(1): m.group(2)
        for s in ddl.tables()
        if (m := re.match(r"CREATE TABLE IF NOT EXISTS (\S+) \((.*)\)", s, re.DOTALL))
    }
    assert list(created) == list(TABLES)
    for table, cols in TABLES.items():
        lines = [ln.strip() for ln in created[table].strip().splitlines()]
        assert [ln.split()[0] for ln in lines[:-1]] == [c.name for c in cols]
        for ln, c in zip(lines, cols, strict=False):
            assert ln.split()[1].rstrip(",") == c.type
            assert ("NOT NULL" in ln) == (not c.nullable)
        assert lines[-1].startswith("PRIMARY KEY")


def test_standard_columns_have_defaults() -> None:
    body = next(s for s in ddl.tables() if "pricing.zone_price " in s)
    assert "version bigint NOT NULL DEFAULT 1" in body
    assert "updated_at timestamptz NOT NULL DEFAULT now()" in body


def test_identities_only_on_single_non_fk_keys() -> None:
    assert ddl.identity_column("sales.sale") == "sale_id"
    assert ddl.identity_column("rules.kvi") is None  # PK is an FK
    assert ddl.identity_column("pricing.zone_price") is None  # composite
    assert ddl.identity_column("ops.business_clock") is None


def test_every_fk_is_constrained_and_indexed_or_leads_the_pk() -> None:
    cons, idx = "\n".join(ddl.constraints()), "\n".join(ddl.indexes())
    for table, col, ref in ddl.foreign_keys():
        assert f"ALTER TABLE {table} ADD CONSTRAINT" in cons
        assert f"FOREIGN KEY ({col}) REFERENCES {ref.rsplit('.', 1)[0]}" in cons
        assert f"ON {table} ({col})" in idx or ddl.primary_key(table)[0] == col
    for table in TABLES:
        assert f"ON {table} (updated_at)" in idx
