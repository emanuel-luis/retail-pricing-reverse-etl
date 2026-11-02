from retail_pricing.data.build import sql_files
from retail_pricing.data.contract import LOGICAL_TYPES, OWNED, STANDARD, TABLES, primary_key


def test_tables_and_columns_are_well_formed() -> None:
    assert len(TABLES) == len(set(TABLES)) == 32
    for table, cols in TABLES.items():
        names = [c.name for c in cols]
        assert len(names) == len(set(names)), table
        assert primary_key(table), table
        assert cols[-2:] == STANDARD, table
        assert all(LOGICAL_TYPES.fullmatch(c.type) for c in cols), table
        assert not any(c.pk and c.nullable for c in cols), table
        assert all(c.duckdb_type for c in cols)


def test_pricing_owned_tables_carry_cycle_and_actor() -> None:
    owned = {t for t, cols in TABLES.items() if set(OWNED) <= set(cols)}
    assert owned == {"pricing.zone_price", "pricing.store_price_override", "pricing.promo_price"}


def test_foreign_keys_point_backwards_in_load_order() -> None:
    order = list(TABLES)
    for i, (table, cols) in enumerate(TABLES.items()):
        for c in cols:
            if c.fk is None:
                continue
            parent, column = c.fk.rsplit(".", 1)
            assert parent in order[:i], f"{table}.{c.name} -> {parent} is not loaded earlier"
            target = {p.name: p for p in TABLES[parent]}[column]
            assert target.pk and target.type == c.type, f"{table}.{c.name}"


def test_sql_files_follow_the_contract() -> None:
    tables = [t for t, _ in sql_files() if t is not None]
    assert tables == list(TABLES)
