from retail_pricing.control.seed import JOBS, extract_tables, pk_column
from retail_pricing.data.contract import TABLES


def test_every_contract_table_is_seeded_in_contract_order() -> None:
    seeds = extract_tables()
    assert [t.table_name for t in seeds] == list(TABLES)
    assert [t.load_order for t in seeds] == list(range(1, len(TABLES) + 1))


def test_select_list_is_the_contract_columns() -> None:
    for seed in extract_tables():
        assert seed.select_list.split(", ") == [c.name for c in TABLES[seed.table_name]]


def test_pk_column_is_a_numeric_key_column() -> None:
    for name, cols in TABLES.items():
        column = next(c for c in cols if c.name == pk_column(name))
        assert column.pk and column.type in ("bigint", "int", "smallint")
    assert pk_column("sales.sale") == "sale_id"
    assert pk_column("inventory.store_stock") == "product_id"
    assert pk_column("rules.ladder_member") == "product_id"
    assert pk_column("pricing.supplier_cost") == "product_id"


def test_jobs_follow_the_plan() -> None:
    assert [j.job_id for j in JOBS] == [
        "cycle",
        "extract",
        "load",
        "transform",
        "reverse.zone_price",
        "reverse.store_override",
        "reverse.promo_price",
        "reverse.shelf_label",
        "reconcile",
    ]
    assert [j.stage_order for j in JOBS] == sorted(j.stage_order for j in JOBS)
    assert {j.job_id: j.batch_rows for j in JOBS if j.batch_rows} == {
        "reverse.zone_price": 1000,
        "reverse.store_override": 1000,
        "reverse.promo_price": 1000,
        "reverse.shelf_label": 5000,
    }
    assert [j.job_id for j in JOBS if j.schedule_interval] == ["cycle"]
