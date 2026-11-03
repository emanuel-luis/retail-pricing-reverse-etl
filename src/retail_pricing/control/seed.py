"""Registry seed: the NiFi jobs and the extract list, derived from the OLTP contract."""

from dataclasses import dataclass

from retail_pricing.data.contract import EXTRACT_TABLES

NUMERIC_TYPES = ("bigint", "int", "smallint")


@dataclass(frozen=True)
class JobSeed:
    job_id: str
    process_group_name: str
    source: str
    target: str
    owner: str
    sla: str  # a Postgres interval literal
    stage_order: int
    schedule_interval: str | None = None
    batch_rows: int | None = None


_OWNER = "data-platform"

JOBS: tuple[JobSeed, ...] = (
    JobSeed(
        "cycle",
        "00 trigger",
        "http or schedule",
        "audit.cycle",
        _OWNER,
        "30 minutes",
        0,
        schedule_interval="10 minutes",
    ),
    JobSeed("extract", "10 extract", "oltp", "s3 raw", _OWNER, "10 minutes", 10),
    JobSeed("load", "20 load", "s3 raw", "clickhouse raw", _OWNER, "10 minutes", 20),
    JobSeed(
        "transform", "30 transform", "clickhouse raw", "clickhouse marts", _OWNER, "15 minutes", 30
    ),
    JobSeed(
        "reverse.zone_price",
        "40 reverse zone_price",
        "clickhouse reverse",
        "oltp pricing.zone_price",
        _OWNER,
        "5 minutes",
        40,
        batch_rows=1000,
    ),
    JobSeed(
        "reverse.store_override",
        "41 reverse store_override",
        "clickhouse reverse",
        "oltp pricing.store_price_override",
        _OWNER,
        "5 minutes",
        41,
        batch_rows=1000,
    ),
    JobSeed(
        "reverse.promo_price",
        "42 reverse promo_price",
        "clickhouse reverse",
        "oltp pricing.promo_price",
        _OWNER,
        "5 minutes",
        42,
        batch_rows=1000,
    ),
    JobSeed(
        "reverse.shelf_label",
        "43 reverse shelf_label",
        "clickhouse reverse",
        "oltp pricing.shelf_label_queue",
        _OWNER,
        "5 minutes",
        43,
        batch_rows=5000,
    ),
    JobSeed(
        "reconcile",
        "50 reconcile",
        "clickhouse and oltp",
        "audit.reconcile_result",
        _OWNER,
        "5 minutes",
        50,
    ),
)


@dataclass(frozen=True)
class ExtractSeed:
    table_name: str
    pk_column: str
    select_list: str
    load_order: int


def pk_column(table: str) -> str:
    """A dense numeric key column for value partitioning.

    The single-column primary key, or for a composite key product_id when it is part of the key
    (the widest range), else the first numeric key column.
    """
    numeric = [c.name for c in EXTRACT_TABLES[table] if c.pk and c.type in NUMERIC_TYPES]
    if not numeric:
        raise ValueError(f"{table} has no numeric primary key column")
    return "product_id" if "product_id" in numeric else numeric[0]


def extract_tables() -> list[ExtractSeed]:
    """Every contract table in contract (foreign key) order, then the extract-only tables."""
    return [
        ExtractSeed(name, pk_column(name), ", ".join(c.name for c in cols), order)
        for order, (name, cols) in enumerate(EXTRACT_TABLES.items(), start=1)
    ]
