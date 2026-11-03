"""Raw ClickHouse tables rendered from the data contract (PLAN 7).

`raw.<schema>__<table>` has the OLTP columns in contract order, then `_cycle_id` and
`_loaded_at`; ReplacingMergeTree(version) ordered by the primary key, no partitioning.
"""

from retail_pricing.data.contract import TABLES, Column, primary_key

# Text columns that hold a small set of codes, statuses or channels.
LOW_CARDINALITY = frozenset(
    {
        "channel",
        "ladder_type",
        "movement_type",
        "promo_policy",
        "reason_code",
        "source",
        "state",
        "status",
        "target",
        "updated_by",
    }
)
META_COLUMNS = (("_cycle_id", "UInt64"), ("_loaded_at", "DateTime64(3, 'UTC')"))
_SCALAR = {
    "bigint": "Int64",
    "int": "Int32",
    "smallint": "Int16",
    "date": "Date32",
    "timestamptz": "DateTime64(3, 'UTC')",
    "boolean": "Bool",
}


def raw_name(table: str) -> str:
    """'master.product' -> 'raw.master__product'."""
    return "raw." + table.replace(".", "__")


def column_type(col: Column) -> str:
    if col.name == "version":
        return "UInt64"  # a safe ReplacingMergeTree version type
    if col.type.startswith("numeric"):
        base = "Decimal" + col.type.removeprefix("numeric")
    elif col.type == "text":
        base = "String"
    else:
        base = _SCALAR[col.type]
    if col.type == "text" and col.name in LOW_CARDINALITY:
        return f"LowCardinality(Nullable({base}))" if col.nullable else f"LowCardinality({base})"
    return f"Nullable({base})" if col.nullable else base


def create_table(table: str) -> str:
    cols = [f"{c.name} {column_type(c)}" for c in TABLES[table]]
    cols += [f"{name} {type_}" for name, type_ in META_COLUMNS]
    body = ",\n".join(f"    {c}" for c in cols)
    order = ", ".join(primary_key(table))
    return (
        f"CREATE TABLE IF NOT EXISTS {raw_name(table)} (\n{body}\n)\n"
        f"ENGINE = ReplacingMergeTree(version)\nORDER BY ({order})"
    )


def tables() -> list[str]:
    return [create_table(t) for t in TABLES]
