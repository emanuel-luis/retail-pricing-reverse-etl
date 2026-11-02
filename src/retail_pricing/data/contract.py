"""Single source of truth for the OLTP table shapes written by `data build`.

`TABLES` maps "schema.table" to its columns in DDL order. The mapping order is the FK load
order: every FK points at a table that appears earlier. Later steps generate or test the
Postgres DDL and the ClickHouse raw DDL against this module.
"""

import re
from dataclasses import dataclass

LOGICAL_TYPES = re.compile(r"bigint|int|smallint|numeric\(\d+,\d+\)|text|date|timestamptz|boolean")


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = False
    pk: bool = False
    fk: str | None = None  # "schema.table.column"

    @property
    def duckdb_type(self) -> str:
        t = self.type
        if t.startswith("numeric"):
            return "DECIMAL" + t.removeprefix("numeric")
        return {
            "bigint": "BIGINT",
            "int": "INTEGER",
            "smallint": "SMALLINT",
            "text": "VARCHAR",
            "date": "DATE",
            "timestamptz": "TIMESTAMP WITH TIME ZONE",
            "boolean": "BOOLEAN",
        }[t]


def _pk(name: str, type_: str, fk: str | None = None) -> Column:
    return Column(name, type_, pk=True, fk=fk)


def _fk(name: str, type_: str, ref: str, *, nullable: bool = False) -> Column:
    return Column(name, type_, nullable=nullable, fk=ref)


def _null(name: str, type_: str) -> Column:
    return Column(name, type_, nullable=True)


# Every table ends with these; pricing-owned tables add OWNED before them.
STANDARD = (Column("version", "bigint"), Column("updated_at", "timestamptz"))
OWNED = (_null("last_cycle_id", "bigint"), Column("updated_by", "text"))

PRODUCT = "master.product.product_id"
ZONE = "master.price_zone.zone_id"
STORE = "master.store.store_id"
PROMO = "pricing.promotion.promo_id"

_TABLES: dict[str, tuple[Column, ...]] = {
    "master.category": (_pk("category_id", "int"), Column("category_name", "text")),
    "master.product_class": (
        _pk("class_id", "int"),
        _fk("category_id", "int", "master.category.category_id"),
        Column("class_name", "text"),
    ),
    "master.brand": (
        _pk("brand_id", "int"),
        Column("brand_name", "text"),
        Column("brand_tier", "smallint"),  # 1 private label, 2 national, 3 premium
    ),
    "master.supplier": (_pk("supplier_id", "int"), Column("supplier_name", "text")),
    "master.product": (
        _pk("product_id", "bigint"),
        Column("product_code", "text"),
        Column("product_name", "text"),
        _fk("class_id", "int", "master.product_class.class_id"),
        _fk("brand_id", "int", "master.brand.brand_id", nullable=True),
        _fk("supplier_id", "int", "master.supplier.supplier_id", nullable=True),
        _null("size", "text"),
        Column("pack_units", "numeric(5,2)"),
        Column("is_active", "boolean"),
    ),
    "master.price_zone": (
        _pk("zone_id", "smallint"),
        Column("zone_code", "text"),
        Column("channel", "text"),  # STORE or WEB
        Column("price_factor", "numeric(5,4)"),
    ),
    "master.store": (
        _pk("store_id", "int"),
        Column("store_code", "text"),
        _null("store_name", "text"),
        _fk("zone_id", "smallint", ZONE),
        _null("city", "text"),
        _null("state", "text"),
        Column("price_adjust_pct", "numeric(5,4)"),
    ),
    "master.warehouse": (
        _pk("warehouse_id", "int"),
        Column("warehouse_code", "text"),
        _null("warehouse_name", "text"),
        _null("city", "text"),
        _null("state", "text"),
    ),
    "master.customer": (
        _pk("customer_id", "bigint"),
        Column("customer_code", "text"),
        _null("first_name", "text"),
        _null("last_name", "text"),
        _null("email", "text"),
        _null("city", "text"),
        _null("state", "text"),
        _null("zip", "text"),
    ),
    "rules.price_ending_band": (
        _pk("band_id", "smallint"),
        Column("min_price", "numeric(10,2)"),
        _null("max_price", "numeric(10,2)"),  # NULL = unbounded
        Column("ending", "numeric(3,2)"),
    ),
    "pricing.promotion": (
        _pk("promo_id", "int"),
        Column("promo_code", "text"),
        Column("start_date", "date"),
        Column("end_date", "date"),
        Column("discount_pct", "numeric(5,4)"),
        Column("channel", "text"),
    ),
    "sales.sale": (
        _pk("sale_id", "bigint"),
        Column("channel", "text"),  # STORE or WEB
        _fk("store_id", "int", STORE, nullable=True),
        _fk("customer_id", "bigint", "master.customer.customer_id", nullable=True),
        Column("business_date", "date"),
        Column("sold_at", "timestamptz"),
        Column("total_amount", "numeric(12,2)"),
    ),
    "sales.sale_line": (
        _pk("sale_line_id", "bigint"),
        _fk("sale_id", "bigint", "sales.sale.sale_id"),
        Column("line_no", "smallint"),
        _fk("product_id", "bigint", PRODUCT),
        Column("quantity", "int"),
        Column("unit_price", "numeric(10,2)"),
        Column("discount_amount", "numeric(12,2)"),
        Column("net_amount", "numeric(12,2)"),
        Column("unit_cost", "numeric(10,2)"),
        _fk("promo_id", "int", PROMO, nullable=True),
    ),
    "sales.sale_return": (
        _pk("return_id", "bigint"),
        _fk("sale_line_id", "bigint", "sales.sale_line.sale_line_id"),
        Column("quantity", "int"),
        Column("amount", "numeric(12,2)"),
        Column("business_date", "date"),
    ),
    "inventory.warehouse_stock": (
        _pk("warehouse_id", "int", "master.warehouse.warehouse_id"),
        _pk("product_id", "bigint", PRODUCT),
        Column("on_hand", "int"),
    ),
    "inventory.store_stock": (
        _pk("store_id", "int", STORE),
        _pk("product_id", "bigint", PRODUCT),
        Column("on_hand", "int"),
        Column("last_received_date", "date"),
    ),
    "inventory.stock_movement": (
        _pk("movement_id", "bigint"),
        _fk("store_id", "int", STORE),
        _fk("product_id", "bigint", PRODUCT),
        Column("movement_type", "text"),  # RECEIPT, SALE, RETURN, ADJUST
        Column("qty", "int"),
        Column("business_date", "date"),
    ),
    "rules.kvi": (
        _pk("product_id", "bigint", PRODUCT),
        Column("kvi_tier", "smallint"),
        Column("competitor_index_target", "numeric(4,3)"),
    ),
    "rules.category_rule": (
        _pk("category_id", "int", "master.category.category_id"),
        Column("target_margin", "numeric(5,4)"),
        Column("min_margin", "numeric(5,4)"),
        Column("max_change_pct", "numeric(5,4)"),
        Column("cover_min_days", "smallint"),
        Column("cover_max_days", "smallint"),
        Column("aging_max_days", "smallint"),
        Column("promo_policy", "text"),  # keep_depth or exclude
    ),
    "rules.ladder_group": (
        _pk("ladder_group_id", "int"),
        Column("ladder_type", "text"),  # PACK or TIER
        _fk("class_id", "int", "master.product_class.class_id"),
        _fk("brand_id", "int", "master.brand.brand_id", nullable=True),  # PACK only
        _null("size", "text"),  # TIER only
    ),
    "rules.ladder_member": (
        _pk("ladder_group_id", "int", "rules.ladder_group.ladder_group_id"),
        _pk("product_id", "bigint", PRODUCT),
        Column("rank", "smallint"),
    ),
    "pricing.zone_price": (
        _pk("product_id", "bigint", PRODUCT),
        _pk("zone_id", "smallint", ZONE),
        Column("price", "numeric(10,2)"),
        Column("effective_from", "date"),
        *OWNED,
    ),
    "pricing.store_price_override": (
        _pk("override_id", "bigint"),
        _fk("store_id", "int", STORE),
        _fk("product_id", "bigint", PRODUCT),
        Column("price", "numeric(10,2)"),
        Column("valid_from", "date"),
        _null("valid_to", "date"),
        *OWNED,
    ),
    "pricing.promo_item": (_pk("promo_id", "int", PROMO), _pk("product_id", "bigint", PRODUCT)),
    "pricing.promo_price": (
        _pk("promo_id", "int", PROMO),
        _pk("product_id", "bigint", PRODUCT),
        _pk("zone_id", "smallint", ZONE),
        Column("promo_price", "numeric(10,2)"),
        *OWNED,
    ),
    "pricing.supplier_cost": (
        _pk("product_id", "bigint", PRODUCT),
        _pk("valid_from", "date"),
        _fk("supplier_id", "int", "master.supplier.supplier_id", nullable=True),
        Column("unit_cost", "numeric(12,4)"),
    ),
    "pricing.price_history": (
        _pk("history_id", "bigint"),
        _fk("product_id", "bigint", PRODUCT),
        _fk("zone_id", "smallint", ZONE),
        Column("price", "numeric(10,2)"),
        Column("valid_from", "timestamptz"),
        _null("valid_to", "timestamptz"),
        Column("source", "text"),  # seed, engine, manual
        _null("cycle_id", "bigint"),
    ),
    "pricing.price_change": (
        _pk("change_id", "bigint"),
        Column("cycle_id", "bigint"),
        Column("target", "text"),
        _fk("product_id", "bigint", PRODUCT),
        _fk("zone_id", "smallint", ZONE, nullable=True),
        _fk("store_id", "int", STORE, nullable=True),
        _fk("promo_id", "int", PROMO, nullable=True),
        _null("old_price", "numeric(10,2)"),
        Column("new_price", "numeric(10,2)"),
        _null("reason_code", "text"),
        Column("applied_at", "timestamptz"),
    ),
    "pricing.shelf_label_queue": (
        _pk("label_id", "bigint"),
        Column("cycle_id", "bigint"),
        _fk("store_id", "int", STORE),
        _fk("product_id", "bigint", PRODUCT),
        Column("price", "numeric(10,2)"),
        _null("promo_price", "numeric(10,2)"),
        Column("due_date", "date"),
        Column("status", "text"),  # queued or printed
    ),
    "market.competitor": (
        _pk("competitor_id", "smallint"),
        Column("competitor_name", "text"),
        Column("price_bias", "numeric(5,4)"),
    ),
    "market.competitor_price": (
        _pk("observation_id", "bigint"),
        _fk("competitor_id", "smallint", "market.competitor.competitor_id"),
        _fk("product_id", "bigint", PRODUCT),
        _fk("zone_id", "smallint", ZONE),
        Column("observed_date", "date"),
        Column("price", "numeric(10,2)"),
    ),
    "ops.business_clock": (_pk("id", "smallint"), Column("business_date", "date")),
}

TABLES: dict[str, tuple[Column, ...]] = {name: (*cols, *STANDARD) for name, cols in _TABLES.items()}


def primary_key(table: str) -> tuple[str, ...]:
    return tuple(c.name for c in TABLES[table] if c.pk)
