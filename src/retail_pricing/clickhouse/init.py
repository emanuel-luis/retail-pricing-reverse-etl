"""`ch init`: databases from sql/clickhouse/*.sql, then the raw tables. Safe to run twice."""

import logging
from importlib.resources import files

from retail_pricing.clickhouse import ddl
from retail_pricing.clickhouse.client import connect
from retail_pricing.settings import ClickHouseSettings

log = logging.getLogger(__name__)


def sql_statements() -> list[str]:
    """Statements of every sql/clickhouse/*.sql in name order (no semicolons inside them)."""
    out: list[str] = []
    for f in sorted(
        (files("retail_pricing") / "sql" / "clickhouse").iterdir(), key=lambda p: p.name
    ):
        if f.name.endswith(".sql"):
            lines = f.read_text(encoding="utf-8").splitlines()
            text = "\n".join(ln for ln in lines if not ln.startswith("--"))
            out += [s.strip() for s in text.split(";") if s.strip()]
    return out


def init(settings: ClickHouseSettings) -> None:
    with connect(settings) as ch:
        for stmt in sql_statements():
            ch.command(stmt)
        for stmt in ddl.tables():
            ch.command(stmt)
    log.info("clickhouse init done", extra={"tables": len(ddl.tables())})
