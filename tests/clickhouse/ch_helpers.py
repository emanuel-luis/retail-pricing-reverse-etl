from pathlib import Path

from retail_pricing.clickhouse import ddl
from retail_pricing.clickhouse.client import connect
from retail_pricing.clickhouse.init import init
from retail_pricing.clickhouse.load_raw import load_raw
from retail_pricing.data.contract import EXTRACT_TABLES
from retail_pricing.settings import ClickHouseSettings

SF = 0.01


def reset_raw(settings: ClickHouseSettings, data_dir: Path, *cycles: int) -> None:
    """Truncate every raw table, then load the seed once per cycle."""
    init(settings)
    with connect(settings) as ch:
        for t in EXTRACT_TABLES:
            ch.command(f"TRUNCATE TABLE {ddl.raw_name(t)}")
    for cycle in cycles:
        load_raw(SF, cycle, data_dir, settings)
