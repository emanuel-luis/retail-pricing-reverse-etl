"""`ch load-raw` (dev and CI only): the statement shape NiFi sends with s3(), from the bind mount.

`file()` reads under ClickHouse's user_files_path, which compose mounts from ./data, so the path
is `oltp/sf=<N>/<schema>__<table>.parquet` relative to the data directory.
"""

import logging
import time
from pathlib import Path

from retail_pricing.clickhouse import ddl
from retail_pricing.clickhouse.client import connect
from retail_pricing.data.build import oltp_dir
from retail_pricing.data.contract import TABLES
from retail_pricing.data.manifest import read_manifest, sf_label
from retail_pricing.settings import ClickHouseSettings

log = logging.getLogger(__name__)


class LoadRawError(RuntimeError):
    pass


def load_raw(sf: float, cycle: int, data_dir: Path, settings: ClickHouseSettings) -> int:
    """Insert every contract table as cycle `cycle`; return the rows inserted."""
    manifest = read_manifest(oltp_dir(data_dir, sf) / "manifest.json")
    if manifest is None:
        raise LoadRawError(f"no OLTP seed manifest for SF{sf:g}; run `data all --sf {sf:g}`")
    total = 0
    with connect(settings) as ch:
        for table in TABLES:
            raw = ddl.raw_name(table)
            expected = int(manifest["tables"][table]["rows"])
            count = f"SELECT count() FROM {raw} WHERE _cycle_id = {cycle}"
            before = int(ch.scalar(count) or 0)
            path = f"oltp/sf={sf_label(sf)}/{table.replace('.', '__')}.parquet"
            t0 = time.perf_counter()
            # use_hive_partitioning = 0: the sf=<N> folder must not become a column (DECISIONS 33)
            ch.command(
                f"INSERT INTO {raw} SELECT *, {cycle}, now64(3) FROM file('{path}', Parquet)",
                use_hive_partitioning=0,
            )
            seconds = round(time.perf_counter() - t0, 3)
            rows = int(ch.scalar(count) or 0) - before
            log.info("ch load-raw table", extra={"table": raw, "rows": rows, "s": seconds})
            if rows != expected:
                raise LoadRawError(f"{raw}: inserted {rows} rows, manifest says {expected}")
            total += rows
    return total
