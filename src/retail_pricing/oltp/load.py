"""`oltp load`: schema from the contract, COPY of the seed Parquet, then the rest (PLAN 4.1).

Phases: tables (PKs only), copy, constraints, indexes, triggers, apply, roles, sequences,
analyze. Each phase and each table logs one JSON line with rows and seconds. Loading into
non-empty tables is refused; `replace=True` drops the schemas first (a schema change means a
reset, not a migration).
"""

import io
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources import files
from pathlib import Path

import psycopg
import pyarrow.csv as pacsv  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from psycopg import sql

from retail_pricing.data.build import oltp_dir
from retail_pricing.data.contract import TABLES
from retail_pricing.oltp import ddl
from retail_pricing.settings import OltpSettings

log = logging.getLogger(__name__)

BATCH_ROWS = 65_536


class LoadError(RuntimeError):
    pass


def sql_file(name: str) -> str:
    return (files("retail_pricing") / "sql" / "oltp" / name).read_text(encoding="utf-8")


def owner_connect(settings: OltpSettings) -> psycopg.Connection:
    return psycopg.connect(settings.conninfo(settings.owner_user, settings.owner_password))


@contextmanager
def _phase(name: str) -> Iterator[None]:
    t0 = time.perf_counter()
    yield
    log.info("oltp load phase", extra={"phase": name, "s": round(time.perf_counter() - t0, 3)})


def _run(conn: psycopg.Connection, stmts: list[str]) -> None:
    with conn.cursor() as cur:
        for s in stmts:
            cur.execute(s.encode())
    conn.commit()


def non_empty_tables(conn: psycopg.Connection) -> list[str]:
    out = []
    with conn.cursor() as cur:
        for table in TABLES:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (table,))
            row = cur.fetchone()
            if row and row[0]:
                cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})".encode())
                row = cur.fetchone()
                if row and row[0]:
                    out.append(table)
    conn.rollback()
    return out


def copy_table(conn: psycopg.Connection, table: str, path: Path, into: str | None = None) -> int:
    """Stream Parquet record batches as CSV into COPY. Strings are always quoted by pyarrow,
    so an empty string stays '' and only a null becomes an unquoted empty field (NULL)."""
    cols = [c.name for c in TABLES[table]]
    pf = pq.ParquetFile(path)
    if pf.schema_arrow.names != cols:
        raise LoadError(f"{path.name} columns {pf.schema_arrow.names} differ from the contract")
    opts = pacsv.WriteOptions(include_header=False)
    rows = 0
    copy_sql = f"COPY {into or table} ({', '.join(cols)}) FROM STDIN (FORMAT csv)"
    with conn.cursor() as cur, cur.copy(copy_sql.encode()) as cp:
        for batch in pf.iter_batches(batch_size=BATCH_ROWS):
            buf = io.BytesIO()
            pacsv.write_csv(batch, buf, write_options=opts)
            cp.write(buf.getvalue())
            rows += batch.num_rows
    conn.commit()
    return rows


def create_roles(conn: psycopg.Connection, settings: OltpSettings) -> None:
    """Create or re-password the login roles, then apply the grants. Passwords travel only as
    SQL literals inside the statement, never in a log line."""
    roles = {
        "workload": (settings.workload_user, settings.workload_password),
        "nifi": (settings.nifi_user, settings.nifi_password),
        "exporter": (settings.exporter_user, settings.exporter_password),
    }
    with conn.cursor() as cur:
        for user, password in roles.values():
            if not password.get_secret_value():
                raise LoadError(f"no password configured for role {user}")
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
            verb = "ALTER" if cur.fetchone() else "CREATE"
            cur.execute(
                sql.SQL(verb + " ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(user), sql.Literal(password.get_secret_value())
                )
            )
        grants = sql.SQL(sql_file("060_roles.sql")).format(
            db=sql.Identifier(settings.db), **{k: sql.Identifier(u) for k, (u, _) in roles.items()}
        )
        cur.execute(grants)
    conn.commit()


def reset_sequences() -> list[str]:
    return [
        f"SELECT setval(pg_get_serial_sequence('{t}', '{col}'), coalesce(max({col}), 0) + 1, false)"
        f" FROM {t}"
        for t in TABLES
        if (col := ddl.identity_column(t))
    ]


def load(sf: float, data_dir: Path, settings: OltpSettings, *, replace: bool = False) -> int:
    """Load the seed of scale `sf`; returns the total number of rows copied."""
    src = oltp_dir(data_dir, sf)
    missing = [t for t in TABLES if not (src / f"{t.replace('.', '__')}.parquet").exists()]
    if missing:
        raise LoadError(f"seed files missing under {src}: {missing}; run `data all --sf {sf}`")
    t0, total = time.perf_counter(), 0
    with owner_connect(settings) as conn:
        if replace:
            with _phase("drop"):
                _run(conn, [f"DROP SCHEMA IF EXISTS {s} CASCADE" for s in ddl.SCHEMAS])
        elif busy := non_empty_tables(conn):
            raise LoadError(f"tables already hold rows ({', '.join(busy)}); use --replace")
        with _phase("tables"):
            _run(conn, ddl.tables())
        with _phase("copy"):
            for table in TABLES:
                t1 = time.perf_counter()
                rows = copy_table(conn, table, src / f"{table.replace('.', '__')}.parquet")
                total += rows
                s = round(time.perf_counter() - t1, 3)
                log.info("oltp load table", extra={"table": table, "rows": rows, "s": s})
        with _phase("constraints"):
            _run(conn, ddl.constraints())
        with _phase("indexes"):
            _run(conn, ddl.indexes())
        with _phase("triggers"):
            _run(conn, [sql_file("040_triggers.sql")])
        with _phase("apply"):
            _run(conn, [sql_file("050_apply.sql")])
        with _phase("roles"):
            create_roles(conn, settings)
        with _phase("sequences"):
            _run(conn, reset_sequences())
        conn.autocommit = True
        with _phase("analyze"):
            conn.execute(b"ANALYZE")
    log.info(
        "oltp loaded",
        extra={"sf": sf, "rows": total, "total_s": round(time.perf_counter() - t0, 2)},
    )
    return total


def status(settings: OltpSettings) -> dict[str, int | None]:
    """Row count per contract table; None when the table does not exist."""
    out: dict[str, int | None] = {}
    with owner_connect(settings) as conn, conn.cursor() as cur:
        for table in TABLES:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (table,))
            row = cur.fetchone()
            if row and row[0]:
                cur.execute(f"SELECT count(*) FROM {table}".encode())
                row = cur.fetchone()
                out[table] = int(row[0]) if row else 0
            else:
                out[table] = None
    return out
