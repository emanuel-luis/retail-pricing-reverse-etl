"""`retail-pricing control init` and `status`: build the control database, idempotently."""

import logging
from importlib.resources import files

import psycopg
from psycopg import sql

from retail_pricing.control.seed import JOBS, extract_tables
from retail_pricing.settings import ControlSettings

log = logging.getLogger(__name__)


def _step(name: str, **extra: object) -> None:
    log.info("control init step", extra={"step": name, **extra})


def create_roles(conn: psycopg.Connection, s: ControlSettings) -> None:
    """Create or update the login roles; the passwords never reach a log line."""
    for user, password in (
        (s.nifi_user, s.nifi_password),
        (s.grafana_user, s.grafana_password),
        (s.exporter_user, s.exporter_password),
    ):
        secret = password.get_secret_value()
        if not secret:
            raise ValueError(f"the password of role {user} is empty; set it in the environment")
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,)).fetchone()
        verb = "ALTER" if exists else "CREATE"
        conn.execute(
            sql.SQL("{} ROLE {} LOGIN PASSWORD {}").format(
                sql.SQL(verb), sql.Identifier(user), sql.Literal(secret)
            )
        )
        conn.execute(sql.SQL("ALTER ROLE {} SET timezone TO 'UTC'").format(sql.Identifier(user)))
        _step("role", role=user, action=verb.lower())


def apply_sql_files(conn: psycopg.Connection) -> None:
    """Run sql/control/*.sql in name order; each file is one transaction."""
    folder = files("retail_pricing") / "sql" / "control"
    for path in sorted(
        (p for p in folder.iterdir() if p.name.endswith(".sql")), key=lambda p: p.name
    ):
        conn.execute(path.read_text(encoding="utf-8"))
        _step("sql", file=path.name)


def seed_registry(conn: psycopg.Connection) -> None:
    """Upsert jobs and extract tables. Structural columns follow the code; enabled, page_rows,
    sla, owner, schedule and nifi_process_group_id are kept once set."""
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO registry.job (job_id, process_group_name, source, target, owner, sla,
                                         stage_order, schedule_interval, batch_rows)
               VALUES (%s, %s, %s, %s, %s, %s::interval, %s, %s::interval, %s)
               ON CONFLICT (job_id) DO UPDATE SET
                   process_group_name = excluded.process_group_name, source = excluded.source,
                   target = excluded.target, stage_order = excluded.stage_order,
                   batch_rows = excluded.batch_rows""",
            [
                (
                    j.job_id,
                    j.process_group_name,
                    j.source,
                    j.target,
                    j.owner,
                    j.sla,
                    j.stage_order,
                    j.schedule_interval,
                    j.batch_rows,
                )
                for j in JOBS
            ],
        )
        _step("seed_jobs", rows=len(JOBS))
        tables = extract_tables()
        cur.executemany(
            """INSERT INTO registry.extract_table (table_name, pk_column, select_list, load_order)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (table_name) DO UPDATE SET
                   pk_column = excluded.pk_column, select_list = excluded.select_list,
                   load_order = excluded.load_order""",
            [(t.table_name, t.pk_column, t.select_list, t.load_order) for t in tables],
        )
        _step("seed_extract_tables", rows=len(tables))


def init(settings: ControlSettings) -> None:
    """Roles, then the SQL files, then the seed. Safe to run any number of times."""
    conninfo = settings.conninfo(settings.owner_user, settings.owner_password)
    with psycopg.connect(conninfo, autocommit=True) as conn:
        create_roles(conn, settings)
        apply_sql_files(conn)
        with conn.transaction():
            seed_registry(conn)
    _step("done")


def status(settings: ControlSettings, limit: int = 5) -> list[str]:
    """Lines describing the latest cycles and their runs."""
    conninfo = settings.conninfo(settings.owner_user, settings.owner_password)
    lines: list[str] = []
    with psycopg.connect(conninfo) as conn:
        cycles = conn.execute(
            "SELECT cycle_id, mode, status, failed_stage, started_at FROM audit.cycle"
            " ORDER BY cycle_id DESC LIMIT %s",
            (limit,),
        ).fetchall()
        for cycle_id, mode, st, failed_stage, started_at in cycles:
            stage = f" failed_stage={failed_stage}" if failed_stage else ""
            lines.append(
                f"cycle {cycle_id} {mode} {st}{stage} started {started_at:%Y-%m-%d %H:%M:%S}Z"
            )
            runs = conn.execute(
                "SELECT job_id, status, rows_out FROM audit.v_latest_run r"
                " JOIN registry.job j USING (job_id) WHERE r.cycle_id = %s ORDER BY j.stage_order",
                (cycle_id,),
            ).fetchall()
            lines += [
                f"  {job:<24} {st2:<10} rows={rows if rows is not None else '-'}"
                for job, st2, rows in runs
            ]
    return lines or ["no cycles yet"]
