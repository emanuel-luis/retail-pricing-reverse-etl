"""`control init`, the registry seed and the roles."""

from typing import Any

import psycopg
import pytest
from control_helpers import one

from retail_pricing.control.init import init
from retail_pricing.data.contract import EXTRACT_TABLES
from retail_pricing.settings import ControlSettings

pytestmark = pytest.mark.sql

Conn = psycopg.Connection[Any]


def connect(s: ControlSettings, user: str, password: Any) -> psycopg.Connection[Any]:
    return psycopg.connect(s.conninfo(user, password), autocommit=True)


def snapshot(conn: Conn) -> list[Any]:
    return [
        conn.execute("SELECT * FROM registry.job ORDER BY job_id").fetchall(),
        conn.execute("SELECT * FROM registry.extract_table ORDER BY load_order").fetchall(),
        conn.execute(
            "SELECT proname, md5(prosrc) FROM pg_proc p JOIN pg_namespace n"
            " ON n.oid = p.pronamespace WHERE nspname IN ('audit', 'registry')"
            " ORDER BY 1"
        ).fetchall(),
        conn.execute(
            "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE nspname IN ('audit', 'registry') ORDER BY 1"
        ).fetchall(),
    ]


def test_seed_covers_every_contract_table(db: Conn) -> None:
    rows = db.execute(
        "SELECT table_name, pk_column, select_list, load_order, page_rows, enabled"
        " FROM registry.extract_table ORDER BY load_order"
    ).fetchall()
    assert [r[0] for r in rows] == list(EXTRACT_TABLES)
    assert [r[3] for r in rows] == list(range(1, len(EXTRACT_TABLES) + 1))
    assert {r[4] for r in rows} == {250000}
    assert all(r[5] for r in rows)
    for name, pk, select_list, *_ in rows:
        assert select_list.split(", ") == [c.name for c in EXTRACT_TABLES[name]]
        assert pk in select_list.split(", ")
    jobs = [r[0] for r in db.execute("SELECT job_id FROM registry.job ORDER BY stage_order")]
    assert jobs == [
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
    assert one(db, "SELECT batch_rows FROM registry.job WHERE job_id = 'reverse.shelf_label'") == (
        5000,
    )


def test_init_twice_is_a_no_op(db: Conn, ctl_settings: ControlSettings) -> None:
    before = snapshot(db)
    init(ctl_settings)
    init(ctl_settings)
    assert snapshot(db) == before


def test_init_keeps_operator_settings(db: Conn, ctl_settings: ControlSettings) -> None:
    db.execute(
        "UPDATE registry.job SET enabled = false, nifi_process_group_id = 'pg-1'"
        " WHERE job_id = 'load'"
    )
    db.execute(
        "UPDATE registry.extract_table SET page_rows = 5000, enabled = false"
        " WHERE table_name = 'master.brand'"
    )
    init(ctl_settings)
    assert one(
        db, "SELECT enabled, nifi_process_group_id FROM registry.job WHERE job_id = 'load'"
    ) == (False, "pg-1")
    assert one(
        db,
        "SELECT page_rows, enabled FROM registry.extract_table WHERE table_name = 'master.brand'",
    ) == (5000, False)


def test_nifi_role_can_do_what_nifi_needs(db: Conn, ctl_settings: ControlSettings) -> None:
    s = ctl_settings
    with connect(s, s.nifi_user, s.nifi_password) as nifi:
        cycle_id, status = one(nifi, "SELECT * FROM registry.start_cycle('full', 'nifi', NULL)")
        assert status == "started"
        run_id, _ = one(nifi, "SELECT * FROM audit.start_run(%s, 'extract')", cycle_id)
        nifi.execute(
            "INSERT INTO audit.step_event (run_id, step_id, step_name, item, event)"
            " VALUES (%s, gen_random_uuid(), 'x', 'i', 'started')",
            (run_id,),
        )
        assert one(nifi, "SELECT * FROM audit.end_run(%s)", run_id)[0] == "failed"  # open step
        nifi.execute(
            "INSERT INTO audit.reconcile_expected VALUES (%s, 't', 0, 0, 'c', 1, 1, 1)"
            " ON CONFLICT (cycle_id, target, zone_id, category_id, check_name)"
            " DO UPDATE SET rows = excluded.rows",
            (cycle_id,),
        )
        nifi.execute(
            "INSERT INTO audit.reverse_batch (cycle_id, target, batch_no, run_id)"
            " VALUES (%s, 't', 1, %s) ON CONFLICT (cycle_id, target, batch_no)"
            " DO UPDATE SET sent = 1",
            (cycle_id, run_id),
        )
        nifi.execute("SELECT * FROM audit.finish_cycle(%s, 'failed', 'extract', 'x')", (cycle_id,))
    assert one(db, "SELECT count(*) FROM audit.step")[0] == 1  # written by the trigger


@pytest.mark.parametrize(
    "statement",
    [
        "DROP TABLE audit.cycle",
        "DROP TABLE registry.job",
        "CREATE TABLE registry.rogue (x int)",
        "CREATE TABLE public.rogue (x int)",
        "TRUNCATE audit.step_event",
        "UPDATE audit.cycle SET status = 'succeeded'",
        "DELETE FROM registry.watermark",
        (
            "INSERT INTO registry.job (job_id, process_group_name, owner, sla, stage_order)"
            " VALUES ('x', 'x', 'x', interval '1 minute', 99)"
        ),
        "UPDATE audit.step SET status = 'failed'",
        "INSERT INTO audit.alert (kind, message) VALUES ('x', 'x')",
        "SELECT * FROM audit.cycle",
    ],
)
def test_nifi_role_cannot_touch_anything_else(
    ctl_settings: ControlSettings, statement: str
) -> None:
    s = ctl_settings
    with (
        connect(s, s.nifi_user, s.nifi_password) as nifi,
        pytest.raises(psycopg.errors.InsufficientPrivilege),
    ):
        nifi.execute(statement)  # type: ignore[call-overload]


def test_grafana_role_reads_audit_and_registry_only(
    db: Conn, ctl_settings: ControlSettings
) -> None:
    s = ctl_settings
    with connect(s, s.grafana_user, s.grafana_password) as grafana:
        for view in ("v_cycle_stage_duration", "v_rows_per_stage", "v_reverse_totals", "v_sla"):
            grafana.execute(f"SELECT * FROM audit.{view}")
        grafana.execute("SELECT * FROM registry.job")
        grafana.execute("SELECT * FROM audit.alert")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            grafana.execute("DELETE FROM audit.alert")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            grafana.execute("SELECT * FROM registry.start_cycle('full', 'x', NULL)")


def test_exporter_role_is_pg_monitor(ctl_settings: ControlSettings) -> None:
    s = ctl_settings
    with connect(s, s.exporter_user, s.exporter_password) as exporter:
        row = one(exporter, "SELECT pg_has_role('pg_monitor', 'member')")
        assert row == (True,)
        exporter.execute("SELECT count(*) FROM pg_stat_activity")


def test_definer_functions_are_not_owned_by_a_superuser(db: Conn) -> None:
    rows = db.execute(
        "SELECT p.oid::regprocedure::text, r.rolname, r.rolsuper, p.proconfig::text"
        " FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace"
        " JOIN pg_roles r ON r.oid = p.proowner"
        " WHERE n.nspname IN ('audit', 'registry') AND p.prosecdef"
    ).fetchall()
    assert len(rows) >= 11  # ten NiFi functions and the step trigger
    for name, owner, superuser, config in rows:
        assert (owner, superuser) == ("audit_writer", False), name
        assert "search_path" in config, name
    assert one(db, "SELECT rolsuper, rolcanlogin FROM pg_roles WHERE rolname = 'audit_writer'") == (
        False,
        False,
    )


@pytest.mark.parametrize(
    "statement",
    [
        "DROP TABLE audit.cycle",
        "TRUNCATE audit.step",
        "CREATE TABLE audit.rogue (x int)",
        "DELETE FROM audit.cycle",
    ],
)
def test_audit_writer_cannot_drop_truncate_or_create(db: Conn, statement: str) -> None:
    db.execute("SET ROLE audit_writer")
    try:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            db.execute(statement)  # type: ignore[call-overload]
    finally:
        db.execute("RESET ROLE")
