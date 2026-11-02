# ruff: noqa: F811
"""The registry and audit functions NiFi calls."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from control_fixtures import ctl_settings, db, one  # noqa: F401
from psycopg.errors import RaiseException

pytestmark = pytest.mark.sql

Conn = psycopg.Connection[Any]
HIGH = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def start(db: Conn, mode: str = "incremental") -> int:
    cycle_id, status = one(db, "SELECT * FROM registry.start_cycle(%s, 'test', NULL)", mode)
    assert status == "started"
    return int(cycle_id)


def run(db: Conn, cycle_id: int, job: str) -> uuid.UUID:
    run_id, enabled = one(db, "SELECT * FROM audit.start_run(%s, %s)", cycle_id, job)
    assert enabled
    return run_id  # type: ignore[no-any-return]


def event(db: Conn, run_id: uuid.UUID, step_id: uuid.UUID, kind: str, **kw: Any) -> None:
    db.execute(
        "INSERT INTO audit.step_event (run_id, step_id, step_name, item, event, rows, bytes,"
        " detail) VALUES (%s, %s, 'item', %s, %s, %s, %s, %s::jsonb)",
        (
            run_id,
            step_id,
            kw.get("item", "t#0-0"),
            kind,
            kw.get("rows"),
            kw.get("bytes"),
            kw.get("detail"),
        ),
    )


def step_row(db: Conn, step_id: uuid.UUID) -> tuple[Any, ...]:
    return one(
        db,
        "SELECT status, attempts, rows, error, ended_at FROM audit.step WHERE step_id = %s",
        step_id,
    )


# start_cycle ------------------------------------------------------------------------------


def test_start_cycle_busy_while_running(db: Conn) -> None:
    first = start(db)
    assert one(db, "SELECT * FROM registry.start_cycle('full', 'test', NULL)") == (first, "busy")
    assert one(db, "SELECT count(*) FROM audit.cycle")[0] == 1


def test_start_cycle_rejects_bad_mode(db: Conn) -> None:
    with pytest.raises(RaiseException):
        db.execute("SELECT * FROM registry.start_cycle('weekly', 'test', NULL)")


def test_rerun_resets_a_failed_cycle(db: Conn) -> None:
    cycle_id = start(db)
    db.execute("SELECT audit.finish_cycle(%s, 'failed', 'load', 'boom')", (cycle_id,))
    assert one(db, "SELECT * FROM registry.start_cycle('full', 'test', %s)", cycle_id) == (
        cycle_id,
        "rerun",
    )
    status, stage, error, ended, attempt = one(
        db, "SELECT status, failed_stage, error, ended_at, attempt FROM audit.cycle"
    )
    assert (status, stage, error, ended, attempt) == ("running", None, None, None, 2)


def test_rerun_refuses_a_succeeded_cycle(db: Conn) -> None:
    cycle_id = start(db)
    db.execute("SELECT audit.finish_cycle(%s, 'succeeded')", (cycle_id,))
    with pytest.raises(RaiseException):
        db.execute("SELECT * FROM registry.start_cycle('full', 'test', %s)", (cycle_id,))


def test_start_cycle_if_due(db: Conn) -> None:
    assert db.execute("SELECT * FROM registry.start_cycle_if_due()").fetchall() == [(1,)]
    assert db.execute("SELECT * FROM registry.start_cycle_if_due()").fetchall() == []  # running
    db.execute("SELECT audit.finish_cycle(1, 'succeeded')")
    assert db.execute("SELECT * FROM registry.start_cycle_if_due()").fetchall() == []  # not due
    db.execute("UPDATE audit.cycle SET started_at = now() - interval '11 minutes'")
    assert db.execute("SELECT * FROM registry.start_cycle_if_due()").fetchall() == [(2,)]


def test_start_cycle_if_due_respects_enabled_and_schedule(db: Conn) -> None:
    db.execute("UPDATE registry.job SET enabled = false WHERE job_id = 'cycle'")
    assert db.execute("SELECT * FROM registry.start_cycle_if_due()").fetchall() == []
    db.execute(
        "UPDATE registry.job SET enabled = true, schedule_interval = NULL WHERE job_id = 'cycle'"
    )
    assert db.execute("SELECT * FROM registry.start_cycle_if_due()").fetchall() == []


# start_run, stage_plan -------------------------------------------------------------------


def test_start_run_skips_a_disabled_job(db: Conn) -> None:
    cycle_id = start(db)
    db.execute("UPDATE registry.job SET enabled = false WHERE job_id = 'reverse.shelf_label'")
    run_id, enabled = one(db, "SELECT * FROM audit.start_run(%s, 'reverse.shelf_label')", cycle_id)
    assert enabled is False
    assert one(
        db, "SELECT status, ended_at IS NOT NULL FROM audit.job_run WHERE run_id = %s", run_id
    ) == ("skipped", True)
    assert (
        db.execute(
            "SELECT * FROM audit.stage_plan(%s, 'reverse.shelf_label')", (cycle_id,)
        ).fetchall()
        == []
    )
    assert one(db, "SELECT status FROM audit.end_run(%s)", run_id) == ("skipped",)


def test_stage_plan(db: Conn) -> None:
    cycle_id = start(db)
    assert db.execute(
        "SELECT * FROM audit.stage_plan(%s, 'transform')", (cycle_id,)
    ).fetchall() == [("transform", None)]
    targets = [
        r[0] for r in db.execute("SELECT * FROM audit.stage_plan(%s, 'reconcile')", (cycle_id,))
    ]
    assert targets == ["zone_price", "store_override", "promo_price", "shelf_label"]


# extract_plan -----------------------------------------------------------------------------


def plan(db: Conn, cycle_id: int, wm_high: datetime = HIGH, mode: str | None = None) -> list[Any]:
    return db.execute(
        "SELECT * FROM registry.extract_plan(%s, %s, DATE '2025-12-04', %s)",
        (cycle_id, wm_high, mode),
    ).fetchall()


def test_extract_plan_full_without_watermark_and_sets_business_date(db: Conn) -> None:
    cycle_id = start(db)
    rows = plan(db, cycle_id)
    assert len(rows) == one(db, "SELECT count(*) FROM registry.extract_table")[0]
    assert {r[1] for r in rows} == {"full"}
    assert {r[2] for r in rows} == {None}
    assert [r[7] for r in rows] == sorted(r[7] for r in rows)
    assert str(one(db, "SELECT business_date FROM audit.cycle")[0]) == "2025-12-04"


def test_extract_plan_incremental_uses_the_overlap(db: Conn) -> None:
    db.execute(
        "INSERT INTO registry.watermark (table_name, high_value) VALUES ('master.category', %s)",
        (HIGH,),
    )
    cycle_id = start(db)
    later = HIGH + timedelta(hours=1)
    by_table = {r[0]: r for r in plan(db, cycle_id, later)}
    assert by_table["master.category"][1:4] == ("incremental", HIGH - timedelta(minutes=2), later)
    assert by_table["master.brand"][1] == "full"  # no watermark yet


def test_extract_plan_full_mode_ignores_watermarks(db: Conn) -> None:
    db.execute(
        "INSERT INTO registry.watermark (table_name, high_value) VALUES ('master.category', %s)",
        (HIGH,),
    )
    cycle_id = start(db, "full")
    assert {r[1] for r in plan(db, cycle_id)} == {"full"}


def test_extract_plan_is_idempotent_per_cycle(db: Conn) -> None:
    cycle_id = start(db)
    first = plan(db, cycle_id)
    db.execute(
        "UPDATE registry.extract_table SET enabled = false WHERE table_name = 'ops.business_clock'"
    )
    again = plan(db, cycle_id, HIGH + timedelta(days=1))
    assert again == first


# step_event trigger ----------------------------------------------------------------------


def test_step_trigger_tracks_state_and_attempts(db: Conn) -> None:
    run_id = run(db, start(db), "extract")
    step = uuid.uuid4()
    event(db, run_id, step, "started")
    assert step_row(db, step)[:2] == ("running", 1)
    event(db, run_id, step, "retried", detail='{"error": "timeout"}')
    event(db, run_id, step, "retried")
    assert step_row(db, step)[:2] == ("running", 3)
    assert step_row(db, step)[3] == "timeout"
    event(db, run_id, step, "succeeded", rows=10, bytes=99)
    status, attempts, rows, error, ended = step_row(db, step)
    assert (status, attempts, rows, error) == ("succeeded", 3, 10, None) and ended is not None
    other = uuid.uuid4()
    event(db, run_id, other, "started")
    event(db, run_id, other, "failed", detail='{"message": "bad"}')
    assert step_row(db, other)[0::3] == ("failed", "bad")


def test_step_event_is_append_only(db: Conn) -> None:
    run_id = run(db, start(db), "extract")
    event(db, run_id, uuid.uuid4(), "started")
    with pytest.raises(psycopg.errors.RestrictViolation):
        db.execute("UPDATE audit.step_event SET rows = 1")
    with pytest.raises(psycopg.errors.RestrictViolation):
        db.execute("DELETE FROM audit.step_event")


# end_run, load_plan ---------------------------------------------------------------------


def test_end_run_succeeds_and_sums_rows(db: Conn) -> None:
    run_id = run(db, start(db), "extract")
    for rows in (5, 7):
        step = uuid.uuid4()
        event(db, run_id, step, "started")
        event(db, run_id, step, "succeeded", rows=rows, bytes=100)
    assert one(db, "SELECT * FROM audit.end_run(%s)", run_id) == ("succeeded", 0, 0)
    assert one(db, "SELECT status, rows_out, bytes FROM audit.job_run") == ("succeeded", 12, 200)
    assert one(db, "SELECT * FROM audit.end_run(%s)", run_id) == ("succeeded", 0, 0)  # twice
    assert one(db, "SELECT count(*) FROM audit.alert")[0] == 0


def test_end_run_fails_on_failed_step_and_alerts_once(db: Conn) -> None:
    cycle_id = start(db)
    run_id = run(db, cycle_id, "load")
    ok, bad = uuid.uuid4(), uuid.uuid4()
    event(db, run_id, ok, "started")
    event(db, run_id, ok, "succeeded", rows=1)
    event(db, run_id, bad, "started", item="raw.t")
    event(db, run_id, bad, "failed", item="raw.t", detail='{"error": "count mismatch"}')
    assert one(db, "SELECT * FROM audit.end_run(%s)", run_id) == ("failed", 1, 0)
    db.execute("SELECT * FROM audit.end_run(%s)", (run_id,))
    kind, severity, message, alert_cycle = one(
        db, "SELECT kind, severity, message, cycle_id FROM audit.alert"
    )
    assert (kind, severity, alert_cycle) == ("stage_failed", "error", cycle_id)
    assert "count mismatch" in message
    assert "count mismatch" in one(db, "SELECT error FROM audit.job_run")[0]


def test_end_run_fails_on_an_open_step(db: Conn) -> None:
    run_id = run(db, start(db), "extract")
    event(db, run_id, uuid.uuid4(), "started")
    assert one(db, "SELECT * FROM audit.end_run(%s)", run_id) == ("failed", 0, 1)
    assert one(db, "SELECT kind FROM audit.alert")[0] == "stage_failed"


def test_load_plan_counts_only_tables_with_rows(db: Conn) -> None:
    cycle_id = start(db)
    run_id = run(db, cycle_id, "extract")
    for item, rows in (
        ("master.brand#0-0", 4),
        ("master.brand#0-1", 6),
        ("master.category#0-0", 3),
        ("master.supplier#0-0", 0),
    ):
        step = uuid.uuid4()
        event(db, run_id, step, "started", item=item)
        event(db, run_id, step, "succeeded", item=item, rows=rows)
    got = db.execute("SELECT * FROM audit.load_plan(%s, %s)", (cycle_id, run_id)).fetchall()
    assert [(r[0], r[2], r[3]) for r in got] == [("master.category", 3, 1), ("master.brand", 10, 2)]
    assert got[0][1] == f"raw/master.category/cycle={cycle_id}/run={run_id}/"


# commit_watermarks -----------------------------------------------------------------------


def finish_load(db: Conn, cycle_id: int) -> None:
    run_id = run(db, cycle_id, "load")
    db.execute("SELECT * FROM audit.end_run(%s)", (run_id,))


def test_commit_watermarks_requires_a_succeeded_load(db: Conn) -> None:
    cycle_id = start(db)
    plan(db, cycle_id)
    with pytest.raises(RaiseException):
        db.execute("SELECT * FROM registry.commit_watermarks(%s)", (cycle_id,))
    assert one(db, "SELECT count(*) FROM registry.watermark")[0] == 0


def test_commit_watermarks_only_advances(db: Conn) -> None:
    first = start(db)
    plan(db, first, HIGH)
    finish_load(db, first)
    db.execute("SELECT * FROM registry.commit_watermarks(%s)", (first,))
    db.execute("SELECT * FROM registry.commit_watermarks(%s)", (first,))  # twice
    assert one(db, "SELECT DISTINCT high_value FROM registry.watermark")[0] == HIGH
    db.execute("SELECT audit.finish_cycle(%s, 'succeeded')", (first,))
    second = start(db)
    plan(db, second, HIGH - timedelta(hours=1))  # clock went backwards
    finish_load(db, second)
    db.execute("SELECT * FROM registry.commit_watermarks(%s)", (second,))
    assert one(db, "SELECT DISTINCT high_value, cycle_id FROM registry.watermark") == (HIGH, first)
    db.execute("SELECT audit.finish_cycle(%s, 'succeeded')", (second,))
    third = start(db)
    plan(db, third, HIGH + timedelta(hours=1))
    finish_load(db, third)
    db.execute("SELECT * FROM registry.commit_watermarks(%s)", (third,))
    assert one(db, "SELECT DISTINCT high_value, cycle_id FROM registry.watermark") == (
        HIGH + timedelta(hours=1),
        third,
    )


# reconcile -------------------------------------------------------------------------------


def put_check(db: Conn, table: str, cycle_id: int, wsum: int, rows: int = 10) -> None:
    db.execute(
        f"INSERT INTO audit.reconcile_{table} (cycle_id, target, zone_id, check_name, rows,"
        " sum_cents, wsum) VALUES (%s, 'zone_price', 3, 'price', %s, 500, %s)",
        (cycle_id, rows, wsum),
    )


def test_reconcile_passes_on_equal_checksums(db: Conn) -> None:
    cycle_id = start(db)
    put_check(db, "expected", cycle_id, 77)
    put_check(db, "actual", cycle_id, 77)
    assert one(db, "SELECT * FROM audit.reconcile(%s)", cycle_id) == (True, 0)
    assert one(db, "SELECT passed FROM audit.reconcile_result")[0] is True
    assert one(db, "SELECT count(*) FROM audit.alert")[0] == 0


def test_reconcile_fails_on_different_or_missing_checks(db: Conn) -> None:
    cycle_id = start(db)
    put_check(db, "expected", cycle_id, 77)
    put_check(db, "actual", cycle_id, 78)
    db.execute(
        "INSERT INTO audit.reconcile_expected VALUES (%s, 'shelf_label', 0, 0, 'rows', 1, 1, 1)",
        (cycle_id,),
    )  # no actual side
    assert one(db, "SELECT * FROM audit.reconcile(%s)", cycle_id) == (False, 2)
    db.execute("SELECT * FROM audit.reconcile(%s)", (cycle_id,))  # twice: one alert
    assert one(db, "SELECT count(*), min(kind) FROM audit.alert") == (1, "reconcile_failed")
    # fix the data and rerun: passes and resolves the alert
    db.execute("DELETE FROM audit.reconcile_expected WHERE target = 'shelf_label'")
    db.execute("UPDATE audit.reconcile_actual SET wsum = 77")
    assert one(db, "SELECT * FROM audit.reconcile(%s)", cycle_id) == (True, 0)
    assert one(db, "SELECT resolved_at IS NOT NULL FROM audit.alert")[0] is True


# finish_cycle ----------------------------------------------------------------------------


def test_finish_cycle(db: Conn) -> None:
    cycle_id = start(db)
    run_id = run(db, cycle_id, "load")
    assert one(db, "SELECT * FROM audit.finish_cycle(%s, 'failed', 'load', 'boom')", cycle_id) == (
        cycle_id,
        "failed",
    )
    assert one(db, "SELECT status, failed_stage, error, ended_at IS NOT NULL FROM audit.cycle") == (
        "failed",
        "load",
        "boom",
        True,
    )
    assert one(db, "SELECT status FROM audit.job_run WHERE run_id = %s", run_id) == ("failed",)
    # a second call changes nothing
    assert one(db, "SELECT * FROM audit.finish_cycle(%s, 'succeeded')", cycle_id) == (
        cycle_id,
        "failed",
    )
    with pytest.raises(RaiseException):
        db.execute("SELECT * FROM audit.finish_cycle(%s, 'done')", (cycle_id,))


def test_views_answer(db: Conn) -> None:
    cycle_id = start(db)
    run_id = run(db, cycle_id, "extract")
    step = uuid.uuid4()
    event(db, run_id, step, "started")
    event(db, run_id, step, "succeeded", rows=3)
    db.execute("SELECT * FROM audit.end_run(%s)", (run_id,))
    db.execute(
        "INSERT INTO audit.reverse_batch (cycle_id, target, batch_no, sent, applied)"
        " VALUES (%s, 'zone_price', 1, 10, 9)",
        (cycle_id,),
    )
    for view in ("v_cycle_stage_duration", "v_rows_per_stage", "v_reverse_totals", "v_sla"):
        assert db.execute(f"SELECT * FROM audit.{view}").fetchall(), view
