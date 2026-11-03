"""Full and incremental cycles through the deployed flow (marker `integration`, PR 4 scope).

Needs the core stack with the OLTP loaded (SF0.01), `control init`, `ch init`, `s3 init` and
`retail-pricing nifi deploy --env local|ci`. The raw tables are truncated first, and the cycle
schedule is switched off while the module runs so only the tests start cycles.
"""

import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import psycopg
import pytest
from psycopg import sql
from typer.testing import CliRunner

from retail_pricing.cli import app
from retail_pricing.clickhouse import ddl
from retail_pricing.clickhouse.client import connect as ch_connect
from retail_pricing.cycle import SUCCEEDED, post_trigger, wait_cycle
from retail_pricing.settings import ClickHouseSettings, ControlSettings, OltpSettings

pytestmark = pytest.mark.integration

META = {name for name, _ in ddl.META_COLUMNS}


def control() -> psycopg.Connection[Any]:
    s = ControlSettings()
    return psycopg.connect(s.conninfo(s.owner_user, s.owner_password), autocommit=True)


def oltp() -> psycopg.Connection[Any]:
    s = OltpSettings()
    return psycopg.connect(s.conninfo(s.owner_user, s.owner_password), autocommit=True)


def tables() -> list[str]:
    with control() as c:
        return [
            r[0]
            for r in c.execute(
                "SELECT table_name FROM registry.extract_table WHERE enabled ORDER BY load_order"
            )
        ]


def step_rows(cycle_id: int, step_name: str) -> dict[str, int]:
    """Rows of the succeeded steps of a cycle per table (item before '#')."""
    with control() as c:
        rows = c.execute(
            "SELECT split_part(s.item, '#', 1), sum(s.rows)::bigint FROM audit.step s"
            " JOIN audit.job_run r USING (run_id) WHERE r.cycle_id = %s AND s.step_name = %s"
            " AND s.status = 'succeeded' GROUP BY 1",
            (cycle_id, step_name),
        ).fetchall()
    return {t: n for t, n in rows if n}


def oltp_counts(window: dict[str, tuple[datetime, datetime]] | None = None) -> dict[str, int]:
    out = {}
    with oltp() as o:
        for t in tables():
            q = sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(*t.split(".")))
            if window is None:
                n = o.execute(q).fetchone()
            else:
                q += sql.SQL(" WHERE updated_at > %s AND updated_at <= %s")
                n = o.execute(q, window[t]).fetchone()
            assert n is not None
            if n[0]:
                out[t] = n[0]
    return out


def raw_final_counts() -> dict[str, int]:
    with ch_connect(ClickHouseSettings()) as ch:
        return {
            t: int(n)
            for t in tables()
            if (n := ch.rows(f"SELECT count() FROM {ddl.raw_name(t)} FINAL")[0][0]) != "0"
        }


def run_cycle(mode: str) -> int:
    status, body = post_trigger(mode, None)
    assert status == 202, body
    cycle_id = int(body["cycle_id"])
    assert wait_cycle(cycle_id, 900, echo=print) == SUCCEEDED
    return cycle_id


@pytest.fixture(scope="module", autouse=True)
def quiet_schedule() -> Iterator[None]:
    """No scheduled cycles during the module; wait for one a fresh deploy may have started."""
    with control() as c:
        c.execute("UPDATE registry.job SET schedule_interval = NULL WHERE job_id = 'cycle'")
        deadline = time.monotonic() + 900
        while c.execute("SELECT 1 FROM audit.cycle WHERE status = 'running'").fetchone():
            assert time.monotonic() < deadline, "a cycle is still running"
            time.sleep(5)
    yield
    with control() as c:
        c.execute(
            "UPDATE registry.job SET schedule_interval = interval '10 minutes'"
            " WHERE job_id = 'cycle'"
        )


@pytest.fixture(scope="module")
def full() -> int:
    with ch_connect(ClickHouseSettings()) as ch:
        for t in tables():
            ch.command(f"TRUNCATE TABLE {ddl.raw_name(t)}")
    return run_cycle("full")


@pytest.fixture(scope="module")
def incremental(full: int) -> dict[str, Any]:
    result = CliRunner().invoke(app, ["workload", "edit", "--count", "20", "--seed", "11"])
    assert result.exit_code == 0, result.output
    status, body = post_trigger("incremental", None)
    assert status == 202, body
    second = post_trigger("incremental", None)
    cycle_id = int(body["cycle_id"])
    assert wait_cycle(cycle_id, 900, echo=print) == SUCCEEDED
    return {"cycle_id": cycle_id, "second": second}


def test_full_cycle_extracts_and_loads_every_row(full: int) -> None:
    oltp_rows = oltp_counts()
    assert step_rows(full, "extract.file") == oltp_rows
    assert step_rows(full, "load.table") == oltp_rows
    assert raw_final_counts() == oltp_rows


def test_watermarks_are_committed_after_load(full: int) -> None:
    with control() as c:
        rows = c.execute(
            "SELECT w.table_name, w.cycle_id, w.high_value = p.wm_high,"
            " w.updated_at > r.ended_at FROM registry.watermark w"
            " JOIN audit.extract_plan p ON p.table_name = w.table_name AND p.cycle_id = %s"
            " JOIN audit.job_run r ON r.cycle_id = p.cycle_id AND r.job_id = 'load'",
            (full,),
        ).fetchall()
    assert {r[0] for r in rows} == set(tables())
    assert all(r[1:] == (full, True, True) for r in rows), rows


def test_incremental_cycle_extracts_only_the_changed_window(incremental: dict[str, Any]) -> None:
    cycle_id = incremental["cycle_id"]
    with control() as c:
        plan = c.execute(
            "SELECT table_name, mode, wm_low, wm_high FROM audit.extract_plan WHERE cycle_id = %s",
            (cycle_id,),
        ).fetchall()
    assert {m for _, m, _, _ in plan} == {"incremental"}
    changed = oltp_counts({t: (lo, hi) for t, _, lo, hi in plan})
    assert changed.get("pricing.zone_price", 0) >= 20
    assert step_rows(cycle_id, "extract.file") == changed
    assert step_rows(cycle_id, "load.table") == changed
    assert raw_final_counts() == oltp_counts()


def test_second_trigger_while_running_is_busy(incremental: dict[str, Any]) -> None:
    status, body = incremental["second"]
    assert status == 409
    assert body == {"cycle_id": incremental["cycle_id"], "status": "busy"}


def test_bad_request_is_refused() -> None:
    assert post_trigger("sideways", None)[0] == 400


def test_raw_columns_follow_the_oltp_order(full: int) -> None:
    with oltp() as o, ch_connect(ClickHouseSettings()) as ch:
        for t in tables():
            schema, name = t.split(".")
            pg = [
                r[0]
                for r in o.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = %s"
                    " AND table_name = %s ORDER BY ordinal_position",
                    (schema, name),
                )
            ]
            raw = [
                r[0]
                for r in ch.rows(
                    "SELECT name FROM system.columns WHERE database = 'raw'"
                    f" AND table = '{schema}__{name}' ORDER BY position"
                )
            ]
            assert [c for c in raw if c not in META] == pg, t


def test_deploy_recorded_the_group_ids() -> None:
    with control() as c:
        ids = dict(
            c.execute(
                "SELECT job_id, nifi_process_group_id FROM registry.job"
                " WHERE job_id IN ('cycle', 'extract', 'load')"
            ).fetchall()
        )
    assert set(ids) == {"cycle", "extract", "load"}
    assert all(ids.values()) and len(set(ids.values())) == 3


def test_failed_load_fails_the_cycle_and_a_rerun_recovers(incremental: dict[str, Any]) -> None:
    """A load that cannot insert fails at the load gate, finalize marks the cycle failed, the
    watermarks stay, and a rerun of the same cycle id loads the same window."""
    from retail_pricing.cycle import FAILED

    result = CliRunner().invoke(app, ["workload", "edit", "--count", "5", "--seed", "12"])
    assert result.exit_code == 0, result.output
    with control() as c:
        before = c.execute("SELECT max(cycle_id) FROM registry.watermark").fetchone()
    with ch_connect(ClickHouseSettings()) as ch:
        ch.command("RENAME TABLE raw.pricing__zone_price TO raw.pricing__zone_price_hidden")
        try:
            status, body = post_trigger("incremental", None)
            assert status == 202, body
            cycle_id = int(body["cycle_id"])
            assert wait_cycle(cycle_id, 900, echo=print) == FAILED
        finally:
            ch.command("RENAME TABLE raw.pricing__zone_price_hidden TO raw.pricing__zone_price")
    with control() as c:
        cycle = c.execute(
            "SELECT status, failed_stage FROM audit.cycle WHERE cycle_id = %s", (cycle_id,)
        ).fetchone()
        failed = c.execute(
            "SELECT s.item FROM audit.step s JOIN audit.job_run r USING (run_id)"
            " WHERE r.cycle_id = %s AND s.status = 'failed'",
            (cycle_id,),
        ).fetchall()
        after = c.execute("SELECT max(cycle_id) FROM registry.watermark").fetchone()
    assert cycle == ("failed", "load")
    assert failed == [("pricing.zone_price",)]
    assert after == before

    status, body = post_trigger("incremental", cycle_id)
    assert (status, body) == (202, {"cycle_id": cycle_id, "status": "rerun"})
    assert wait_cycle(cycle_id, 900, echo=print) == SUCCEEDED
    assert raw_final_counts() == oltp_counts()


@pytest.mark.timeout(900)  # the lost item runs out of every retry first (about 4 min)
def test_an_item_lost_before_its_first_event_fails_the_cycle(incremental: dict[str, Any]) -> None:
    """Every step event of one planned table is refused, as when the control DB is down past the
    retries: the item never writes a step, end_run still fails the extract run, the cycle fails
    and no watermark moves. A rerun after the fault extracts the table and commits it."""
    from retail_pricing.cycle import FAILED

    table = tables()[0]
    with control() as c:
        before = c.execute(
            "SELECT high_value, cycle_id FROM registry.watermark WHERE table_name = %s", (table,)
        ).fetchone()
        c.execute(
            "CREATE FUNCTION audit.test_refuse_event() RETURNS trigger LANGUAGE plpgsql AS $$"
            f" BEGIN IF NEW.item = '{table}' THEN RAISE EXCEPTION 'test: refused'; END IF;"
            " RETURN NEW; END $$;"
            "CREATE TRIGGER test_refuse_event BEFORE INSERT ON audit.step_event"
            " FOR EACH ROW EXECUTE FUNCTION audit.test_refuse_event()"
        )
    try:
        status, body = post_trigger("incremental", None)
        assert status == 202, body
        cycle_id = int(body["cycle_id"])
        assert wait_cycle(cycle_id, 900, echo=print) == FAILED
    finally:
        with control() as c:
            c.execute(
                "DROP TRIGGER test_refuse_event ON audit.step_event;"
                "DROP FUNCTION audit.test_refuse_event()"
            )
    with control() as c:
        cycle = c.execute(
            "SELECT status, failed_stage FROM audit.cycle WHERE cycle_id = %s", (cycle_id,)
        ).fetchone()
        run = c.execute(
            "SELECT status, error FROM audit.job_run WHERE cycle_id = %s AND job_id = 'extract'",
            (cycle_id,),
        ).fetchone()
        steps = c.execute(
            "SELECT count(*) FROM audit.step s JOIN audit.job_run r USING (run_id)"
            " WHERE r.cycle_id = %s AND split_part(s.item, '#', 1) = %s",
            (cycle_id, table),
        ).fetchone()
        after = c.execute(
            "SELECT high_value, cycle_id FROM registry.watermark WHERE table_name = %s", (table,)
        ).fetchone()
        moved = c.execute(
            "SELECT count(*) FROM registry.watermark WHERE cycle_id = %s", (cycle_id,)
        ).fetchone()
    assert cycle == ("failed", "extract")
    assert run is not None and run[0] == "failed"
    assert f"never started: {table}" in run[1]
    assert steps == (0,)
    assert after == before
    assert moved == (0,)

    status, body = post_trigger("incremental", cycle_id)
    assert (status, body) == (202, {"cycle_id": cycle_id, "status": "rerun"})
    assert wait_cycle(cycle_id, 900, echo=print) == SUCCEEDED
    with control() as c:
        assert c.execute(
            "SELECT cycle_id FROM registry.watermark WHERE table_name = %s", (table,)
        ).fetchone() == (cycle_id,)
