"""Full and incremental cycles through the deployed flow (marker `integration`).

Needs the core stack with the dbt runner and the OLTP loaded (SF0.01), `control init`,
`ch init`, `s3 init` and `retail-pricing nifi deploy --env local|ci`. The raw tables are
truncated first, and the cycle schedule is switched off while the module runs so only the tests
start cycles. Each cycle runs the whole loop: extract, load, transform, reverse and reconcile.
"""

import subprocess
import time
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg import sql
from typer.testing import CliRunner

from retail_pricing.cli import app
from retail_pricing.clickhouse import ddl
from retail_pricing.clickhouse.client import connect as ch_connect
from retail_pricing.cycle import SUCCEEDED, post_trigger, wait_cycle, wait_stage
from retail_pricing.data.contract import APPLY_TABLES
from retail_pricing.settings import ClickHouseSettings, ControlSettings, OltpSettings

pytestmark = pytest.mark.integration

META = {name for name, _ in ddl.META_COLUMNS}
# Tables the reverse load writes after the extract: their OLTP counts move past the snapshot.
LOOP_WRITTEN = {
    "pricing.zone_price",
    "pricing.store_price_override",
    "pricing.promo_price",
    "pricing.price_change",
    "pricing.price_history",
    "pricing.shelf_label_queue",
    *APPLY_TABLES,
}
TARGETS = ("zone_price", "store_override", "promo_price", "shelf_label")


def untouched(counts: dict[str, int]) -> dict[str, int]:
    return {t: n for t, n in counts.items() if t not in LOOP_WRITTEN}


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


def edit(*args: str) -> None:
    result = CliRunner().invoke(app, ["workload", "edit", *args])
    assert result.exit_code == 0, result.output


def reverse_batches(cycle_id: int) -> list[tuple[Any, ...]]:
    """(target, batch_no, sent, applied, unchanged, conflicts, rejects, replayed) of a cycle."""
    with control() as c:
        return c.execute(
            "SELECT target, batch_no, sent, applied, unchanged, conflicts, rejects, replayed"
            " FROM audit.reverse_batch WHERE cycle_id = %s ORDER BY target, batch_no",
            (cycle_id,),
        ).fetchall()


def reconcile_results(cycle_id: int) -> list[tuple[str, str, bool]]:
    with control() as c:
        return c.execute(
            "SELECT target, check_name, passed FROM audit.reconcile_result WHERE cycle_id = %s",
            (cycle_id,),
        ).fetchall()


def stage_outcome(cycle_id: int) -> list[tuple[Any, ...]]:
    """Every stage row's status and every batch's counts of a cycle, all targets."""
    with oltp() as o:
        rows = o.execute(
            "SELECT 'batch', target, batch_no::text, received, applied, unchanged, conflicts,"
            " rejects FROM pricing_apply.batch WHERE cycle_id = %s",
            (cycle_id,),
        ).fetchall()
        for t in TARGETS:
            rows += o.execute(
                sql.SQL(
                    "SELECT {}, status, status_detail, md5(row(s.*)::text), NULL::int, NULL::int,"
                    " NULL::int, NULL::int FROM pricing_apply.{} s WHERE cycle_id = %s"
                ).format(sql.Literal(t), sql.Identifier(f"{t}_stage")),
                (cycle_id,),
            ).fetchall()
    return sorted(rows, key=str)


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
    """Cycle 2: 20 edits before it starts (the incremental window), then 20 KVI edits while it
    is in transform, after its snapshot and before its apply, so apply_batch meets conflicts."""
    edit("--count", "20", "--seed", "11")
    status, body = post_trigger("incremental", None)
    assert status == 202, body
    second = post_trigger("incremental", None)
    cycle_id = int(body["cycle_id"])
    assert wait_stage(cycle_id, "transform", 600) == SUCCEEDED
    edit("--count", "20", "--kvi", "--seed", "7")
    assert wait_cycle(cycle_id, 900, echo=print) == SUCCEEDED
    return {"cycle_id": cycle_id, "second": second}


@pytest.fixture(scope="module")
def third(incremental: dict[str, Any]) -> int:
    """Cycle 3 extracts cycle 2's apply outcome, which closes the loop for cycle 2."""
    return run_cycle("incremental")


def test_full_cycle_extracts_and_loads_every_row(full: int) -> None:
    extracted = step_rows(full, "extract.file")
    assert step_rows(full, "load.table") == extracted
    assert raw_final_counts() == extracted
    assert untouched(extracted) == untouched(oltp_counts())
    assert "pricing.zone_price" in extracted


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
    extracted = step_rows(cycle_id, "extract.file")
    assert extracted.get("pricing.zone_price", 0) >= 20
    assert step_rows(cycle_id, "load.table") == extracted
    # The reverse load moves rows of the loop's own tables past the window after the extract.
    assert untouched(extracted) == untouched(changed)
    assert untouched(raw_final_counts()) == untouched(oltp_counts())


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
        ids = dict(c.execute("SELECT job_id, nifi_process_group_id FROM registry.job").fetchall())
    assert len(ids) == 9
    assert all(ids.values()) and len(set(ids.values())) == 9


def test_full_cycle_closes_the_loop(full: int) -> None:
    """dbt is green, every target reverse-loads in order, the counts add up, reconcile passes
    and the OLTP holds the dbt proposals for every applied key."""
    with control() as c:
        nodes = dict(
            c.execute(
                "SELECT status, count(*) FROM audit.dbt_node_result WHERE cycle_id = %s GROUP BY 1",
                (full,),
            ).fetchall()
        )
        runs = c.execute(
            "SELECT job_id, status FROM audit.job_run WHERE cycle_id = %s ORDER BY started_at",
            (full,),
        ).fetchall()
    assert nodes and set(nodes) <= {"success", "pass", "warn"}, nodes
    assert [j for j, _ in runs] == [
        "extract",
        "load",
        "transform",
        *(f"reverse.{t}" for t in TARGETS),
        "reconcile",
    ]
    assert {s for _, s in runs} == {"succeeded"}
    batches = reverse_batches(full)
    assert any(b[0] == "zone_price" and b[3] > 0 for b in batches), batches
    for _, _, sent, applied, unchanged, conflicts, rejects, _ in batches:
        assert sent == applied + unchanged + conflicts + rejects
    results = reconcile_results(full)
    assert results and all(passed for *_, passed in results), results
    assert {"transit", "applied"} == {check for _, check, _ in results}

    with ch_connect(ClickHouseSettings()) as ch:
        proposed = {
            (int(str(p)), int(str(z))): Decimal(str(price))
            for p, z, price in ch.rows(
                f"SELECT product_id, zone_id, new_price FROM reverse.rev_zone_price"
                f" WHERE cycle_id = {full}"
            )
        }
    with oltp() as o:
        applied = o.execute(
            "SELECT s.product_id, s.zone_id, t.price FROM pricing_apply.zone_price_stage s"
            " JOIN pricing.zone_price t USING (product_id, zone_id)"
            # Later cycles of the module reprice some keys: keep those this cycle still owns.
            " WHERE s.cycle_id = %s AND s.status = 'applied' AND t.last_cycle_id = s.cycle_id",
            (full,),
        ).fetchall()
    assert applied
    assert all(proposed[(p, z)] == price for p, z, price in applied)


def test_edits_during_transform_are_conflicts_that_keep_the_human_price(
    incremental: dict[str, Any],
) -> None:
    cycle_id = incremental["cycle_id"]
    batches = reverse_batches(cycle_id)
    assert sum(b[5] for b in batches) > 0, batches
    for _, _, sent, applied, unchanged, conflicts, rejects, _ in batches:
        assert sent == applied + unchanged + conflicts + rejects
    with oltp() as o:
        rows = o.execute(
            "SELECT c.actual_price, t.price, t.updated_by, t.last_cycle_id"
            " FROM pricing_apply.conflict c JOIN pricing.zone_price t"
            " ON c.key_text = t.product_id || ':' || t.zone_id"
            " WHERE c.cycle_id = %s AND c.target = 'zone_price'",
            (cycle_id,),
        ).fetchall()
    assert rows
    assert all(human == now and by == "store-ops" and last is None for human, now, by, last in rows)
    assert all(passed for *_, passed in reconcile_results(cycle_id))


def test_next_cycle_brings_the_outcome_to_the_marts(
    incremental: dict[str, Any], third: int
) -> None:
    cycle_id = incremental["cycle_id"]
    with oltp() as o:
        conflicts = o.execute(
            "SELECT target, key_text FROM pricing_apply.conflict WHERE cycle_id = %s",
            (cycle_id,),
        ).fetchall()
        applied = o.execute(
            "SELECT count(*) FROM pricing_apply.zone_price_stage"
            " WHERE cycle_id = %s AND status = 'applied'",
            (cycle_id,),
        ).fetchone()
    with ch_connect(ClickHouseSettings()) as ch:
        marts = ch.rows(
            f"SELECT target, key_text FROM marts.mart_conflicts WHERE cycle_id = {cycle_id}"
        )
        outcome = {
            str(k): str(v)
            for k, v in (
                ch.rows(
                    "SELECT outcome, toString(count()) FROM marts.mart_proposed_vs_applied"
                    f" WHERE cycle_id = {cycle_id} AND target = 'zone_price' GROUP BY outcome"
                )
            )
        }
    assert sorted(map(tuple, marts)) == sorted(conflicts)
    assert applied is not None and outcome.get("applied") == str(applied[0])
    assert "not_sent" not in outcome


@pytest.mark.timeout(900)  # apply_batch fails through every retry first (about 2 min)
def test_a_cycle_stopped_mid_reverse_reruns_to_the_same_outcome(third: int) -> None:
    """apply_batch refuses shelf labels: prices are applied, the cycle fails at the label stage.
    The rerun replays every applied batch with its stored counts and applies the labels; the
    price outcome is identical and reconcile passes."""
    from retail_pricing.cycle import FAILED

    edit("--count", "20", "--kvi", "--seed", "13")
    with oltp() as o:
        o.execute(
            "CREATE FUNCTION pricing_apply.test_refuse_batch() RETURNS trigger LANGUAGE plpgsql"
            " AS $$ BEGIN IF NEW.target = 'shelf_label' THEN RAISE EXCEPTION 'test: refused';"
            " END IF; RETURN NEW; END $$;"
            "CREATE TRIGGER test_refuse_batch BEFORE INSERT ON pricing_apply.batch"
            " FOR EACH ROW EXECUTE FUNCTION pricing_apply.test_refuse_batch()"
        )
    try:
        status, body = post_trigger("incremental", None)
        assert status == 202, body
        cycle_id = int(body["cycle_id"])
        assert wait_cycle(cycle_id, 900, echo=print) == FAILED
    finally:
        with oltp() as o:
            o.execute(
                "DROP TRIGGER test_refuse_batch ON pricing_apply.batch;"
                "DROP FUNCTION pricing_apply.test_refuse_batch()"
            )
    with control() as c:
        assert c.execute(
            "SELECT status, failed_stage FROM audit.cycle WHERE cycle_id = %s", (cycle_id,)
        ).fetchone() == ("failed", "reverse.shelf_label")
    before = [r for r in stage_outcome(cycle_id) if "shelf_label" not in r[:2]]
    assert any(r[0] == "batch" and r[1] == "zone_price" for r in before), before

    status, body = post_trigger("incremental", cycle_id)
    assert (status, body) == (202, {"cycle_id": cycle_id, "status": "rerun"})
    assert wait_cycle(cycle_id, 900, echo=print) == SUCCEEDED
    after = stage_outcome(cycle_id)
    assert [r for r in after if "shelf_label" not in r[:2]] == before
    batches = reverse_batches(cycle_id)
    assert all(b[7] == 1 for b in batches if b[0] != "shelf_label"), batches
    assert any(b[0] == "shelf_label" and b[7] == 0 and b[3] > 0 for b in batches), batches
    assert all(passed for *_, passed in reconcile_results(cycle_id))


GUARDRAIL = "/home/app/dbt/tests/guardrails/g99_test_injected.sql"


def runner_sh(cmd: str) -> None:
    subprocess.run(
        ["docker", "compose", "exec", "-T", "dbt-runner", "sh", "-c", cmd], check=True, timeout=60
    )


def test_a_guardrail_error_fails_the_cycle_before_any_reverse_load(third: int) -> None:
    """An extra error-severity guardrail that always returns a row (written into the runner's
    project while the test runs): dbt fails, transform fails, and no batch reaches the OLTP."""
    from retail_pricing.cycle import FAILED

    edit("--count", "20", "--kvi", "--seed", "17")
    runner_sh(f"printf 'select 1 as injected\\n' > {GUARDRAIL}")
    try:
        status, body = post_trigger("incremental", None)
        assert status == 202, body
        cycle_id = int(body["cycle_id"])
        assert wait_cycle(cycle_id, 900, echo=print) == FAILED
    finally:
        runner_sh(f"rm -f {GUARDRAIL}")
    with control() as c:
        cycle = c.execute(
            "SELECT status, failed_stage FROM audit.cycle WHERE cycle_id = %s", (cycle_id,)
        ).fetchone()
        injected = c.execute(
            "SELECT status FROM audit.dbt_node_result WHERE cycle_id = %s"
            " AND unique_id LIKE 'test.%%g99_test_injected%%'",
            (cycle_id,),
        ).fetchall()
        runs = c.execute(
            "SELECT job_id FROM audit.job_run WHERE cycle_id = %s", (cycle_id,)
        ).fetchall()
    assert cycle == ("failed", "transform")
    assert injected == [("fail",)]
    assert not any(j.startswith("reverse.") for (j,) in runs)
    assert reverse_batches(cycle_id) == []
    with oltp() as o:
        assert o.execute(
            "SELECT count(*) FROM pricing_apply.batch WHERE cycle_id = %s", (cycle_id,)
        ).fetchone() == (0,)


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
    assert untouched(raw_final_counts()) == untouched(oltp_counts())


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
    # Other items may fail too (their events can meet the same fault); the guarantee is that
    # the table whose events were all refused is named as never started.
    assert "never started:" in run[1]
    assert table in run[1].split("never started:", 1)[1]
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
