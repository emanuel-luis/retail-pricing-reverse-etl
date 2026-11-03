"""Group `50 reconcile`: the expected and actual checksums of each target (PLAN 4.1, 5, 6.5).

One `reconcile.target` step per target of `audit.stage_plan`. Expected rows: the `transit` check
from ClickHouse `reverse.rev_reconcile_expected` and the `applied` check from the OLTP stage
outcome (`pricing_apply.reconcile_expected_applied`, DECISIONS 120). Actual rows:
`pricing_apply.reconcile_actual`. The parent gate then runs `audit.reconcile`. `wsum` travels as a
decimal string end to end (Int128 in ClickHouse), never as a double.
"""

from retail_pricing.nifi.builder.common import (
    CONTROL,
    OLTP,
    attrs,
    body,
    clickhouse,
    event_sql,
    put_records,
    put_sql,
    query,
    stage,
)
from retail_pricing.nifi.builder.dsl import Group

KEYS = "cycle_id,target,zone_id,category_id,check_name"
STEP = "reconcile.target"
_OLTP_COLUMNS = (
    "${cycle_id}::bigint as cycle_id, target, zone_id, category_id, check_name, rows,"
    " sum_cents::bigint as sum_cents, wsum::text as wsum"
)


def build(parent: Group) -> Group:
    s = stage(parent, "50 reconcile", "reconcile")
    g = s.g
    plan = query(
        g, "stage_plan", "select item, target from audit.stage_plan(${cycle_id}, 'reconcile')"
    )
    g.connect(s.run, plan, "unmatched")
    s.guard_token(plan, "failure")
    split = s.split(plan)

    item = attrs(g, "target", {"item": "$.item", "target": "$.target"})
    started = put_sql(g, "target started", event_sql(STEP, "started"))
    # 64-bit and wider integers stay quoted (the ClickHouse default): wsum is Int128.
    transit_sql = body(
        g,
        "transit query",
        "SELECT cycle_id, target, zone_id, category_id, check_name, rows, sum_cents, wsum"
        " FROM reverse.rev_reconcile_expected WHERE cycle_id = ${cycle_id}"
        " AND target = '${target}' FORMAT JSONEachRow",
    )
    transit = clickhouse(g, "transit expected")
    put_transit = put_records(g, "put transit", CONTROL, "audit.reconcile_expected", "UPSERT", KEYS)
    applied = query(
        g,
        "applied expected",
        f"select {_OLTP_COLUMNS} from pricing_apply.reconcile_expected_applied(${{cycle_id}},"
        " '${target}')",
        OLTP,
    )
    put_applied = put_records(g, "put applied", CONTROL, "audit.reconcile_expected", "UPSERT", KEYS)
    actual = query(
        g,
        "actual",
        f"select {_OLTP_COLUMNS} from pricing_apply.reconcile_actual(${{cycle_id}}, '${{target}}')",
        OLTP,
    )
    put_actual = put_records(g, "put actual", CONTROL, "audit.reconcile_actual", "UPSERT", KEYS)
    done = put_sql(g, "target succeeded", event_sql(STEP, "succeeded"), auto=("success",))

    g.connect(split, item, "success")
    g.connect(item, started, "matched")
    g.connect(item, s.step_failed, "unmatched", "failure")
    g.connect(started, transit_sql, "success")
    s.guard(started, "failure", "retry", step=STEP)
    g.connect(transit_sql, transit, "success")
    g.connect(transit_sql, s.step_failed, "failure")
    g.connect(transit, put_transit, "Response")
    s.guard(transit, "Retry", "Failure", step=STEP)
    g.connect(transit, s.step_failed, "No Retry")
    g.connect(put_transit, applied, "success")
    s.guard(put_transit, "failure", "retry", step=STEP)
    g.connect(applied, put_applied, "success")
    s.guard(applied, "failure", step=STEP)
    g.connect(put_applied, actual, "success")
    s.guard(put_applied, "failure", "retry", step=STEP)
    g.connect(actual, put_actual, "success")
    s.guard(actual, "failure", step=STEP)
    g.connect(put_actual, done, "success")
    s.guard(put_actual, "failure", "retry", step=STEP)
    s.guard(done, "failure", "retry", step=STEP)
    return g
