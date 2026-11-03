"""Groups `40`-`43 reverse <target>`: changed rows back to the OLTP (PLAN 4.1, 6.5).

One template per target. The plan is the cycle's batches in `reverse.rev_<target>` (zero batches:
the run succeeds with no items). Each batch is one `reverse.batch` step (item `<target>#<n>`):
its rows from ClickHouse as JSONEachRow, INSERT_IGNORE into `pricing_apply.<target>_stage`,
`apply_batch`, then the counts upserted into `audit.reverse_batch`. Staging and apply_batch are
idempotent per (cycle, target, batch), so a retried or rerun batch replays its stored outcome.
"""

from retail_pricing.nifi.builder.common import (
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

# Columns of pricing_apply.<target>_stage that the rev model fills (the rest have defaults).
_PRICE = "batch_no, new_price, old_price, expected_version, reason_code, row_hash"
STAGE_COLUMNS = {
    "zone_price": f"cycle_id, product_id, zone_id, {_PRICE}",
    "store_override": f"cycle_id, store_id, product_id, {_PRICE}",
    "promo_price": f"cycle_id, promo_id, product_id, zone_id, {_PRICE}",
    "shelf_label": "cycle_id, store_id, product_id, batch_no, price, promo_price, due_date,"
    " source_target, zone_id, row_hash",
}
TARGETS = ("zone_price", "store_override", "promo_price", "shelf_label")
GROUP_NAMES = {t: f"{40 + i} reverse {t}" for i, t in enumerate(TARGETS)}
# Keys as numbers, decimals as strings: Postgres casts them exactly (never through a double).
_JSON = "output_format_json_quote_64bit_integers=0&output_format_json_quote_decimals=1"
STEP = "reverse.batch"


def build(parent: Group, target: str) -> Group:
    s = stage(parent, GROUP_NAMES[target], f"reverse.{target}")
    g = s.g
    plan_sql = body(
        g,
        "batches query",
        f"SELECT batch_no, count() AS batch_rows FROM reverse.rev_{target}"
        " WHERE cycle_id = ${cycle_id} GROUP BY batch_no ORDER BY batch_no FORMAT JSON",
    )
    plan = clickhouse(g, "batches")
    g.connect(s.run, plan_sql, "unmatched")
    g.connect(plan_sql, plan, "success")
    g.connect(plan_sql, s.token_failed, "failure")
    s.guard_token(plan, "Retry", "Failure")
    g.connect(plan, s.token_failed, "No Retry")
    split = s.split(plan, "$.data.*", "Response")

    item = attrs(g, "batch", {"batch_no": "$.batch_no", "batch_rows": "$.batch_rows"})
    step = g.proc("UpdateAttribute", "batch step", {"item": f"{target}#${{batch_no}}"})
    started = put_sql(g, "batch started", event_sql(STEP, "started"))
    rows_sql = body(
        g,
        "rows query",
        f"SELECT {STAGE_COLUMNS[target]} FROM reverse.rev_{target}"
        " WHERE cycle_id = ${cycle_id} AND batch_no = ${batch_no} FORMAT JSONEachRow",
    )
    rows = clickhouse(g, "batch rows", _JSON)
    staged = put_records(
        g,
        "stage rows",
        OLTP,
        f"pricing_apply.{target}_stage",
        "INSERT_IGNORE",
        rollback=True,
        ignore_unmatched_columns=True,
    )
    apply = query(
        g,
        "apply_batch",
        "select received, applied, unchanged, conflicts, rejects, replayed"
        f" from pricing_apply.apply_batch(${{cycle_id}}, '{target}', ${{batch_no}})",
        OLTP,
    )
    counts = attrs(
        g,
        "counts",
        {k: f"$[0].{k}" for k in ("received", "applied", "unchanged", "conflicts", "rejects")}
        | {"replayed": "$[0].replayed"},
    )
    record = put_sql(
        g,
        "reverse_batch",
        "INSERT INTO audit.reverse_batch (cycle_id, target, batch_no, run_id, sent, applied,"
        " unchanged, conflicts, rejects, replayed) VALUES (${cycle_id},"
        f" '{target}', ${{batch_no}}, '${{run_id}}', ${{batch_rows}}, ${{applied}},"
        " ${unchanged}, ${conflicts}, ${rejects}, ${replayed:equals('true'):ifElse(1, 0)})"
        " ON CONFLICT (cycle_id, target, batch_no) DO UPDATE SET run_id = excluded.run_id,"
        " sent = excluded.sent, applied = excluded.applied, unchanged = excluded.unchanged,"
        " conflicts = excluded.conflicts, rejects = excluded.rejects,"
        " replayed = excluded.replayed, at = now()",
    )
    done = put_sql(
        g,
        "batch succeeded",
        event_sql(
            STEP,
            "succeeded",
            rows="${received}",
            detail="jsonb_build_object('sent', ${batch_rows}, 'applied', ${applied},"
            " 'unchanged', ${unchanged}, 'conflicts', ${conflicts}, 'rejects', ${rejects},"
            " 'replayed', ${replayed})",
        ),
        auto=("success",),
    )

    g.connect(split, item, "success")
    g.connect(item, step, "matched")
    g.connect(item, s.step_failed, "unmatched", "failure")
    g.connect(step, started, "success")
    g.connect(started, rows_sql, "success")
    s.guard(started, "failure", "retry", step=STEP)
    g.connect(rows_sql, rows, "success")
    g.connect(rows_sql, s.step_failed, "failure")
    g.connect(rows, staged, "Response")
    s.guard(rows, "Retry", "Failure", step=STEP)
    g.connect(rows, s.step_failed, "No Retry")
    g.connect(staged, apply, "success")
    # With Rollback On Failure a failing insert stays queued and is retried in place; these
    # relationships only see FlowFiles if that property is ever switched off.
    s.guard(staged, "failure", "retry", step=STEP)
    g.connect(apply, counts, "success")
    s.guard(apply, "failure", step=STEP)
    g.connect(counts, record, "matched")
    g.connect(counts, s.step_failed, "unmatched", "failure")
    g.connect(record, done, "success")
    s.guard(record, "failure", "retry", step=STEP)
    s.guard(done, "failure", "retry", step=STEP)
    return g
