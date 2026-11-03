"""Group `20 load`: ClickHouse pulls each table's Parquet from S3 (PLAN 6.5, DECISIONS 100).

One `load.table` step per table with the extract run's files; it succeeds only when ClickHouse
wrote exactly the rows the extract steps recorded.
"""

from retail_pricing.nifi.builder.common import attrs, event_sql, put_sql, query, stage
from retail_pricing.nifi.builder.dsl import Group

# `filename=` is relative to the raw_s3 collection URL; overriding `url=` needs a grant the nifi
# user does not have (DECISIONS 100).
INSERT = (
    "INSERT INTO raw.${table_name:replace('.', '__')} SELECT *, ${cycle_id}, now64(3)"
    " FROM s3(raw_s3, filename='${prefix}*.parquet', format='Parquet')"
    " SETTINGS use_hive_partitioning=0"
)


def build(parent: Group) -> Group:
    s = stage(parent, "20 load", "load")
    g = s.g
    plan = query(
        g,
        "load_plan",
        "select table_name, prefix, expected_rows, file_count"
        " from audit.load_plan(${cycle_id}, '${extract_run_id}'::uuid)",
    )
    g.connect(s.run, plan, "unmatched")
    s.guard_token(plan, "failure")
    split = s.split(plan)

    item = attrs(
        g,
        "table",
        {k: f"$.{k}" for k in ("table_name", "prefix", "expected_rows", "file_count")},
    )
    step = g.proc("UpdateAttribute", "table step", {"item": "${table_name}"})
    started = put_sql(g, "table started", event_sql("load.table", "started"))
    sql = g.proc(
        "ReplaceText",
        "insert select",
        {
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Replacement Value": INSERT,
        },
    )
    http = g.proc(
        "InvokeHTTP",
        "clickhouse insert",
        {
            "HTTP Method": "POST",
            "HTTP URL": "#{ch_url}/?wait_end_of_query=1&max_insert_threads=2",
            "Request Username": "#{ch_user}",
            "Request Password": "#{ch_password}",
            "Request Content-Type": "text/plain",
            "Socket Read Timeout": "#{ch_read_timeout}",
        },
        auto=("Original",),
    )
    written = g.proc(
        "UpdateAttribute",
        "written rows",
        {"written_rows": "${X-ClickHouse-Summary:jsonPath('$.written_rows')}"},
    )
    check = g.proc(
        "RouteOnAttribute", "rows match", {"match": "${written_rows:equals(${expected_rows})}"}
    )
    mismatch = g.proc(
        "UpdateAttribute",
        "rows differ",
        {"step.error": "written_rows ${written_rows} != expected_rows ${expected_rows}"},
    )
    done = put_sql(
        g,
        "table succeeded",
        event_sql(
            "load.table",
            "succeeded",
            rows="${written_rows}",
            detail="jsonb_build_object('prefix', '${prefix}', 'files', ${file_count})",
        ),
        auto=("success",),
    )

    g.connect(split, item, "success")
    g.connect(item, step, "matched")
    g.connect(item, s.step_failed, "unmatched", "failure")
    g.connect(step, started, "success")
    g.connect(started, sql, "success")
    s.guard(started, "failure", "retry", step="load.table")
    g.connect(sql, http, "success")
    g.connect(sql, s.step_failed, "failure")
    g.connect(http, written, "Response")
    s.guard(http, "Retry", "Failure", step="load.table")
    g.connect(http, s.step_failed, "No Retry")
    g.connect(written, check, "success")
    g.connect(check, done, "match")
    g.connect(check, mismatch, "unmatched")
    g.connect(mismatch, s.step_failed, "success")
    s.guard(done, "failure", "retry", step="load.table")
    return g
