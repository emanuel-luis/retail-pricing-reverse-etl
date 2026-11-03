"""Group `10 extract`: OLTP tables to Parquet on S3, one step per file (PLAN 6.5).

Steps: `extract.table` (item `<table>`) opens when the table's item starts and closes once its
page queries exist; a query that fails later is recorded on it too. Every file is its own
`extract.file` step (item `<table>#<page>-<fragment>`) with rows, bytes and the S3 key, which is
what `audit.load_plan` sums.
"""

from retail_pricing.nifi.builder.common import (
    OLTP,
    PARQUET_WRITER,
    S3_CREDENTIALS,
    attrs,
    event_sql,
    put_sql,
    query,
    stage,
)
from retail_pricing.nifi.builder.dsl import Group

# ponytail: concurrent tasks are not parameters in a flow definition; 4 matches
# extract_concurrency and the oltp-pool size of 8.
EXTRACT_TASKS = 4


def build(parent: Group) -> Group:
    s = stage(parent, "10 extract", "extract")
    g = s.g
    clock = query(
        g,
        "business clock",
        "select now()::text as wm_high, business_date::text as business_date"
        " from ops.business_clock where id = 1",
        OLTP,
    )
    window = attrs(g, "window", {"wm_high": "$[0].wm_high", "business_date": "$[0].business_date"})
    plan = query(
        g,
        "extract_plan",
        "select table_name, mode, wm_low::text as wm_low, wm_high::text as wm_high, page_rows,"
        " pk_column, select_list from registry.extract_plan(${cycle_id},"
        " '${wm_high}'::timestamptz, '${business_date}'::date, NULL)",
    )
    g.connect(s.run, clock, "unmatched")
    g.connect(clock, window, "success")
    s.guard_token(clock, "failure")
    g.connect(window, plan, "matched")
    g.connect(window, s.token_failed, "unmatched", "failure")
    s.guard_token(plan, "failure")
    split = s.split(plan)

    # One table.
    item = attrs(
        g,
        "table",
        {k: f"$.{k}" for k in ("table_name", "mode", "wm_low", "wm_high", "page_rows", "pk_column")}
        | {"select_list": "$.select_list"},
    )
    step = g.proc(
        "UpdateAttribute",
        "table step",
        {"step_id": "${UUID()}", "item": "${table_name}", "page": "0"},
    )
    started = put_sql(g, "table started", event_sql("extract.table", "started"))
    mode = g.proc("RouteOnAttribute", "mode", {"full": "${mode:equals('full')}"})
    fetch = g.proc(
        "GenerateTableFetch",
        "full pages",
        {
            "Database Connection Pooling Service": OLTP,
            "Database Type": "PostgreSQL",
            "Table Name": "${table_name}",
            "Columns to Return": "${select_list}",
            "Additional WHERE Clause": "updated_at <= '${wm_high}'",
            "Column for Value Partitioning": "${pk_column}",
            "Partition Size": "${page_rows}",
            "Output Empty FlowFile on Zero Results": "true",
        },
    )
    page = g.proc(
        "UpdateAttribute",
        "page",
        {
            "page": "${fragment.index:replaceNull('0')}",
            "Delete Attributes Expression": "fragment\\..*",
        },
    )
    incremental = g.proc(
        "ReplaceText",
        "incremental query",
        {
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Replacement Value": "SELECT ${select_list} FROM ${table_name}"
            " WHERE updated_at > '${wm_low}' AND updated_at <= '${wm_high}'",
        },
    )
    # Every page closes the table step again (same step_id); an empty table gives one empty
    # FlowFile from GenerateTableFetch, which stops here.
    planned = put_sql(g, "table succeeded", event_sql("extract.table", "succeeded"))
    has_sql = g.proc(
        "RouteOnAttribute", "has query", {"query": "${fileSize:gt(0)}"}, auto=("unmatched",)
    )
    run = g.proc(
        "ExecuteSQLRecord",
        "query pages",
        {
            "Database Connection Pooling Service": OLTP,
            "Record Writer": PARQUET_WRITER,
            "Use Avro Logical Types": "true",
            "Max Rows Per FlowFile": "#{page_rows}",
            "Fetch Size": "#{fetch_size}",
            "Set Auto Commit": "false",
        },
        tasks=EXTRACT_TASKS,
    )
    file_step = g.proc(
        "UpdateAttribute",
        "file step",
        {
            "step_id": "${UUID()}",
            "item": "${table_name}#${page}-${fragment.index:replaceNull('0')}",
        },
    )
    empty = g.proc("RouteOnAttribute", "rows", {"empty": "${executesql.row.count:equals('0')}"})
    key = g.proc(
        "UpdateAttribute",
        "s3 key",
        {
            "s3.key": "raw/${table_name}/cycle=${cycle_id}/run=${run_id}/"
            "part-${page}-${fragment.index:replaceNull('0')}.parquet"
        },
    )
    put = g.proc(
        "PutS3Object",
        "put parquet",
        {
            "Bucket": "#{s3_bucket}",
            "Object Key": "${s3.key}",
            "Endpoint Override URL": "#{s3_endpoint}",
            "Use Path Style Access": "true",
            "Region": "us-east-1",
            "AWS Credentials Provider Service": S3_CREDENTIALS,
        },
        tasks=2,
    )
    done = put_sql(
        g,
        "file succeeded",
        event_sql(
            "extract.file",
            "succeeded",
            rows="${executesql.row.count}",
            bytes_="${executesql.row.count:equals('0'):ifElse('0', ${fileSize})}",
            detail="jsonb_build_object('s3_key', NULLIF('${s3.key}', ''))",
        ),
        auto=("success",),
    )

    g.connect(split, item, "split")
    g.connect(item, step, "matched")
    g.connect(item, s.step_failed, "unmatched", "failure")
    g.connect(step, started, "success")
    g.connect(started, mode, "success")
    s.guard(started, "failure", "retry", step="extract.table")
    g.connect(mode, fetch, "full")
    g.connect(mode, incremental, "unmatched")
    g.connect(fetch, page, "success")
    s.guard(fetch, "failure", step="extract.table")
    g.connect(page, planned, "success")
    g.connect(incremental, planned, "success")
    g.connect(incremental, s.step_failed, "failure")
    g.connect(planned, has_sql, "success")
    s.guard(planned, "failure", "retry", step="extract.table")
    g.connect(has_sql, run, "query")
    g.connect(run, file_step, "success")
    s.guard(run, "failure", step="extract.table")
    g.connect(file_step, empty, "success")
    g.connect(empty, done, "empty")
    g.connect(empty, key, "unmatched")
    g.connect(key, put, "success")
    g.connect(put, done, "success")
    s.guard(put, "failure", step="extract.file")
    s.guard(done, "failure", "retry", step="extract.file")
    return g
