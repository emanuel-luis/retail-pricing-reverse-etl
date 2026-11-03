"""Generate config/grafana/dashboards/*.json, the three provisioned dashboards.

Run `uv run python scripts/gen_dashboards.py` after changing a panel and commit the JSON with it;
`tests/unit/test_monitoring.py` regenerates the files and fails when they differ.
"""

import json
import sys
from pathlib import Path

OUT = (
    Path(sys.argv[1])
    if len(sys.argv) > 1
    else Path(__file__).parent.parent / "config/grafana/dashboards"
)
PG = {"type": "grafana-postgresql-datasource", "uid": "control"}
CH = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
PROM = {"type": "prometheus", "uid": "prometheus"}


def pg(sql: str, fmt: str = "table") -> dict:
    return {
        "refId": "A",
        "datasource": PG,
        "rawSql": sql,
        "format": fmt,
        "rawQuery": True,
        "editorMode": "code",
    }


def ch(sql: str) -> dict:
    return {
        "refId": "A",
        "datasource": CH,
        "editorType": "sql",
        "rawSql": sql,
        "format": 1,
        "queryType": "table",
    }


def prom(*exprs: tuple[str, str]) -> list[dict]:
    return [
        {"refId": chr(65 + i), "datasource": PROM, "expr": e, "legendFormat": legend, "range": True}
        for i, (e, legend) in enumerate(exprs)
    ]


def panel(
    title,
    kind,
    ds,
    targets,
    w=12,
    h=8,
    unit=None,
    stacked=False,
    transformations=None,
    description=None,
):
    p = {
        "title": title,
        "type": kind,
        "datasource": ds,
        "targets": targets if isinstance(targets, list) else [targets],
        "gridPos": {"w": w, "h": h},
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if description:
        p["description"] = description
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if kind == "barchart":
        p["options"] = {
            "xTickLabelRotation": 0,
            "stacking": "normal" if stacked else "none",
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
        }
    if kind == "timeseries" and stacked:
        p["fieldConfig"]["defaults"]["custom"] = {"stacking": {"mode": "normal"}}
    if transformations:
        p["transformations"] = transformations
    return p


def matrix(row: str, col: str, val: str) -> list[dict]:
    return [
        {
            "id": "groupingToMatrix",
            "options": {
                "rowField": row,
                "columnField": col,
                "valueField": val,
                "emptyValue": "zero",
            },
        }
    ]


def dashboard(uid, title, description, panels, time_from="now-24h"):
    x = y = 0
    row_h = 0
    for i, p in enumerate(panels, start=1):
        w, h = p["gridPos"]["w"], p["gridPos"]["h"]
        if x + w > 24:
            x, y, row_h = 0, y + row_h, 0
        p["id"] = i
        p["gridPos"].update({"x": x, "y": y})
        x += w
        row_h = max(row_h, h)
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["retail-pricing"],
        "editable": False,
        "schemaVersion": 41,
        "version": 1,
        "refresh": "30s",
        "time": {"from": time_from, "to": "now"},
        "timezone": "utc",
        "panels": panels,
        "templating": {"list": []},
        "annotations": {"list": []},
    }


LATEST_RB = "(SELECT max(cycle_id) FROM audit.reverse_batch)"

overview = dashboard(
    "pipeline-overview",
    "Pipeline overview",
    "Cycles, stages, reverse outcomes and checks from the control database (audit and registry).",
    [
        panel(
            "Cycles",
            "table",
            PG,
            pg(
                "SELECT cycle_id, mode, business_date, status, failed_stage, attempt, started_at,\n"
                "       ended_at,\n"
                "       extract(epoch FROM coalesce(ended_at, now()) - started_at) AS duration_s\n"
                "FROM audit.cycle\nORDER BY cycle_id DESC\nLIMIT 50"
            ),
            w=24,
        ),
        panel(
            "Stage duration per cycle",
            "barchart",
            PG,
            pg(
                "SELECT cycle_id::text AS cycle,\n"
                "       sum(duration_s) FILTER (WHERE job_id = 'extract') AS extract,\n"
                "       sum(duration_s) FILTER (WHERE job_id = 'load') AS load,\n"
                "       sum(duration_s) FILTER (WHERE job_id = 'transform') AS transform,\n"
                "       sum(duration_s) FILTER (WHERE job_id LIKE 'reverse.%') AS reverse,\n"
                "       sum(duration_s) FILTER (WHERE job_id = 'reconcile') AS reconcile\n"
                "FROM audit.v_cycle_stage_duration\nGROUP BY cycle_id\nORDER BY cycle_id"
            ),
            unit="s",
            stacked=True,
        ),
        panel(
            "Rows and bytes per stage",
            "table",
            PG,
            pg(
                "SELECT cycle_id, job_id, status, rows_in, rows_out, bytes\n"
                "FROM audit.v_rows_per_stage\nORDER BY cycle_id DESC, stage_order\nLIMIT 100"
            ),
        ),
        panel(
            "Extract vs load per table",
            "table",
            PG,
            pg(
                "SELECT r.cycle_id, s.item AS table_name,\n"
                "       sum(s.rows) FILTER (WHERE s.step_name = 'extract.table') AS extract_rows,\n"
                "       sum(s.rows) FILTER (WHERE s.step_name = 'load.table') AS load_rows,\n"
                "       sum(s.bytes) FILTER (WHERE s.step_name = 'extract.table') AS extract_bytes\n"
                "FROM audit.step s\nJOIN audit.v_latest_run r USING (run_id)\n"
                "WHERE s.step_name IN ('extract.table', 'load.table') AND s.status = 'succeeded'\n"
                "GROUP BY r.cycle_id, s.item\nORDER BY r.cycle_id DESC, s.item\nLIMIT 200"
            ),
            description="Rows each table extracted (Parquet written) and loaded (rows checked "
            "in ClickHouse), latest run of each stage.",
        ),
        panel(
            "Reverse outcomes per target (latest cycle)",
            "barchart",
            PG,
            pg(
                "SELECT target, sent, applied, unchanged, conflicts, rejects\n"
                f"FROM audit.v_reverse_totals\nWHERE cycle_id = {LATEST_RB}\nORDER BY target"
            ),
        ),
        panel(
            "Conflict rate per cycle",
            "timeseries",
            PG,
            pg(
                "SELECT cycle_started_at AS time,\n"
                "       sum(conflicts)::float8 / nullif(sum(sent), 0) AS conflict_rate\n"
                "FROM audit.v_reverse_totals\nWHERE $__timeFilter(cycle_started_at)\n"
                "GROUP BY cycle_started_at\nORDER BY 1",
                fmt="time_series",
            ),
            unit="percentunit",
        ),
        panel(
            "dbt test results per cycle",
            "barchart",
            PG,
            pg(
                "SELECT d.cycle_id::text AS cycle,\n"
                "       count(*) FILTER (WHERE d.status = 'pass') AS pass,\n"
                "       count(*) FILTER (WHERE d.status = 'warn') AS warn,\n"
                "       count(*) FILTER (WHERE d.status IN ('fail', 'error')) AS error\n"
                "FROM audit.dbt_node_result d\nJOIN audit.v_latest_run r USING (run_id)\n"
                "WHERE d.resource_type IN ('test', 'unit_test')\n"
                "GROUP BY d.cycle_id\nORDER BY d.cycle_id"
            ),
            stacked=True,
        ),
        panel(
            "Reconciliation",
            "table",
            PG,
            pg(
                "SELECT cycle_id, target,\n"
                "       count(*) FILTER (WHERE passed) AS passed,\n"
                "       count(*) FILTER (WHERE NOT passed) AS failed\n"
                "FROM audit.reconcile_result\nGROUP BY cycle_id, target\n"
                "ORDER BY cycle_id DESC, target\nLIMIT 80"
            ),
        ),
        panel(
            "Run duration vs SLA",
            "table",
            PG,
            pg(
                "SELECT cycle_id, job_id, status, round(duration_s::numeric, 1) AS duration_s,\n"
                "       sla_s, breached\nFROM audit.v_sla\nORDER BY cycle_id DESC, job_id\n"
                "LIMIT 100"
            ),
        ),
        panel(
            "Open alerts",
            "table",
            PG,
            pg(
                "SELECT raised_at, severity, kind, cycle_id, message\nFROM audit.alert\n"
                "WHERE resolved_at IS NULL\nORDER BY raised_at DESC\nLIMIT 50"
            ),
        ),
        panel(
            "Watermark lag per table",
            "bargauge",
            PG,
            pg(
                "SELECT table_name, extract(epoch FROM now() - high_value) AS lag_s\n"
                "FROM registry.watermark\nORDER BY lag_s DESC"
            ),
            unit="s",
            w=24,
            h=10,
        ),
    ],
)

LATEST_IMPACT = "(SELECT max(cycle_id) FROM marts.mart_price_change_impact)"
outcomes = dashboard(
    "pricing-outcomes",
    "Pricing outcomes",
    "What the engine proposed and what the OLTP applied, from the ClickHouse pricing and marts "
    "databases. A cycle's apply outcome is complete once the next cycle has extracted.",
    [
        panel(
            "Changes per cycle by reason",
            "barchart",
            CH,
            ch(
                "SELECT toString(cycle_id) AS cycle, primary_reason, sum(n_changed) AS n_changed\n"
                "FROM marts.mart_price_change_impact\nGROUP BY cycle_id, primary_reason\n"
                "ORDER BY cycle_id, primary_reason"
            ),
            stacked=True,
            transformations=matrix("cycle", "primary_reason", "n_changed"),
        ),
        panel(
            "Average change by category (latest cycle)",
            "barchart",
            CH,
            ch(
                # A label Grafana cannot parse as a number, or percentunit formats the ids too.
                "SELECT concat('category ', toString(category_id)) AS category,\n"
                "       round(sum(avg_change_pct * n_changed) / sum(n_changed), 4) AS avg_change\n"
                f"FROM marts.mart_price_change_impact\nWHERE cycle_id = {LATEST_IMPACT}\n"
                "GROUP BY category_id\nORDER BY category_id"
            ),
            unit="percentunit",
        ),
        panel(
            "Zone price changes up and down by percent (latest cycle)",
            "barchart",
            CH,
            ch(
                "SELECT toString(bucket) AS pct_bucket,\n"
                "       countIf(is_up) AS up, countIf(NOT is_up) AS down\n"
                "FROM (\n"
                "    SELECT toInt32(floor(toFloat64(change_pct) * 100)) AS bucket,\n"
                "           change_pct > 0 AS is_up\n"
                "    FROM pricing.pricing_price_change_log\n"
                "    WHERE target = 'zone_price'\n"
                "      AND cycle_id = (SELECT max(cycle_id) FROM pricing.pricing_price_change_log)\n"
                ")\nGROUP BY bucket\nORDER BY bucket"
            ),
            description="Changed zone prices per whole-percent bucket of change.",
        ),
        panel(
            "Projected 28-day delta by zone (latest cycle)",
            "barchart",
            CH,
            ch(
                "SELECT toString(zone_id) AS zone,\n"
                "       round(sum(projected_margin_delta), 2) AS margin_delta,\n"
                "       round(sum(projected_revenue_delta), 2) AS revenue_delta\n"
                f"FROM marts.mart_price_change_impact\nWHERE cycle_id = {LATEST_IMPACT}\n"
                "GROUP BY zone_id\nORDER BY zone_id"
            ),
        ),
        panel(
            "Proposed vs applied vs conflicts per cycle",
            "barchart",
            CH,
            ch(
                "SELECT toString(cycle_id) AS cycle,\n"
                "       countIf(outcome = 'applied') AS applied,\n"
                "       countIf(outcome = 'unchanged') AS unchanged,\n"
                "       countIf(outcome = 'conflict') AS conflicts,\n"
                "       countIf(outcome = 'rejected') AS rejected,\n"
                "       countIf(outcome IN ('staged', 'not_sent')) AS pending\n"
                "FROM marts.mart_proposed_vs_applied\nGROUP BY cycle_id\nORDER BY cycle_id"
            ),
            stacked=True,
            description="Every proposed row by apply outcome; pending rows are "
            "not yet extracted back (the next cycle completes them).",
        ),
        panel(
            "Top conflicts by price gap",
            "table",
            CH,
            ch(
                "SELECT cycle_id, target, key_text, expected_version, actual_version,\n"
                "       proposed_price, human_price,\n"
                "       abs(toFloat64(proposed_price) - toFloat64(human_price)) AS gap,\n"
                "       editor, detected_at\n"
                "FROM marts.mart_conflicts\nORDER BY gap DESC, detected_at DESC\nLIMIT 50"
            ),
        ),
        panel(
            "Shelf labels per store (latest cycle)",
            "barchart",
            CH,
            ch(
                "SELECT toString(store_id) AS store, count() AS labels\n"
                "FROM pricing.pricing_shelf_label\n"
                "WHERE cycle_id = (SELECT max(cycle_id) FROM pricing.pricing_shelf_label)\n"
                "GROUP BY store_id\nORDER BY labels DESC\nLIMIT 30"
            ),
        ),
        panel(
            "Held zone prices by hold reason",
            "barchart",
            CH,
            ch(
                "SELECT toString(cycle_id) AS cycle, primary_reason AS hold_reason, count() AS n\n"
                "FROM pricing.pricing_zone_price\nWHERE startsWith(primary_reason, 'HOLD_')\n"
                "GROUP BY cycle_id, primary_reason\nORDER BY cycle_id, primary_reason"
            ),
            stacked=True,
            transformations=matrix("cycle", "hold_reason", "n"),
        ),
    ],
)

DB = 'datname=~"retail|control"'
NIFI_ROOT = 'component_type="RootProcessGroup"'
platform = dashboard(
    "platform",
    "Platform",
    "NiFi, ClickHouse, both Postgres databases, the workload generator and the dbt runner, "
    "from Prometheus.",
    [
        panel("Scrape targets up", "stat", PROM, prom(("up", "{{job}}")), w=24, h=4),
        panel(
            "NiFi queued FlowFiles",
            "timeseries",
            PROM,
            prom((f"nifi_amount_items_queued{{{NIFI_ROOT}}}", "queued")),
        ),
        panel(
            "NiFi queued bytes",
            "timeseries",
            PROM,
            prom((f"nifi_size_content_queued_total{{{NIFI_ROOT}}}", "queued")),
            unit="bytes",
        ),
        panel(
            "NiFi active threads",
            "timeseries",
            PROM,
            prom((f"nifi_amount_threads_active{{{NIFI_ROOT}}}", "active")),
        ),
        panel(
            "NiFi bytes read and written per group (5 min window)",
            "timeseries",
            PROM,
            prom(
                (
                    'nifi_amount_bytes_read{component_type="ProcessGroup"}',
                    "{{component_name}} read",
                ),
                (
                    'nifi_amount_bytes_written{component_type="ProcessGroup"}',
                    "{{component_name}} written",
                ),
            ),
            unit="bytes",
        ),
        panel(
            "NiFi JVM heap",
            "timeseries",
            PROM,
            prom(("nifi_jvm_heap_used", "used"), ("nifi_jvm_heap_committed", "committed")),
            unit="bytes",
        ),
        panel(
            "ClickHouse queries/s",
            "timeseries",
            PROM,
            prom(
                ("rate(ClickHouseProfileEvents_Query[1m])", "queries"),
                ("rate(ClickHouseProfileEvents_FailedQuery[1m])", "failed"),
            ),
            unit="reqps",
        ),
        panel(
            "ClickHouse memory vs server limit",
            "timeseries",
            PROM,
            prom(
                ("ClickHouseMetrics_MemoryTracking", "tracked"),
                ("0.8 * ClickHouseAsyncMetrics_CGroupMemoryTotal", "server limit (0.8 x cgroup)"),
            ),
            unit="bytes",
        ),
        panel(
            "ClickHouse parts and merges",
            "timeseries",
            PROM,
            prom(
                ("ClickHouseMetrics_PartsActive", "active parts"),
                ("ClickHouseMetrics_Merge", "running merges"),
            ),
        ),
        panel(
            "ClickHouse S3 read and external sort/group-by bytes/s",
            "timeseries",
            PROM,
            prom(
                ("rate(ClickHouseProfileEvents_ReadBufferFromS3Bytes[1m])", "S3 read"),
                ("rate(ClickHouseProfileEvents_ExternalSortCompressedBytes[1m])", "external sort"),
                (
                    "rate(ClickHouseProfileEvents_ExternalAggregationCompressedBytes[1m])",
                    "external group by",
                ),
            ),
            unit="Bps",
        ),
        panel(
            "Postgres transactions/s",
            "timeseries",
            PROM,
            prom(
                (
                    (
                        f"sum by (datname) (rate(pg_stat_database_xact_commit{{{DB}}}[1m])"
                        f" + rate(pg_stat_database_xact_rollback{{{DB}}}[1m]))"
                    ),
                    "{{datname}}",
                )
            ),
        ),
        panel(
            "Postgres connections",
            "timeseries",
            PROM,
            prom((f"sum by (datname) (pg_stat_database_numbackends{{{DB}}})", "{{datname}}")),
        ),
        panel(
            "Postgres locks by mode",
            "timeseries",
            PROM,
            prom((f"sum by (datname, mode) (pg_locks_count{{{DB}}}) > 0", "{{datname}} {{mode}}")),
        ),
        panel(
            "Postgres deadlocks (5 min)",
            "timeseries",
            PROM,
            prom((f"increase(pg_stat_database_deadlocks{{{DB}}}[5m])", "{{datname}}")),
        ),
        panel(
            "Postgres tuples written/s",
            "timeseries",
            PROM,
            prom(
                (
                    f"sum by (datname) (rate(pg_stat_database_tup_inserted{{{DB}}}[1m]))",
                    "{{datname}} inserted",
                ),
                (
                    f"sum by (datname) (rate(pg_stat_database_tup_updated{{{DB}}}[1m]))",
                    "{{datname}} updated",
                ),
                (
                    f"sum by (datname) (rate(pg_stat_database_tup_deleted{{{DB}}}[1m]))",
                    "{{datname}} deleted",
                ),
            ),
        ),
        panel(
            "Postgres cache hit ratio",
            "timeseries",
            PROM,
            prom(
                (
                    (
                        f"sum by (datname) (rate(pg_stat_database_blks_hit{{{DB}}}[5m]))"
                        f" / (sum by (datname) (rate(pg_stat_database_blks_hit{{{DB}}}[5m]))"
                        f" + sum by (datname) (rate(pg_stat_database_blks_read{{{DB}}}[5m])))"
                    ),
                    "{{datname}}",
                )
            ),
            unit="percentunit",
        ),
        panel(
            "Postgres dead tuples (top 10 tables)",
            "timeseries",
            PROM,
            prom(
                (
                    "topk(10, pg_stat_user_tables_n_dead_tup)",
                    "{{datname}} {{schemaname}}.{{relname}}",
                )
            ),
        ),
        panel(
            "Workload transactions and errors/s",
            "timeseries",
            PROM,
            prom(
                ("sum by (type) (rate(workload_transactions_total[1m]))", "{{type}}"),
                ("sum by (type) (rate(workload_errors_total[1m]))", "{{type}} errors"),
            ),
        ),
        panel(
            "dbt runner: last job duration and jobs",
            "timeseries",
            PROM,
            prom(
                ("dbt_runner_last_job_duration_seconds", "last job duration (s)"),
                ("increase(dbt_runner_jobs_total[1h])", "{{status}} jobs (1 h)"),
                ("dbt_runner_running", "running"),
            ),
            w=24,
        ),
    ],
    time_from="now-3h",
)

for name, d in [
    ("pipeline-overview", overview),
    ("pricing-outcomes", outcomes),
    ("platform", platform),
]:
    (OUT / f"{name}.json").write_text(
        json.dumps(d, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    print(name, len(d["panels"]))
