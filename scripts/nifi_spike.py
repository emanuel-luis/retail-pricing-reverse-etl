"""NiFi format and behavior spike, docs/PLAN.md 6.2 (run once by hand; the results are DECISIONS rows).

    uv run python scripts/nifi_spike.py build    # (a) build the reference group and export it
    uv run python scripts/nifi_spike.py check    # (b) round trip, then (c)-(i) on the deployed copy

Needs NiFi, oltp (loaded, SF0.01), control-db (control init), clickhouse (ch init) and s3 (s3 init).
The reference flow pins the definition format only; part B generates the real flows.
"""

import json
import sys
import time
from pathlib import Path
from typing import Any, ClassVar

import psycopg

from retail_pricing.clickhouse.client import connect as ch_connect
from retail_pricing.nifi.client import Client, connect
from retail_pricing.nifi.deploy import GROUP, child_group, deploy, root_id, walk
from retail_pricing.settings import (
    ClickHouseSettings,
    ControlSettings,
    NifiSettings,
    OltpSettings,
)

REFERENCE = Path("nifi/spike/reference-flow.json")
CONTEXT = "retail-pricing-local"
BARRIER = {
    "flowfileConcurrency": "SINGLE_FLOWFILE_PER_NODE",
    "flowfileOutboundPolicy": "BATCH_OUTPUT",
}
ONCE = "1 hour"  # GenerateFlowFile fires at start, then not again during the check


class Group:
    """Creates components in one process group through the REST API."""

    types: ClassVar[dict[str, dict[str, Any]]] = {}

    def __init__(self, c: Client, gid: str):
        self.c, self.id, self.n = c, gid, 0
        if not Group.types:
            for t in c.get("/flow/processor-types")["processorTypes"]:
                Group.types[t["type"].rsplit(".", 1)[1]] = t
            for t in c.get("/flow/controller-service-types")["controllerServiceTypes"]:
                Group.types[t["type"].rsplit(".", 1)[1]] = t

    def _rev(self) -> dict[str, Any]:
        return {"version": 0, "clientId": self.c.client_id}

    def _pos(self) -> dict[str, float]:
        self.n += 1
        return {"x": float((self.n - 1) % 4 * 450), "y": float((self.n - 1) // 4 * 250)}

    def _type(self, simple: str) -> dict[str, Any]:
        t = Group.types[simple]
        return {"type": t["type"], "bundle": t["bundle"]}

    def svc(self, simple: str, name: str, props: dict[str, str]) -> str:
        body = {
            "revision": self._rev(),
            "component": {**self._type(simple), "name": name, "properties": props},
        }
        return str(self.c.post(f"/process-groups/{self.id}/controller-services", json=body)["id"])

    def proc(
        self,
        simple: str,
        name: str,
        props: dict[str, str] | None = None,
        auto: tuple[str, ...] = (),
        period: str | None = None,
        tasks: str | None = None,
    ) -> str:
        config: dict[str, Any] = {
            "properties": props or {},
            "autoTerminatedRelationships": list(auto),
        }
        if period:
            config["schedulingPeriod"] = period
        if tasks:
            config["concurrentlySchedulableTaskCount"] = tasks
        body = {
            "revision": self._rev(),
            "component": {
                **self._type(simple),
                "name": name,
                "position": self._pos(),
                "config": config,
            },
        }
        return str(self.c.post(f"/process-groups/{self.id}/processors", json=body)["id"])

    def port(self, kind: str, name: str) -> str:
        body = {"revision": self._rev(), "component": {"name": name, "position": self._pos()}}
        return str(self.c.post(f"/process-groups/{self.id}/{kind}-ports", json=body)["id"])

    def group(self, name: str, **settings: str) -> "Group":
        body = {"revision": self._rev(), "component": {"name": name, "position": self._pos()}}
        pg = self.c.post(f"/process-groups/{self.id}/process-groups", json=body)
        # POST ignores the FlowFile concurrency and outbound policy; a PUT sets them. Every group
        # is bound to the context: a child does not inherit its parent's.
        component = {"id": pg["id"], **settings}
        if pc := self.c.get(f"/process-groups/{self.id}")["component"].get("parameterContext"):
            component["parameterContext"] = {"id": pc["id"]}
        self.c.put(
            f"/process-groups/{pg['id']}", json={"revision": pg["revision"], "component": component}
        )
        return Group(self.c, pg["id"])

    def conn(
        self, src: tuple[str, str, str], dst: tuple[str, str, str], rels: tuple[str, ...] = ()
    ) -> str:
        """src/dst = (id, group id, type), type PROCESSOR, INPUT_PORT or OUTPUT_PORT."""
        ends = {
            k: {"id": v[0], "groupId": v[1], "type": v[2]}
            for k, v in (("source", src), ("destination", dst))
        }
        body = {"revision": self._rev(), "component": {**ends, "selectedRelationships": list(rels)}}
        return str(self.c.post(f"/process-groups/{self.id}/connections", json=body)["id"])

    def p(self, pid: str) -> tuple[str, str, str]:
        return (pid, self.id, "PROCESSOR")


def disable(c: Client, pid: str) -> None:
    rev = c.get(f"/processors/{pid}")["revision"]
    c.put(f"/processors/{pid}/run-status", json={"revision": rev, "state": "DISABLED"})


# -- (a) build and export --------------------------------------------------------------------------


def build(c: Client) -> str:
    root = root_id(c)
    from retail_pricing.nifi.deploy import load_params, remove_group, upsert_parameter_context

    if old := child_group(c, root, GROUP):
        remove_group(c, old["id"])

    values, sensitive = load_params(Path("nifi/params/local.json"))
    pc_id = upsert_parameter_context(c, CONTEXT, values, sensitive)
    top = Group(c, Group(c, root).group(GROUP).id)
    pg = c.get(f"/process-groups/{top.id}")
    c.put(
        f"/process-groups/{top.id}",
        json={
            "revision": pg["revision"],
            "component": {"id": top.id, "parameterContext": {"id": pc_id}},
        },
    )

    pool = {
        "Database Driver Class Name": "org.postgresql.Driver",
        "Database Driver Locations": "#{jdbc_driver_dir}",
        "Validation Query": "select 1",
    }
    oltp = top.svc(
        "DBCPConnectionPool",
        "oltp-pool",
        {
            **pool,
            "Database Connection URL": "#{oltp_jdbc_url}",
            "Database User": "#{oltp_user}",
            "Password": "#{oltp_password}",
            "Max Total Connections": "8",
        },
    )
    ctrl = top.svc(
        "DBCPConnectionPool",
        "control-pool",
        {
            **pool,
            "Database Connection URL": "#{control_jdbc_url}",
            "Database User": "#{control_user}",
            "Password": "#{control_password}",
            "Max Total Connections": "4",
        },
    )
    s3c = top.svc(
        "AWSCredentialsProviderControllerService",
        "s3-credentials",
        {"Access Key ID": "#{s3_access_key}", "Secret Access Key": "#{s3_secret_key}"},
    )
    reader = top.svc("JsonTreeReader", "json-reader", {})
    writer = top.svc("JsonRecordSetWriter", "json-writer", {"Output Grouping": "output-array"})
    parquet = top.svc(
        "ParquetRecordSetWriter",
        "parquet-writer",
        {"Compression Type": "SNAPPY", "Row Group Size": "32 MB"},
    )
    httpctx = top.svc("StandardHttpContextMap", "http-context", {})

    # (c) barrier: Gen -> [in -> SplitJson; original -> out; split -> hold (disabled)] -> out -> sink
    g = top.group("c barrier")
    gen = g.proc("GenerateFlowFile", "gen", {"Custom Text": "[1,2,3,4,5]"}, period=ONCE)
    inner = g.group("barrier", **BARRIER)
    pin, pout = inner.port("input", "in"), inner.port("output", "out")
    split = inner.proc("SplitJson", "split", {"JsonPath Expression": "$.*"}, auto=("failure",))
    hold = inner.proc("UpdateAttribute", "hold", {"item": "${fragment.index}"}, auto=("success",))
    inner.conn((pin, inner.id, "INPUT_PORT"), inner.p(split))
    inner.conn(inner.p(split), (pout, inner.id, "OUTPUT_PORT"), ("original",))
    inner.conn(inner.p(split), inner.p(hold), ("split",))
    sink = g.proc("UpdateAttribute", "sink", auto=("success",))
    g.conn(g.p(gen), (pin, inner.id, "INPUT_PORT"), ("success",))
    g.conn((pout, inner.id, "OUTPUT_PORT"), g.p(sink))

    # (d) Parquet: ExecuteSQLRecord -> PutS3Object; ClickHouse reads it back with s3()
    g = top.group("d parquet")
    gen = g.proc("GenerateFlowFile", "gen", {"cycle_id": "999"}, period=ONCE)
    q = g.proc(
        "ExecuteSQLRecord",
        "extract",
        {
            "Database Connection Pooling Service": oltp,
            "Record Writer": parquet,
            "SQL Query": "SELECT * FROM pricing.price_history ORDER BY history_id",
            "Use Avro Logical Types": "true",
            "Max Rows Per FlowFile": "100",
            "Fetch Size": "#{fetch_size}",
            "Set Auto Commit": "false",
        },
        auto=("failure",),
        tasks="1",
    )
    key = g.proc(
        "UpdateAttribute",
        "key",
        {"s3.key": "spike/price_history/cycle=${cycle_id}/part-${fragment.index}.parquet"},
    )
    put = g.proc(
        "PutS3Object",
        "put",
        {
            "Bucket": "#{s3_bucket}",
            "Object Key": "${s3.key}",
            "Endpoint Override URL": "#{s3_endpoint}",
            "Use Path Style Access": "true",
            "Region": "us-east-1",
            "AWS Credentials Provider Service": s3c,
        },
        auto=("failure",),
    )
    sink = g.proc("UpdateAttribute", "sink", auto=("success",))
    g.conn(g.p(gen), g.p(q), ("success",))
    g.conn(g.p(q), g.p(key), ("success",))
    g.conn(g.p(key), g.p(put), ("success",))
    g.conn(g.p(put), g.p(sink), ("success",))

    # (e) GenerateTableFetch with EL from attributes
    g = top.group("e table fetch")
    gen = g.proc(
        "GenerateFlowFile",
        "gen",
        {
            "table_name": "pricing.price_history",
            "select_list": "history_id, price, valid_to",
            "pk_column": "history_id",
            "wm_high": "2999-01-01 00:00:00+00",
            "page_rows": "100",
        },
        period=ONCE,
    )
    gtf = g.proc(
        "GenerateTableFetch",
        "fetch",
        {
            "Database Connection Pooling Service": oltp,
            "Database Type": "PostgreSQL",
            "Table Name": "${table_name}",
            "Columns to Return": "${select_list}",
            "Additional WHERE Clause": "updated_at <= '${wm_high}'",
            "Column for Value Partitioning": "${pk_column}",
            "Partition Size": "${page_rows}",
        },
        auto=("failure",),
    )
    sink = g.proc("UpdateAttribute", "sink", auto=("success",))
    g.conn(g.p(gen), g.p(gtf), ("success",))
    g.conn(g.p(gtf), g.p(sink), ("success",))

    # (f) PutSQL with sql.args.N.*
    g = top.group("f putsql")
    gen = g.proc(
        "GenerateFlowFile",
        "gen",
        {
            "sql.args.1.type": "-5",
            "sql.args.1.value": "42",
            "sql.args.2.type": "12",
            "sql.args.2.value": "it's ok",
            "sql.args.3.type": "93",
            "sql.args.3.value": "2025-12-04 10:11:12.345",
            "sql.args.3.format": "yyyy-MM-dd HH:mm:ss.SSS",
        },
        period=ONCE,
    )
    putsql = g.proc(
        "PutSQL",
        "putsql",
        {
            "JDBC Connection Pool": ctrl,
            "SQL Statement": "INSERT INTO public.spike_putsql (n, s, ts) VALUES (?, ?, ?)",
        },
        auto=("failure", "retry"),
    )
    sink = g.proc("UpdateAttribute", "sink", auto=("success",))
    g.conn(g.p(gen), g.p(putsql), ("success",))
    g.conn(g.p(putsql), g.p(sink), ("success",))

    # (g) InvokeHTTP headers as attributes, (h) JsonTreeReader on JSONEachRow via PutDatabaseRecord
    g = top.group("gh clickhouse http")
    gen = g.proc(
        "GenerateFlowFile",
        "gen",
        {
            "Custom Text": (
                "SELECT number AS n, toDecimal64(number, 2) AS d, if(number = 1, NULL, toString(number)) AS s,"
                " toDateTime64('2025-12-04 10:11:12.345', 3, 'UTC') + number AS ts"
                " FROM numbers(3) FORMAT JSONEachRow SETTINGS output_format_json_quote_64bit_integers=0"
            )
        },
        period=ONCE,
    )
    http = g.proc(
        "InvokeHTTP",
        "clickhouse",
        {
            "HTTP Method": "POST",
            "HTTP URL": "#{ch_url}/?wait_end_of_query=1",
            "Request Username": "#{ch_user}",
            "Request Password": "#{ch_password}",
            "Request Content-Type": "text/plain",
            "Socket Read Timeout": "#{ch_read_timeout}",
        },
        auto=("Original", "Failure", "No Retry", "Retry"),
    )
    attrs = g.proc(
        "UpdateAttribute",
        "summary",
        {"read_rows": "${X-ClickHouse-Summary:jsonPath('$.read_rows')}"},
    )
    pdr = g.proc(
        "PutDatabaseRecord",
        "rows",
        {
            "Database Connection Pooling Service": ctrl,
            "Record Reader": reader,
            "Database Type": "PostgreSQL",
            "Statement Type": "INSERT",
            "Table Name": "spike_jsonrows",
            "Schema Name": "public",
            "Maximum Batch Size": "1000",
            "Rollback On Failure": "true",
        },
        auto=("failure", "retry"),
    )
    sink = g.proc("UpdateAttribute", "sink", auto=("success",))
    g.conn(g.p(gen), g.p(http), ("success",))
    g.conn(g.p(http), g.p(attrs), ("Response",))
    g.conn(g.p(attrs), g.p(pdr), ("success",))
    g.conn(g.p(pdr), g.p(sink), ("success",))

    # (i) ExecuteSQLRecord paging and streaming settings
    g = top.group("i paging")
    gen = g.proc("GenerateFlowFile", "gen", period=ONCE)
    q = g.proc(
        "ExecuteSQLRecord",
        "query",
        {
            "Database Connection Pooling Service": oltp,
            "Record Writer": writer,
            "SQL Query": "SELECT g AS n FROM generate_series(1, 25000) g",
            "Max Rows Per FlowFile": "10000",
            "Fetch Size": "1000",
            "Set Auto Commit": "false",
        },
        auto=("failure",),
    )
    sink = g.proc("UpdateAttribute", "sink", auto=("success",))
    g.conn(g.p(gen), g.p(q), ("success",))
    g.conn(g.p(q), g.p(sink), ("success",))

    # The remaining types the builder uses, wired like a stage group; not run by the check.
    g = top.group("template", **BARRIER)
    pin, pout = g.port("input", "in"), g.port("output", "out")
    req = g.proc(
        "HandleHttpRequest",
        "trigger",
        {
            "Listening Port": "#{trigger_port}",
            "Allowed Paths": "/cycles",
            "HTTP Context Map": httpctx,
            "Allow GET": "false",
            "Allow PUT": "false",
            "Allow DELETE": "false",
        },
    )
    ev = g.proc(
        "EvaluateJsonPath",
        "body",
        {"Destination": "flowfile-attribute", "mode": "$.mode"},
        auto=("failure", "unmatched"),
    )
    route = g.proc("RouteOnAttribute", "route", {"ok": "${mode:matches('^(full|incremental)$')}"})
    text = g.proc(
        "ReplaceText",
        "body 400",
        {
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Replacement Value": '{"error":"bad mode"}',
        },
        auto=("failure",),
    )
    resp = g.proc(
        "HandleHttpResponse",
        "respond",
        {"HTTP Status Code": "400", "HTTP Context Map": httpctx},
        auto=("success", "failure"),
    )
    retry = g.proc(
        "RetryFlowFile",
        "retry",
        {"Maximum Retries": "#{retry_max}"},
        auto=("retries_exceeded", "failure"),
    )
    g.conn(g.p(req), g.p(ev), ("success",))
    g.conn(g.p(ev), g.p(route), ("matched",))
    g.conn(g.p(route), g.p(text), ("unmatched",))
    g.conn(g.p(text), g.p(resp), ("success",))
    g.conn(g.p(route), (pout, g.id, "OUTPUT_PORT"), ("ok",))
    g.conn((pin, g.id, "INPUT_PORT"), g.p(retry))
    g.conn(g.p(retry), g.p(retry), ("retry",))
    # A port with no connection on the outside is invalid.
    tin = top.proc("GenerateFlowFile", "template gen", period=ONCE)
    tout = top.proc("UpdateAttribute", "template done", auto=("success",))
    top.conn(top.p(tin), (pin, g.id, "INPUT_PORT"), ("success",))
    top.conn((pout, g.id, "OUTPUT_PORT"), top.p(tout))

    for _path, flow in walk(c, top.id):
        for p in flow["processors"]:
            if p["component"]["name"] in ("sink", "hold"):
                disable(c, p["id"])
    return top.id


def export(c: Client, gid: str, path: Path) -> dict[str, Any]:
    body = c.get(f"/process-groups/{gid}/download")
    data = json.loads(body) if isinstance(body, str) else body
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return dict(data)


# -- (b)-(i) checks --------------------------------------------------------------------------------


def normalize(x: Any) -> Any:
    """Drop what changes on every upload: instance ids and the per-instance group id."""
    if isinstance(x, dict):
        return {
            k: normalize(v)
            for k, v in sorted(x.items())
            if k not in {"instanceIdentifier", "instanceGroupId", "latest"}
        }
    if isinstance(x, list):
        return sorted((normalize(v) for v in x), key=lambda v: json.dumps(v, sort_keys=True))
    return x


def diff(a: Any, b: Any, path: str = "") -> list[str]:
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        for k in sorted(set(a) | set(b)):
            out += diff(a.get(k), b.get(k), f"{path}.{k}")
        return out
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return [
            d for i, (x, y) in enumerate(zip(a, b, strict=True)) for d in diff(x, y, f"{path}[{i}]")
        ]
    return [] if a == b else [f"{path}: {str(a)[:80]} != {str(b)[:80]}"]


def find(c: Client, top: str, group: str) -> tuple[str, dict[str, str], dict[str, str]]:
    """(group id, processor name -> id, connection 'src->dst' -> id) for a scenario group."""
    for path, flow in walk(c, top):
        if path.endswith("/" + group):
            procs = {p["component"]["name"]: p["id"] for p in flow["processors"]}
            names = {p["id"]: p["component"]["name"] for p in flow["processors"]}
            conns = {}
            for k in flow["connections"]:
                s, d = k["component"]["source"], k["component"]["destination"]
                conns[f"{names.get(s['id'], s['name'])}->{names.get(d['id'], d['name'])}"] = k["id"]
            return flow["id"], procs, conns
    raise KeyError(group)


def run_status(c: Client, pid: str, state: str) -> None:
    rev = c.get(f"/processors/{pid}")["revision"]
    c.put(f"/processors/{pid}/run-status", json={"revision": rev, "state": state})


def queued(c: Client, conn_id: str) -> int:
    return int(
        c.get(f"/flow/connections/{conn_id}/status")["connectionStatus"]["aggregateSnapshot"][
            "flowFilesQueued"
        ]
    )


def listing(c: Client, conn_id: str) -> list[dict[str, Any]]:
    """Attributes of every FlowFile queued in a connection."""
    req = c.post(f"/flowfile-queues/{conn_id}/listing-requests")["listingRequest"]
    url = f"/flowfile-queues/{conn_id}/listing-requests/{req['id']}"
    done = c.poll(lambda: c.get(url)["listingRequest"], lambda r: r["finished"], "queue listing")
    c.delete(url)
    return [
        c.get(f"/flowfile-queues/{conn_id}/flowfiles/{f['uuid']}")["flowFile"]
        for f in done["flowFileSummaries"]
    ]


def content(c: Client, conn_id: str, uuid: str) -> str:
    return str(c.get(f"/flowfile-queues/{conn_id}/flowfiles/{uuid}/content"))


def start_group(c: Client, gid: str) -> None:
    c.put(f"/flow/process-groups/{gid}", json={"id": gid, "state": "RUNNING"})


def wait_queued(c: Client, conn_id: str, n: int, timeout_s: float = 90) -> int:
    return int(c.poll(lambda: queued(c, conn_id), lambda q: q >= n, f"{n} FlowFiles", timeout_s))


def check(c: Client) -> dict[str, Any]:
    out: dict[str, Any] = {}
    # (b) round trip through deploy, then download and compare.
    ref = json.loads(REFERENCE.read_text(encoding="utf-8"))
    top = deploy(c, REFERENCE, "local", Path("nifi/params/local.json"), start=False)
    again = export(c, top, Path("runs/nifi-spike/roundtrip.json"))
    # The upload gives the top group a new versioned id; map it back before comparing.
    again = json.loads(
        json.dumps(again).replace(
            again["flowContents"]["identifier"], ref["flowContents"]["identifier"]
        )
    )
    out["b_diff"] = diff(normalize(ref), normalize(again))[:40]

    # (c) barrier
    gid, _procs, conns = find(c, top, "c barrier")
    _bid, bprocs, bconns = find(c, top, "barrier")
    start_group(c, gid)
    wait_queued(c, bconns["split->hold"], 5)
    time.sleep(5)
    out["c_held"] = {
        "splits_waiting": queued(c, bconns["split->hold"]),
        "original_at_out_port": queued(c, bconns["split->out"]),
        "released_to_sink": queued(c, conns["out->sink"]),
    }
    run_status(c, bprocs["hold"], "STOPPED")
    run_status(c, bprocs["hold"], "RUNNING")
    wait_queued(c, conns["out->sink"], 1)
    out["c_after"] = {
        "splits_waiting": queued(c, bconns["split->hold"]),
        "released_to_sink": queued(c, conns["out->sink"]),
        "attrs": sorted(listing(c, conns["out->sink"])[0]["attributes"]),
    }

    # (d) Parquet to S3 to ClickHouse
    gid, _procs, conns = find(c, top, "d parquet")
    start_group(c, gid)
    wait_queued(c, conns["put->sink"], 4)
    ff = listing(c, conns["put->sink"])
    out["d_flowfiles"] = [
        {
            k: f["attributes"].get(k)
            for k in ("s3.key", "executesql.row.count", "mime.type", "fragment.count")
        }
        for f in ff
    ]
    chs = ClickHouseSettings()
    # filename= is relative to the collection's URL; overriding url= needs SHOW NAMED COLLECTIONS
    # SECRETS, which the nifi user does not have.
    glob = "spike/price_history/cycle=999/*.parquet"
    src = f"s3(raw_s3, filename='{glob}', format='Parquet')"
    with ch_connect(chs) as admin:
        admin.command("TRUNCATE TABLE raw.pricing__price_history")
    # As the nifi user, with exactly the statement shape PLAN 6.5 sends.
    with ch_connect(chs, "nifi") as ch:
        out["d_schema"] = ch.rows(f"DESCRIBE {src} SETTINGS use_hive_partitioning=0")
        ch.command(
            f"INSERT INTO raw.pricing__price_history SELECT *, 999, now64(3) FROM {src}"
            " SETTINGS use_hive_partitioning=0"
        )
    with ch_connect(chs) as ch:
        out["d_clickhouse"] = ch.rows(
            "SELECT count(), sum(price), countIf(valid_to IS NULL), countIf(cycle_id IS NULL),"
            " min(valid_from), max(valid_to) FROM raw.pricing__price_history"
        )
    o = OltpSettings()
    with psycopg.connect(o.conninfo(o.owner_user, o.owner_password)) as conn:
        out["d_oltp"] = conn.execute(
            "SELECT count(*), sum(price), count(*) FILTER (WHERE valid_to IS NULL),"
            " count(*) FILTER (WHERE cycle_id IS NULL), min(valid_from), max(valid_to)"
            " FROM pricing.price_history"
        ).fetchone()

    # (e) GenerateTableFetch
    gid, _procs, conns = find(c, top, "e table fetch")
    start_group(c, gid)
    time.sleep(8)
    ff = listing(c, conns["fetch->sink"])
    out["e_pages"] = len(ff)
    if ff:
        first = min(ff, key=lambda f: int(f["attributes"].get("fragment.index", 0)))
        out["e_first_sql"] = content(c, conns["fetch->sink"], first["uuid"])
        out["e_attrs"] = {
            k: v
            for k, v in first["attributes"].items()
            if k.startswith(("generatetablefetch", "fragment"))
        }

    # (f) PutSQL with sql.args
    gid, _procs, conns = find(c, top, "f putsql")
    start_group(c, gid)
    wait_queued(c, conns["putsql->sink"], 1)
    cs = ControlSettings()
    with psycopg.connect(cs.conninfo(cs.owner_user, cs.owner_password)) as conn:
        out["f_rows"] = [
            tuple(map(str, r)) for r in conn.execute("SELECT n, s, ts FROM public.spike_putsql")
        ]

    # (g) + (h)
    gid, _procs, conns = find(c, top, "gh clickhouse http")
    start_group(c, gid)
    wait_queued(c, conns["rows->sink"], 1, timeout_s=60)
    a = listing(c, conns["rows->sink"])[0]["attributes"]
    out["g_attrs"] = {
        k: v
        for k, v in a.items()
        if "ClickHouse" in k or k in ("read_rows", "invokehttp.status.code")
    }
    with psycopg.connect(cs.conninfo(cs.owner_user, cs.owner_password)) as conn:
        out["h_rows"] = [
            tuple(map(str, r))
            for r in conn.execute("SELECT n, d, s, ts FROM public.spike_jsonrows ORDER BY n")
        ]

    # (i) paging
    gid, _procs, conns = find(c, top, "i paging")
    start_group(c, gid)
    wait_queued(c, conns["query->sink"], 3)
    out["i_flowfiles"] = sorted(
        (
            f["attributes"].get("fragment.index"),
            f["attributes"].get("executesql.row.count"),
            f["attributes"].get("fragment.count"),
            f["size"],
        )
        for f in listing(c, conns["query->sink"])
    )
    return out


def prepare_control() -> None:
    cs = ControlSettings()
    with psycopg.connect(cs.conninfo(cs.owner_user, cs.owner_password)) as conn:
        conn.execute("DROP TABLE IF EXISTS public.spike_putsql, public.spike_jsonrows")
        conn.execute("CREATE TABLE public.spike_putsql (n bigint, s text, ts timestamp)")
        conn.execute(
            "CREATE TABLE public.spike_jsonrows (n bigint, d numeric(18,2), s text, ts timestamptz)"
        )
        conn.execute("GRANT USAGE ON SCHEMA public TO nifi")
        conn.execute("GRANT INSERT, SELECT ON public.spike_putsql, public.spike_jsonrows TO nifi")


def main() -> None:
    step = sys.argv[1] if len(sys.argv) > 1 else "build"
    with connect(NifiSettings()) as c:
        c.login()
        if step == "build":
            gid = build(c)
            export(c, gid, REFERENCE)
            print(f"exported {REFERENCE}")
        elif step == "check":
            prepare_control()
            print(json.dumps(check(c), indent=2, default=str))
        else:
            raise SystemExit(f"unknown step {step}")


if __name__ == "__main__":
    main()
