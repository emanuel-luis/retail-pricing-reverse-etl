"""Pieces every group shares: services, audit events, retries and the stage template (PLAN 6.5)."""

from dataclasses import dataclass

from retail_pricing.nifi.builder.dsl import Group, Ref, uid

TOP = "/retail-pricing"
CONTROL = uid(f"{TOP}/control-pool")
OLTP = uid(f"{TOP}/oltp-pool")
S3_CREDENTIALS = uid(f"{TOP}/s3-credentials")
JSON_READER = uid(f"{TOP}/json-reader")
JSON_WRITER = uid(f"{TOP}/json-writer")
PARQUET_WRITER = uid(f"{TOP}/parquet-writer")
HTTP_CONTEXT = uid(f"{TOP}/http-context")

# The first error text a failed processor left on the FlowFile: our own step.error, then the
# attributes ExecuteSQLRecord, PutDatabaseRecord, PutS3Object and InvokeHTTP (a non-2xx body)
# write on failure.
_ERROR = (
    "step.error:replaceNull(${executesql.error.message:replaceNull("
    "${putdatabaserecord.error:replaceNull("
    "${s3.errorMessage:replaceNull(${s3.exception:replaceNull("
    "${invokehttp.response.body:replaceNull("
    "${invokehttp.status.message:replaceNull('see the NiFi bulletins')})})})})})})"
)
ERROR_TEXT = "${" + _ERROR + "}"
ERROR_SQL = "${" + _ERROR + """:replace("'", "''")}"""  # inside a SQL string literal


def sq(attr: str) -> str:
    """An attribute as the inside of a SQL string literal (quotes doubled)."""
    return "${" + attr + """:replace("'", "''")}"""


def add_services(top: Group) -> None:
    """PLAN 6.4; ids are fixed by name, so processors refer to them by the constants above."""
    pool = {
        "Database Driver Class Name": "org.postgresql.Driver",
        "Database Driver Locations": "#{jdbc_driver_dir}",
        "Validation Query": "select 1",
    }
    for name, db, size in (("oltp-pool", "oltp", "8"), ("control-pool", "control", "4")):
        top.service(
            "DBCPConnectionPool",
            name,
            {
                **pool,
                "Database Connection URL": f"#{{{db}_jdbc_url}}",
                "Database User": f"#{{{db}_user}}",
                "Password": f"#{{{db}_password}}",
                "Max Total Connections": size,
            },
        )
    top.service(
        "AWSCredentialsProviderControllerService",
        "s3-credentials",
        {"Access Key ID": "#{s3_access_key}", "Secret Access Key": "#{s3_secret_key}"},
    )
    top.service("JsonTreeReader", "json-reader", {})
    top.service("JsonRecordSetWriter", "json-writer", {"Output Grouping": "output-array"})
    top.service(
        "ParquetRecordSetWriter",
        "parquet-writer",
        {"Compression Type": "SNAPPY", "Row Group Size": "32 MB"},
    )
    top.service("StandardHttpContextMap", "http-context", {})


def query(g: Group, name: str, sql: str, pool: str = CONTROL, auto: tuple[str, ...] = ()) -> Ref:
    """ExecuteSQLRecord with a fixed statement and a JSON array result (functions on a token)."""
    return g.proc(
        "ExecuteSQLRecord",
        name,
        {
            "Database Connection Pooling Service": pool,
            "Record Writer": JSON_WRITER,
            "SQL Query": sql,
        },
        auto=auto,
    )


def attrs(g: Group, name: str, paths: dict[str, str], auto: tuple[str, ...] = ()) -> Ref:
    """EvaluateJsonPath into attributes."""
    return g.proc(
        "EvaluateJsonPath", name, {"Destination": "flowfile-attribute", **paths}, auto=auto
    )


def put_sql(g: Group, name: str, sql: str, auto: tuple[str, ...] = ()) -> Ref:
    # SplitJson leaves fragment.* attributes behind: as a fragmented transaction PutSQL would
    # wait for every sibling, so that support is off. Batch Size 1: each step event is its own
    # statement, so one refused event (a constraint, a trigger, a lost connection) never fails
    # the events of other items that happened to share its batch. Step events are few.
    return g.proc(
        "PutSQL",
        name,
        {
            "JDBC Connection Pool": CONTROL,
            "SQL Statement": sql,
            "Support Fragmented Transactions": "false",
            "Batch Size": "1",
        },
        auto=auto,
    )


def event_sql(
    step_name: str, event: str, rows: str = "NULL", bytes_: str = "NULL", detail: str = "NULL"
) -> str:
    return (
        "INSERT INTO audit.step_event (run_id, step_id, step_name, item, event, rows, bytes,"
        f" detail) VALUES ('${{run_id}}', '${{step_id}}', '{step_name}', '{sq('item')}',"
        f" '{event}', {rows}, {bytes_}, {detail})"
    )


@dataclass
class Stage:
    """A stage group from the PLAN 6.5 template, ready for its plan query and item work."""

    g: Group
    job: str
    run: Ref  # RouteOnAttribute: `unmatched` carries run_id on to the plan
    out: Ref
    token_failed: Ref  # UpdateAttribute: stage.error, then the out port
    step_failed: Ref  # PutSQL step_event failed for the item FlowFile (retried, then logged)

    def guard_token(self, proc: Ref, *rels: str) -> None:
        """Retry a token-level processor; when retries run out the token leaves with an error."""
        retry = retry_of(self.g, proc)
        self.g.connect(proc, retry, *rels)
        self.g.connect(retry, proc, "retry")
        self.g.connect(retry, self.token_failed, "retries_exceeded", "failure")

    def guard(self, proc: Ref, *rels: str, step: str) -> None:
        """Retry an item processor: each retry is a `retried` event, then `failed` at the end."""
        retry = retry_of(self.g, proc)
        retried = put_sql(
            self.g,
            f"{proc.name} retried",
            event_sql(step, "retried", detail=f"jsonb_build_object('error', '{ERROR_SQL}')"),
        )
        self.g.connect(proc, retry, *rels)
        self.g.connect(retry, retried, "retry")
        # The audit row is best effort: the FlowFile goes back to the processor either way.
        self.g.connect(retried, proc, "success", "failure", "retry")
        self.g.connect(retry, self.step_failed, "retries_exceeded", "failure")

    def split(self, plan: Ref, path: str = "$.*", rel: str = "success") -> Ref:
        """SplitJson the plan: the original leaves when the group is empty (Batch Output).

        Returns an UpdateAttribute whose `success` is each item, with its step_id and a stand-in
        item name already set, so every failure path can write a valid step event. The token's
        last SQL error is dropped so items never report it as theirs.
        """
        split = self.g.proc("SplitJson", "split plan", {"JsonPath Expression": path})
        item = self.g.proc(
            "UpdateAttribute",
            "item step",
            {
                "step_id": "${UUID()}",
                "item": "plan item ${fragment.index}",
                "Delete Attributes Expression": "executesql\\.error\\.message",
            },
        )
        self.g.connect(plan, split, rel)
        self.g.connect(split, item, "split")
        self.g.connect(split, self.out, "original")
        self.g.connect(split, self.token_failed, "failure")
        return item


def retry_of(g: Group, proc: Ref) -> Ref:
    """A RetryFlowFile for `proc`. Its counter is `retry.<group>.<processor>`: RetryFlowFile
    refuses a counter another one set, and tokens carry attributes from group to group."""

    def safe(name: str) -> str:
        return "".join(ch if ch.isalnum() else "_" for ch in name)

    return g.proc(
        "RetryFlowFile",
        f"{proc.name} retry",
        {
            "Maximum Retries": "#{retry_max}",
            "Retry Attribute": f"retry.{safe(g.name)}.{safe(proc.name)}",
        },
    )


def stage(parent: Group, name: str, job: str) -> Stage:
    """in -> start_run -> run_id (a disabled job leaves at once); failures leave the token."""
    g = parent.group(name, barrier=True)
    pin, out = g.input(), g.output()
    start = query(
        g,
        "start_run",
        f"select run_id::text as run_id, enabled from audit.start_run(${{cycle_id}}, '{job}')",
    )
    run = attrs(
        g,
        "run_id",
        {"run_id": "$[0].run_id", f"{job}_run_id": "$[0].run_id", "enabled": "$[0].enabled"},
    )
    enabled = g.proc("RouteOnAttribute", "enabled", {"disabled": "${enabled:equals('false')}"})
    token_failed = g.proc(
        "UpdateAttribute", "token failed", {"stage.error": f"{job}: {ERROR_TEXT}"}
    )
    step_failed = put_sql(
        g,
        "step failed",
        event_sql(job, "failed", detail=f"jsonb_build_object('error', '{ERROR_SQL}')"),
        auto=("success",),
    )
    # When even the failed event cannot be written, say so in the NiFi log; end_run still fails
    # the run, because the item's planned step is missing or open.
    lost = g.proc(
        "LogMessage",
        "step failed not recorded",
        {
            "Log Level": "error",
            "Log Message": "failed step event not recorded: run ${run_id} step ${step_id}"
            f" item ${{item}}: {ERROR_TEXT}",
        },
        auto=("success",),
    )
    step_failed_retry = retry_of(g, step_failed)
    g.connect(step_failed, step_failed_retry, "failure", "retry")
    g.connect(step_failed_retry, step_failed, "retry")
    g.connect(step_failed_retry, lost, "retries_exceeded", "failure")
    s = Stage(g, job, enabled, out, token_failed, step_failed)
    g.connect(pin, start)
    g.connect(start, run, "success")
    s.guard_token(start, "failure")
    g.connect(run, enabled, "matched")
    g.connect(run, token_failed, "unmatched", "failure")
    g.connect(enabled, out, "disabled")
    g.connect(token_failed, out, "success")
    return s


def clickhouse(g: Group, name: str, query_params: str = "") -> Ref:
    """InvokeHTTP POST of the FlowFile content (one statement) to ClickHouse as the nifi user.

    `Original` is dropped: the Response FlowFile carries the attributes on.
    """
    return g.proc(
        "InvokeHTTP",
        name,
        {
            "HTTP Method": "POST",
            "HTTP URL": "#{ch_url}/" + (f"?{query_params}" if query_params else ""),
            "Request Username": "#{ch_user}",
            "Request Password": "#{ch_password}",
            "Request Content-Type": "text/plain",
            "Socket Read Timeout": "#{ch_read_timeout}",
        },
        auto=("Original",),
    )


def body(g: Group, name: str, text: str) -> Ref:
    """ReplaceText: the FlowFile content becomes `text` (Expression Language allowed)."""
    return g.proc(
        "ReplaceText",
        name,
        {
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Replacement Value": text,
        },
    )


def put_records(
    g: Group,
    name: str,
    pool: str,
    table: str,
    statement: str,
    keys: str | None = None,
    *,
    rollback: bool = False,
    ignore_unmatched_columns: bool = False,
) -> Ref:
    """PutDatabaseRecord of the JSON content (`json-reader`) into `schema.table` on Postgres."""
    schema, name_ = table.split(".")
    props = {
        "Database Connection Pooling Service": pool,
        "Database Type": "PostgreSQL",
        "Record Reader": JSON_READER,
        "Statement Type": statement,
        "Schema Name": schema,
        "Table Name": name_,
        "Maximum Batch Size": "1000",
    }
    if keys:
        props["Update Keys"] = keys
    if rollback:
        props["Rollback On Failure"] = "true"
    if ignore_unmatched_columns:
        props["Unmatched Column Behavior"] = "Ignore Unmatched Columns"
    return g.proc("PutDatabaseRecord", name, props)
