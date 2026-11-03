"""Group `00 trigger`: the HTTP path and the schedule path start a cycle (PLAN 6.5)."""

from retail_pricing.nifi.builder.common import CONTROL, HTTP_CONTEXT, attrs, query
from retail_pricing.nifi.builder.dsl import Group, Ref


def _respond(
    g: Group, name: str, status: str, body: str, auto: tuple[str, ...] = ("success", "failure")
) -> tuple[Ref, Ref]:
    text = g.proc(
        "ReplaceText",
        f"{name} body",
        {
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Replacement Value": body,
        },
        auto=("failure",),
    )
    resp = g.proc(
        "HandleHttpResponse",
        f"{name} response",
        {
            "HTTP Status Code": status,
            "HTTP Context Map": HTTP_CONTEXT,
            "Content-Type": "application/json",
        },
        auto=auto,
    )
    g.connect(text, resp, "success")
    return text, resp


def build(parent: Group) -> Group:
    g = parent.group("00 trigger")
    out = g.output("cycle")

    # HTTP: POST /cycles {"mode": "full|incremental", "rerun_cycle_id": null|N}
    req = g.proc(
        "HandleHttpRequest",
        "POST /cycles",
        {
            "Listening Port": "#{trigger_port}",
            "Allowed Paths": "/cycles",
            "HTTP Context Map": HTTP_CONTEXT,
            "Allow GET": "false",
            "Allow PUT": "false",
            "Allow DELETE": "false",
        },
    )
    body = attrs(g, "request", {"mode": "$.mode", "rerun_cycle_id": "$.rerun_cycle_id"})
    valid = g.proc(
        "RouteOnAttribute",
        "valid",
        {
            "valid": "${mode:matches('full|incremental'):and("
            "${rerun_cycle_id:replaceNull(''):matches('[0-9]*')})}"
        },
    )
    bad, _ = _respond(
        g,
        "bad request",
        "400",
        '{"error": "mode must be full or incremental and rerun_cycle_id a cycle id or null"}',
    )
    start = query(
        g,
        "start_cycle",
        "select cycle_id, status from registry.start_cycle('${mode}', 'http',"
        " ${rerun_cycle_id:isEmpty():ifElse('NULL', ${rerun_cycle_id})})",
    )
    refused, _ = _respond(
        g, "refused", "500", '{"error": "${executesql.error.message:escapeJson()}"}'
    )
    started = attrs(g, "cycle_id", {"cycle_id": "$[0].cycle_id", "start_status": "$[0].status"})
    ok, resp = _respond(
        g,
        "cycle",
        "${start_status:equals('busy'):ifElse('409', '202')}",
        '{"cycle_id": ${cycle_id}, "status": "${start_status}"}',
        auto=(),
    )
    go = g.proc(
        "RouteOnAttribute",
        "started",
        {"started": "${start_status:equals('busy'):not()}"},
        auto=("unmatched",),
    )
    g.connect(req, body, "success")
    g.connect(body, valid, "matched")
    g.connect(body, bad, "unmatched", "failure")
    g.connect(valid, start, "valid")
    g.connect(valid, bad, "unmatched")
    g.connect(start, started, "success")
    g.connect(start, refused, "failure")
    g.connect(started, ok, "matched")
    g.connect(started, refused, "unmatched", "failure")
    # The cycle row exists once start_cycle returned: it runs even if the client went away.
    g.connect(resp, go, "success", "failure")
    g.connect(go, out, "started")

    # Schedule: every minute ask the registry whether a cycle is due.
    tick = g.proc("GenerateFlowFile", "every minute", period="60 sec")
    due = query(
        g,
        "start_cycle_if_due",
        "select cycle_id from registry.start_cycle_if_due()",
        CONTROL,
        auto=("failure",),
    )
    route = g.proc(
        "RouteOnAttribute",
        "due",
        {"due": "${executesql.row.count:gt(0)}"},
        auto=("unmatched",),
    )
    sched = attrs(g, "scheduled cycle_id", {"cycle_id": "$[0].cycle_id"}, ("unmatched", "failure"))
    g.connect(tick, due, "success")
    g.connect(due, route, "success")
    g.connect(route, sched, "due")
    g.connect(sched, out, "matched")
    return g
