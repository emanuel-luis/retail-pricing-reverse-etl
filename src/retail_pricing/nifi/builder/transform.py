"""Group `30 transform`: one dbt build per cycle through the dbt runner (PLAN 6.5, 8.3).

One `transform.build` step (item `transform`): POST /runs, poll GET /runs/{id} until the job
ends (RetryFlowFile, at most `dbt_poll_max` polls `dbt_poll_penalty` apart), then upsert the node
results into `audit.dbt_node_result`. A failed build, a failed guardrail included, fails the
step, so the gate fails the cycle before any reverse load.
"""

from retail_pricing.nifi.builder.common import (
    CONTROL,
    attrs,
    body,
    event_sql,
    put_records,
    put_sql,
    query,
    stage,
)
from retail_pricing.nifi.builder.dsl import Group

# The runner is idempotent per cycle: a running or succeeded job of the cycle comes back as is.
# as_of is the cycle's business date, which registry.extract_plan stored in audit.cycle from
# this same `business_date` attribute.
RUN_REQUEST = '{"cycle_id": ${cycle_id}, "run_id": "${run_id}", "as_of": "${business_date}"}'
# ponytail: a penalty is not a parameter in a flow definition; this matches dbt_poll_penalty.
POLL_PENALTY = "10 sec"


def build(parent: Group) -> Group:
    s = stage(parent, "30 transform", "transform")
    g = s.g
    plan = query(
        g, "stage_plan", "select item from audit.stage_plan(${cycle_id}, 'transform')", CONTROL
    )
    g.connect(s.run, plan, "unmatched")
    s.guard_token(plan, "failure")
    split = s.split(plan)

    item = attrs(g, "item", {"item": "$.item"})
    started = put_sql(g, "build started", event_sql("transform.build", "started"))
    request = body(g, "run request", RUN_REQUEST)
    post = g.proc(
        "InvokeHTTP",
        "post run",
        {
            "HTTP Method": "POST",
            "HTTP URL": "#{dbt_runner_url}/runs",
            "Request Content-Type": "application/json",
        },
        auto=("Original",),
    )
    job = attrs(g, "job_id", {"job_id": "$.job_id"})
    poll = g.proc(
        "InvokeHTTP",
        "get run",
        {"HTTP Method": "GET", "HTTP URL": "#{dbt_runner_url}/runs/${job_id}"},
        auto=("Original",),
    )
    state = attrs(g, "job status", {"job_status": "$.status", "job_error": "$.error"})
    running = g.proc("RouteOnAttribute", "running", {"running": "${job_status:equals('running')}"})
    wait = g.proc(
        "RetryFlowFile",
        "poll again",
        {"Maximum Retries": "#{dbt_poll_max}", "Retry Attribute": "retry.30_transform.dbt_poll"},
        penalty=POLL_PENALTY,
    )
    timeout = g.proc(
        "UpdateAttribute",
        "poll timeout",
        {"step.error": "dbt job ${job_id} still running after #{dbt_poll_max} polls"},
    )
    results = g.proc(
        "InvokeHTTP",
        "get results",
        {"HTTP Method": "GET", "HTTP URL": "#{dbt_runner_url}/runs/${job_id}/results"},
        auto=("Original",),
    )
    nodes = put_records(
        g, "dbt_node_result", CONTROL, "audit.dbt_node_result", "UPSERT", "run_id,unique_id"
    )
    ok = g.proc("RouteOnAttribute", "built", {"ok": "${job_status:equals('succeeded')}"})
    failed = g.proc(
        "UpdateAttribute",
        "build failed",
        {"step.error": "dbt job ${job_id} ${job_status}: ${job_error:replaceNull('no detail')}"},
    )
    done = put_sql(
        g, "build succeeded", event_sql("transform.build", "succeeded"), auto=("success",)
    )

    g.connect(split, item, "success")
    g.connect(item, started, "matched")
    g.connect(item, s.step_failed, "unmatched", "failure")
    g.connect(started, request, "success")
    s.guard(started, "failure", "retry", step="transform.build")
    g.connect(request, post, "success")
    g.connect(request, s.step_failed, "failure")
    g.connect(post, job, "Response")
    s.guard(post, "Retry", "Failure", step="transform.build")
    g.connect(post, s.step_failed, "No Retry")  # 400, or 409 while another cycle builds
    g.connect(job, poll, "matched")
    g.connect(job, s.step_failed, "unmatched", "failure")
    g.connect(poll, state, "Response")
    s.guard(poll, "Retry", "Failure", step="transform.build")
    g.connect(poll, s.step_failed, "No Retry")
    g.connect(state, running, "matched")
    g.connect(state, s.step_failed, "unmatched", "failure")
    g.connect(running, wait, "running")
    g.connect(wait, poll, "retry")
    g.connect(wait, timeout, "retries_exceeded", "failure")
    g.connect(timeout, s.step_failed, "success")
    g.connect(running, results, "unmatched")
    g.connect(results, nodes, "Response")
    s.guard(results, "Retry", "Failure", step="transform.build")
    # 404: the build failed before dbt wrote run_results.json.
    g.connect(results, failed, "No Retry")
    g.connect(nodes, ok, "success")
    s.guard(nodes, "failure", "retry", step="transform.build")
    g.connect(ok, done, "ok")
    g.connect(ok, failed, "unmatched")
    g.connect(failed, s.step_failed, "success")
    s.guard(done, "failure", "retry", step="transform.build")
    return g
