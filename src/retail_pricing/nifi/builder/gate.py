"""Parent-level gates between stage groups (PLAN 6.5): end_run, then route on its status."""

from retail_pricing.nifi.builder.common import attrs, query, retry_of
from retail_pricing.nifi.builder.dsl import Group, Ref


def _retry(g: Group, proc: Ref, failed: Ref) -> None:
    retry = retry_of(g, proc)
    g.connect(proc, retry, "failure")
    g.connect(retry, proc, "retry")
    g.connect(retry, failed, "retries_exceeded", "failure")


def build(
    parent: Group, job: str, stage_out: Ref, finalize: Ref, then: tuple[str, str] | None = None
) -> Ref:
    """Gate after the stage of `job`. Returns the processor whose `success` leaves the gate.

    A token that already failed in the stage (stage.error), a run that ended failed, or a gate
    query that ran out of retries marks the cycle failed and goes to `finalize`. `then` (name, sql) runs
    after a good run (the load gate commits the watermarks there).
    """
    p = f"gate {job}"
    failed = parent.proc(
        "UpdateAttribute",
        f"{p}: cycle failed",
        {
            "cycle.status": "failed",
            "failed_stage": job,
            "cycle.error": "${stage.error:replaceNull('"
            + job
            + " run failed: see audit.job_run')}",
        },
    )
    check = parent.proc(
        "RouteOnAttribute", f"{p}: token", {"failed": "${stage.error:isEmpty():not()}"}
    )
    end_run = query(
        parent,
        f"{p}: end_run",
        f"select status, failed_steps, open_steps from audit.end_run('${{{job}_run_id}}'::uuid)",
    )
    status = attrs(
        parent,
        f"{p}: status",
        {
            "stage_status": "$[0].status",
            "failed_steps": "$[0].failed_steps",
            "open_steps": "$[0].open_steps",
        },
    )
    ok = parent.proc(
        "RouteOnAttribute",
        f"{p}: ok",
        {"ok": "${stage_status:equals('succeeded'):or(${stage_status:equals('skipped')})}"},
    )
    parent.connect(stage_out, check)
    parent.connect(check, failed, "failed")
    parent.connect(check, end_run, "unmatched")
    parent.connect(end_run, status, "success")
    _retry(parent, end_run, failed)
    parent.connect(status, ok, "matched")
    parent.connect(status, failed, "unmatched", "failure")
    parent.connect(ok, failed, "unmatched")
    parent.connect(failed, finalize, "success")
    if then is None:
        return ok  # callers connect its relationship "ok"
    after = query(parent, f"{p}: {then[0]}", then[1])
    parent.connect(ok, after, "ok")
    _retry(parent, after, failed)
    return after  # callers connect its relationship "success"
