"""Group `90 finalize`: close the cycle with audit.finish_cycle (PLAN 6.5)."""

from retail_pricing.nifi.builder.common import query, sq
from retail_pricing.nifi.builder.dsl import Group, Ref


def build(parent: Group) -> tuple[Group, Ref]:
    g = parent.group("90 finalize")
    pin = g.input()
    finish = query(
        g,
        "finish_cycle",
        "select cycle_id, status from audit.finish_cycle(${cycle_id},"
        " '${cycle.status:replaceNull('succeeded')}', NULLIF('" + sq("failed_stage") + "', ''),"
        " NULLIF('" + sq("cycle.error") + "', ''))",
        auto=("success",),
    )
    retry = g.proc(
        "RetryFlowFile",
        "finish_cycle retry",
        {"Maximum Retries": "#{retry_max}", "Retry Attribute": "retry.finish_cycle"},
        auto=("retries_exceeded", "failure"),
    )
    g.connect(pin, finish)
    g.connect(finish, retry, "failure")
    g.connect(retry, finish, "retry")
    return g, pin
