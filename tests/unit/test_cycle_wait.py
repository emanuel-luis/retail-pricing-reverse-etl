from typing import Any

from retail_pricing.cycle import FAILED, SUCCEEDED, TIMEOUT, wait_cycle


def snap(status: str, **extra: Any) -> dict[str, Any]:
    run = {"job_id": "extract", "status": "running", "rows_out": None, "seconds": 1.0}
    return {"cycle_id": 7, "status": status, "runs": [run], "failed_steps": [], **extra}


def test_wait_returns_the_exit_code_and_prints_each_change_once() -> None:
    seq = iter([None, snap("running"), snap("running"), snap("succeeded")])
    lines: list[str] = []
    assert wait_cycle(7, 60, lambda _: next(seq), lines.append, every_s=0) == SUCCEEDED
    assert lines == ["cycle 7 running: extract running 1s", "cycle 7 succeeded: extract running 1s"]


def test_wait_lists_failed_steps() -> None:
    failed = snap(
        "failed",
        failed_stage="load",
        error="load run failed",
        failed_steps=[{"job_id": "load", "item": "pricing.zone_price", "error": "boom"}],
    )
    lines: list[str] = []
    assert wait_cycle(7, 60, lambda _: failed, lines.append, every_s=0) == FAILED
    assert lines[-1] == "  load pricing.zone_price: boom"


def test_wait_times_out() -> None:
    assert wait_cycle(7, 0, lambda _: snap("running"), lambda _: None, every_s=0) == TIMEOUT


def test_wait_stage_returns_once_the_stage_run_exists() -> None:
    from retail_pricing.cycle import wait_stage

    later = snap("running", runs=[{"job_id": "extract"}, {"job_id": "transform"}])
    seq = iter([None, snap("running"), later])
    assert wait_stage(7, "transform", 60, lambda _: next(seq), every_s=0) == SUCCEEDED
    assert wait_stage(7, "transform", 60, lambda _: snap("failed"), every_s=0) == FAILED
    assert wait_stage(7, "transform", 0, lambda _: snap("running"), every_s=0) == TIMEOUT
