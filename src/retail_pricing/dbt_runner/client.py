"""Host-side client of the dbt runner: start a cycle's build, wait for it, show the failures."""

import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx


class RunnerError(RuntimeError):
    """The runner refused the request or the build did not succeed."""


@dataclass(frozen=True)
class BuildOutcome:
    job: dict[str, Any]
    failed_nodes: list[dict[str, Any]]


def trigger_and_wait(
    base_url: str,
    cycle_id: int,
    as_of: str,
    select: str | None = None,
    timeout_s: float = 1800.0,
    poll_s: float = 5.0,
) -> BuildOutcome:
    """POST /runs, poll GET /runs/{id} until it ends, then fetch the node results."""
    body: dict[str, Any] = {"cycle_id": cycle_id, "run_id": str(uuid.uuid4()), "as_of": as_of}
    if select:
        body["select"] = select
    with httpx.Client(base_url=base_url, timeout=30.0) as http:
        resp = http.post("/runs", json=body)
        if resp.status_code != 202:
            raise RunnerError(f"POST /runs answered {resp.status_code}: {resp.text[:500]}")
        job_id = resp.json()["job_id"]
        deadline = time.monotonic() + timeout_s
        while True:
            job = http.get(f"/runs/{job_id}").raise_for_status().json()
            if job["status"] != "running":
                break
            if time.monotonic() > deadline:
                raise RunnerError(f"job {job_id} still running after {timeout_s:.0f} s")
            time.sleep(poll_s)
        results = http.get(f"/runs/{job_id}/results")
        nodes: list[dict[str, Any]] = results.json() if results.status_code == 200 else []
    failed = [n for n in nodes if n.get("status") in {"error", "fail", "runtime error"}]
    return BuildOutcome(job=job, failed_nodes=failed)
