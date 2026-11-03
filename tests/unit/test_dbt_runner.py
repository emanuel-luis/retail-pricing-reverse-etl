import json
import sys
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from prometheus_client import generate_latest

from retail_pricing.dbt_runner.jobs import (
    Busy,
    JobManager,
    ValidationError,
    parse_request,
    parse_run_results,
)
from retail_pricing.dbt_runner.server import Handler
from retail_pricing.settings import DbtRunnerSettings

FIX = Path(__file__).parents[1] / "fixtures" / "dbt_runner"
RUN = "11111111-1111-4111-8111-111111111111"


def body(**kw: Any) -> dict[str, Any]:
    return {"cycle_id": 7, "run_id": RUN, "as_of": "2025-12-04", **kw}


def manager(tmp_path: Path) -> JobManager:
    cmd = [sys.executable, "-I", str(FIX / "fake_dbt.py")]
    return JobManager(tmp_path / "state", tmp_path, cmd, 2)


def metrics(m: JobManager) -> str:
    return generate_latest(m.registry).decode()


def status_of(m: JobManager, job_id: str) -> dict[str, Any]:
    job = m.get(job_id)
    assert job is not None
    return job


@pytest.mark.parametrize(
    "patch",
    [
        {"cycle_id": 0},
        {"cycle_id": -1},
        {"cycle_id": True},
        {"cycle_id": "7"},
        {"run_id": "nope"},
        {"as_of": "2025-13-01"},
        {"as_of": 20251204},
        {"select": ""},
        {"select": "--profiles-dir x"},
        {"select": "model_a; rm -rf /"},
        {"select": "$(id)"},
        {"select": 5},
    ],
)
def test_invalid_requests_are_rejected(patch: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        parse_request(body(**patch))


def test_non_object_body_is_rejected() -> None:
    with pytest.raises(ValidationError):
        parse_request([1])


def test_valid_request_and_selectors() -> None:
    req = parse_request(body(select="tag:pricing+ stg_sales,dim_zone path:models/mart/*"))
    assert req.cycle_id == 7 and req.as_of == "2025-12-04"
    assert req.select == ("tag:pricing+", "stg_sales,dim_zone", "path:models/mart/*")
    assert parse_request(body()).select == ()


def test_results_parsing_matches_the_audit_table() -> None:
    rows = parse_run_results(FIX / "run_results.json", 7, RUN)
    assert [r["resource_type"] for r in rows] == ["model", "test", "seed"]
    assert set(rows[0]) == {
        "run_id",
        "unique_id",
        "cycle_id",
        "resource_type",
        "status",
        "failures",
        "execution_time_s",
        "rows_affected",
        "message",
    }
    assert rows[0]["rows_affected"] == 120 and rows[0]["failures"] is None
    assert rows[1]["rows_affected"] is None and rows[1]["failures"] == 0
    assert all(r["cycle_id"] == 7 and r["run_id"] == RUN for r in rows)


def test_job_lifecycle_and_idempotency(tmp_path: Path) -> None:
    m = manager(tmp_path)
    req = parse_request(body())
    job = m.start(req)
    assert m.start(req)["job_id"] == job["job_id"]
    m.join()
    done = status_of(m, job["job_id"])
    assert done["status"] == "succeeded"
    assert done["counts"] == {"success": 1, "pass": 1, "error": 1}
    # Succeeded cycle: the same job comes back, no second run.
    assert m.start(req)["job_id"] == job["job_id"]
    text = metrics(m)
    assert 'dbt_runner_jobs_total{status="succeeded"} 1.0' in text
    assert "dbt_runner_running 0.0" in text
    assert "dbt_runner_last_job_duration_seconds" in text


def test_other_cycle_is_refused_while_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = tmp_path / "gate"
    gate.touch()
    monkeypatch.setenv("FAKE_DBT_GATE", str(gate))
    m = manager(tmp_path)
    first = m.start(parse_request(body()))
    assert status_of(m, first["job_id"])["status"] == "running"
    with pytest.raises(Busy):
        m.start(parse_request(body(cycle_id=8)))
    assert "dbt_runner_running 1.0" in metrics(m)
    gate.unlink()
    m.join()
    assert m.start(parse_request(body(cycle_id=8)))["job_id"] != first["job_id"]
    m.join()


def test_failed_job_can_be_retried_and_keeps_its_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_DBT_FAIL", "1")
    m = manager(tmp_path)
    req = parse_request(body())
    job = m.start(req)
    m.join()
    failed = status_of(m, job["job_id"])
    assert failed["status"] == "failed" and "code 1" in failed["error"]
    assert m.start(req)["job_id"] != job["job_id"]
    m.join()
    assert 'dbt_runner_jobs_total{status="failed"} 2.0' in metrics(m)


def test_command_is_an_argument_list(tmp_path: Path) -> None:
    m = manager(tmp_path)
    cmd = m.command(parse_request(body(select="a b")))
    assert cmd[-3:] == ["--select", "a", "b"]
    variables = json.loads(cmd[cmd.index("--vars") + 1])
    assert variables == {"cycle_id": 7, "run_id": RUN, "as_of": "2025-12-04"}
    assert cmd[cmd.index("--target-path") + 1].endswith("cycle=7")
    assert cmd[cmd.index("--log-format") + 1] == "json"
    assert cmd[cmd.index("--profiles-dir") + 1] == cmd[cmd.index("--project-dir") + 1]


def test_restart_marks_running_jobs_failed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gate = tmp_path / "gate"
    gate.touch()
    monkeypatch.setenv("FAKE_DBT_GATE", str(gate))
    m = manager(tmp_path)
    job = m.start(parse_request(body()))
    again = manager(tmp_path)  # a new process reading the same state dir
    got = status_of(again, job["job_id"])
    assert got["status"] == "failed" and "restarted" in got["error"]
    gate.unlink()
    m.join()
    # The restarted manager treats the cycle as retryable.
    monkeypatch.delenv("FAKE_DBT_GATE")
    assert again.start(parse_request(body()))["job_id"] != job["job_id"]
    again.join()


@pytest.fixture
def http(tmp_path: Path) -> Iterator[tuple[str, JobManager]]:
    m = manager(tmp_path)
    handler = type("H", (Handler,), {"manager": m})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}", m
    server.shutdown()
    server.server_close()


def call(url: str, data: Any = None) -> tuple[int, bytes, str]:
    req = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode())
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read(), r.headers["Content-Type"]
    except urllib.error.HTTPError as e:
        return e.code, e.read(), e.headers["Content-Type"]


def test_http_api_end_to_end(http: tuple[str, JobManager]) -> None:
    url, m = http
    assert call(url + "/healthz")[0] == 200
    assert call(url + "/runs", {"cycle_id": 0})[0] == 400
    status, raw, _ = call(url + "/runs", body())
    assert status == 202
    job_id = json.loads(raw)["job_id"]
    m.join()
    assert json.loads(call(f"{url}/runs/{job_id}")[1])["status"] == "succeeded"
    status, raw, _ = call(f"{url}/runs/{job_id}/results")
    assert status == 200 and len(json.loads(raw)) == 3
    assert call(f"{url}/runs/{'0' * 32}")[0] == 404
    assert call(f"{url}/runs/..%2f..")[0] == 404
    status, raw, ctype = call(url + "/metrics")
    assert status == 200 and b"dbt_runner_jobs_total" in raw and ctype.startswith("text/plain")


def test_http_returns_409_for_another_running_cycle(
    http: tuple[str, JobManager], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url, m = http
    gate = tmp_path / "gate"
    gate.touch()
    monkeypatch.setenv("FAKE_DBT_GATE", str(gate))
    assert call(url + "/runs", body())[0] == 202
    assert call(url + "/runs", body(cycle_id=8))[0] == 409
    gate.unlink()
    m.join()


def test_settings_defaults() -> None:
    assert DbtRunnerSettings().threads == 2


def test_retry_never_reports_the_previous_attempts_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_DBT_FAIL", "1")
    m = manager(tmp_path)
    req = parse_request(body())
    first = m.start(req)
    m.join()
    assert status_of(m, first["job_id"])["counts"]  # the first attempt wrote results
    monkeypatch.setenv("FAKE_DBT_EARLY", "1")  # the retry fails before dbt writes anything
    second = m.start(req)
    m.join()
    retry = status_of(m, second["job_id"])
    assert retry["status"] == "failed" and retry["counts"] == {}
    assert m.results(req.cycle_id, req.run_id) is None
