"""Job lifecycle: validation, one-at-a-time execution, JSON persistence, results parsing."""

import json
import logging
import os
import re
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge

from retail_pricing.logging import cycle_id_var, run_id_var

log = logging.getLogger(__name__)

_SELECT_TOKEN = re.compile(r"^[A-Za-z0-9_.:+*@/,]+$")
_JOB_ID = re.compile(r"^[0-9a-f]{32}$")


class ValidationError(ValueError):
    """A request field is missing or malformed (HTTP 400)."""


class Busy(Exception):
    """Another cycle is running (HTTP 409)."""

    def __init__(self, job: dict[str, Any]) -> None:
        super().__init__(f"cycle {job['cycle_id']} is running")
        self.job = job


@dataclass(frozen=True)
class RunRequest:
    cycle_id: int
    run_id: str
    as_of: str
    select: tuple[str, ...] = ()


def parse_request(body: object) -> RunRequest:
    """Strict validation of the POST /runs body; nothing here ever reaches a shell."""
    if not isinstance(body, dict):
        raise ValidationError("body must be a JSON object")
    cycle_id = body.get("cycle_id")
    if isinstance(cycle_id, bool) or not isinstance(cycle_id, int) or cycle_id <= 0:
        raise ValidationError("cycle_id must be a positive integer")
    run_id = body.get("run_id")
    try:
        run_id = str(uuid.UUID(run_id if isinstance(run_id, str) else ""))
    except ValueError:
        raise ValidationError("run_id must be a UUID string") from None
    as_of = body.get("as_of")
    try:
        as_of = date.fromisoformat(as_of if isinstance(as_of, str) else "").isoformat()
    except ValueError:
        raise ValidationError("as_of must be an ISO date (YYYY-MM-DD)") from None
    select: tuple[str, ...] = ()
    if (raw := body.get("select")) is not None:
        tokens = raw.split() if isinstance(raw, str) and len(raw) <= 200 else []
        # A token never starts with "-": it could not become a dbt flag.
        if not tokens or not all(_SELECT_TOKEN.match(t) and t[0] != "-" for t in tokens):
            raise ValidationError("select must be dbt selector tokens of [A-Za-z0-9_.:+*@/,]")
        select = tuple(tokens)
    return RunRequest(cycle_id, run_id, as_of, select)


def parse_run_results(path: Path, cycle_id: int, run_id: str) -> list[dict[str, Any]]:
    """One object per node, shaped like audit.dbt_node_result."""
    doc = json.loads(path.read_text(encoding="utf-8"))
    out = []
    for r in doc.get("results", []):
        unique_id = r["unique_id"]
        out.append(
            {
                "run_id": run_id,
                "unique_id": unique_id,
                "cycle_id": cycle_id,
                "resource_type": unique_id.split(".", 1)[0],
                "status": r["status"],
                "failures": r.get("failures"),
                "execution_time_s": r.get("execution_time"),
                "rows_affected": (r.get("adapter_response") or {}).get("rows_affected"),
                "message": r.get("message"),
            }
        )
    return out


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _tail(path: Path, limit: int = 400) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[-limit:]


class JobManager:
    """Owns the state dir. Every public method is safe to call from handler threads."""

    def __init__(
        self,
        state_dir: Path,
        project_dir: Path,
        dbt_cmd: list[str],
        threads: int,
        registry: CollectorRegistry | None = None,
    ) -> None:
        self.state_dir = state_dir
        self.project_dir = project_dir
        self.dbt_cmd = dbt_cmd
        self.threads = threads
        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._workers: list[threading.Thread] = []
        (state_dir / "jobs").mkdir(parents=True, exist_ok=True)
        self.registry = registry or CollectorRegistry()
        self.m_jobs = Counter(
            "dbt_runner_jobs_total", "Finished jobs by status", ["status"], registry=self.registry
        )
        self.m_last = Gauge(
            "dbt_runner_last_job_duration_seconds",
            "Duration of the last finished job",
            registry=self.registry,
        )
        self.m_running = Gauge("dbt_runner_running", "1 while a job runs", registry=self.registry)
        for status in ("succeeded", "failed"):
            self.m_jobs.labels(status)
        self._load()

    def _path(self, job_id: str) -> Path:
        return self.state_dir / "jobs" / f"{job_id}.json"

    def _save(self, job: dict[str, Any]) -> None:
        tmp = self._path(job["job_id"]).with_suffix(".tmp")
        tmp.write_text(json.dumps(job), encoding="utf-8")
        tmp.replace(self._path(job["job_id"]))

    def _load(self) -> None:
        for f in sorted((self.state_dir / "jobs").glob("*.json")):
            job = json.loads(f.read_text(encoding="utf-8"))
            if job["status"] == "running":
                job.update(
                    status="failed", ended_at=_now(), error="runner restarted during the job"
                )
                self._save(job)
                log.warning("marked interrupted job failed", extra={"job_id": job["job_id"]})
            self._jobs[job["job_id"]] = job

    def get(self, job_id: str) -> dict[str, Any] | None:
        if not _JOB_ID.match(job_id):
            return None
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job else None

    def start(self, req: RunRequest) -> dict[str, Any]:
        """Idempotent per cycle: a running or succeeded job of the cycle is returned as is."""
        with self._lock:
            for job in self._jobs.values():
                if job["cycle_id"] == req.cycle_id and job["status"] in ("running", "succeeded"):
                    return dict(job)
            for job in self._jobs.values():
                if job["status"] == "running":
                    raise Busy(dict(job))
            job = {
                "job_id": uuid.uuid4().hex,
                "cycle_id": req.cycle_id,
                "run_id": req.run_id,
                "as_of": req.as_of,
                "select": list(req.select),
                "status": "running",
                "created_at": _now(),
                "ended_at": None,
                "duration_s": None,
                "returncode": None,
                "error": None,
                "counts": {},
            }
            self._jobs[job["job_id"]] = job
            self._save(job)
            self.m_running.set(1)
            worker = threading.Thread(target=self._run, args=(job["job_id"], req), daemon=True)
            self._workers.append(worker)
            worker.start()
            return dict(job)

    def join(self, timeout: float = 30) -> None:
        for w in self._workers:
            w.join(timeout)

    def target_path(self, cycle_id: int) -> Path:
        return self.state_dir / "target" / f"cycle={cycle_id}"

    def command(self, req: RunRequest) -> list[str]:
        variables = {"cycle_id": req.cycle_id, "run_id": req.run_id, "as_of": req.as_of}
        cmd = [
            *self.dbt_cmd,
            "build",
            "--project-dir",
            str(self.project_dir),
            # profiles.yml lives in the project and reads everything from env.
            "--profiles-dir",
            str(self.project_dir),
            "--vars",
            json.dumps(variables),
            "--target-path",
            str(self.target_path(req.cycle_id)),
            "--log-path",
            str(self.state_dir / "logs"),
            "--log-format",
            "json",
        ]
        return [*cmd, "--select", *req.select] if req.select else cmd

    def _run(self, job_id: str, req: RunRequest) -> None:
        cycle_id_var.set(req.cycle_id)
        run_id_var.set(req.run_id)
        started = time.monotonic()
        log.info("dbt build started", extra={"job_id": job_id})
        status, code, error = "failed", None, None
        try:
            env = {**os.environ, "DBT_THREADS": str(self.threads)}
            log_file = self.state_dir / "jobs" / f"{job_id}.log"
            with log_file.open("wb") as out:
                # Argument list, no shell: request text never reaches one.
                proc = subprocess.run(
                    self.command(req), stdout=out, stderr=subprocess.STDOUT, env=env, check=False
                )
            code = proc.returncode
            status = "succeeded" if code == 0 else "failed"
            if code != 0:
                error = f"dbt exited with code {code}: {_tail(log_file)}"
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            log.exception("dbt build crashed")
        duration = round(time.monotonic() - started, 3)
        counts: dict[str, int] = {}
        try:
            for r in self.results(req.cycle_id, req.run_id) or []:
                counts[r["status"]] = counts.get(r["status"], 0) + 1
        except (OSError, ValueError, KeyError) as exc:
            error = error or f"unreadable run_results.json: {exc}"
            status = "failed"
        with self._lock:
            job = self._jobs[job_id]
            job.update(
                status=status,
                returncode=code,
                error=error,
                ended_at=_now(),
                duration_s=duration,
                counts=counts,
            )
            self._save(job)
            self.m_jobs.labels(status).inc()
            self.m_last.set(duration)
            self.m_running.set(0)
        log.info(
            "dbt build finished",
            extra={"job_id": job_id, "status": status, "duration_s": duration},
        )

    def results(self, cycle_id: int, run_id: str) -> list[dict[str, Any]] | None:
        path = self.target_path(cycle_id) / "run_results.json"
        return parse_run_results(path, cycle_id, run_id) if path.exists() else None
