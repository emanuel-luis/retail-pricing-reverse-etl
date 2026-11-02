"""Stdlib HTTP server in front of the JobManager."""

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from retail_pricing.dbt_runner.jobs import Busy, JobManager, ValidationError, parse_request
from retail_pricing.settings import DbtRunnerSettings

log = logging.getLogger(__name__)
_MAX_BODY = 4096


class Handler(BaseHTTPRequestHandler):
    manager: JobManager  # set on the subclass built by make_server

    def _send(self, status: int, body: object, ctype: str = "application/json") -> None:
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format: str, *args: Any) -> None:
        log.info("http", extra={"request": format % args})

    def do_GET(self) -> None:
        parts = self.path.split("?")[0].strip("/").split("/")
        if parts == ["healthz"]:
            self._send(200, {"status": "ok"})
        elif parts == ["metrics"]:
            self._send(200, generate_latest(self.manager.registry), CONTENT_TYPE_LATEST)
        elif parts[0] == "runs" and len(parts) in (2, 3) and (job := self.manager.get(parts[1])):
            if len(parts) == 2:
                self._send(200, job)
            elif parts[2] != "results":
                self._send(404, {"error": "not found"})
            elif job["status"] == "running":
                self._send(409, {"error": "job still running"})
            elif (res := self.manager.results(job["cycle_id"], job["run_id"])) is None:
                self._send(404, {"error": "no run_results.json for this job"})
            else:
                self._send(200, res)
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path.split("?")[0].rstrip("/") != "/runs":
            return self._send(404, {"error": "not found"})
        try:
            length = int(self.headers.get("Content-Length", ""))
            if not 0 < length <= _MAX_BODY:
                raise ValueError
            req = parse_request(json.loads(self.rfile.read(length)))
        except (ValueError, ValidationError) as exc:
            return self._send(400, {"error": str(exc) or "body must be JSON of at most 4096 bytes"})
        try:
            job = self.manager.start(req)
        except Busy as exc:
            return self._send(409, {"error": str(exc), "job_id": exc.job["job_id"]})
        self._send(202, {"job_id": job["job_id"]})


def make_server(settings: DbtRunnerSettings) -> ThreadingHTTPServer:
    manager = JobManager(
        settings.state_dir, settings.project_dir, [settings.dbt_bin], settings.threads
    )
    handler = type("BoundHandler", (Handler,), {"manager": manager})
    return ThreadingHTTPServer((settings.host, settings.port), handler)


def serve(settings: DbtRunnerSettings) -> None:
    server = make_server(settings)
    log.info("dbt runner listening", extra={"host": settings.host, "port": settings.port})
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
