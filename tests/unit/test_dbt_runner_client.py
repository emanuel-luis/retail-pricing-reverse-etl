import json
from typing import Any

import httpx
import pytest

from retail_pricing.dbt_runner import client


def _transport(statuses: list[str], results: list[dict[str, Any]]) -> httpx.MockTransport:
    seen = iter(statuses)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["cycle_id"] == 3 and body["as_of"] == "2025-12-04"
            return httpx.Response(202, json={"job_id": "j1"})
        if request.url.path.endswith("/results"):
            return httpx.Response(200, json=results)
        return httpx.Response(200, json={"job_id": "j1", "status": next(seen)})

    return httpx.MockTransport(handler)


def test_trigger_waits_and_reports_failed_nodes(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = _transport(
        ["running", "failed"],
        [
            {"unique_id": "model.a", "status": "success"},
            {"unique_id": "test.g01", "status": "fail", "message": "3 rows"},
        ],
    )
    real = httpx.Client
    monkeypatch.setattr(client.httpx, "Client", lambda **kw: real(transport=transport, **kw))
    out = client.trigger_and_wait("http://runner", 3, "2025-12-04", poll_s=0)
    assert out.job["status"] == "failed"
    assert [n["unique_id"] for n in out.failed_nodes] == ["test.g01"]


def test_trigger_raises_when_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = httpx.MockTransport(lambda r: httpx.Response(409, json={"error": "busy"}))
    real = httpx.Client
    monkeypatch.setattr(client.httpx, "Client", lambda **kw: real(transport=transport, **kw))
    with pytest.raises(client.RunnerError, match="409"):
        client.trigger_and_wait("http://runner", 3, "2025-12-04", poll_s=0)
