import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from retail_pricing.nifi import deploy as d
from retail_pricing.nifi.client import Client, NifiError

PARAMS = {"ch_url": "http://clickhouse:8123", "page_rows": 5000, "ch_password": {"env": "CH_PW"}}


def _params(tmp_path: Path) -> Path:
    p = tmp_path / "local.json"
    p.write_text(json.dumps(PARAMS), encoding="utf-8")
    return p


def test_load_params_reads_secrets_from_env(tmp_path: Path) -> None:
    values, sensitive = d.load_params(_params(tmp_path), {"CH_PW": "pw"})
    assert values == {"ch_url": "http://clickhouse:8123", "page_rows": "5000", "ch_password": "pw"}
    assert sensitive == {"ch_password"}


def test_load_params_lists_every_unset_secret(tmp_path: Path) -> None:
    with pytest.raises(NifiError, match=r"ch_password \(env CH_PW\)"):
        d.load_params(_params(tmp_path), {"CH_PW": ""})


class FakeNifi:
    """Just enough of the REST API for one deploy: an old group exists, the new one is valid."""

    def __init__(self, processors: list[dict[str, Any]]):
        self.processors = processors
        self.calls: list[tuple[str, str]] = []
        self.context_body: dict[str, Any] = {}
        self.upload: bytes = b""

    def __call__(self, r: httpx.Request) -> httpx.Response:
        path, m = r.url.path.removeprefix("/nifi-api"), r.method
        self.calls.append((m, path))
        ok = httpx.Response(200, json={})
        if path == "/access/token":
            return httpx.Response(201, text="tok")
        if path == "/flow/process-groups/root":
            return httpx.Response(200, json={"processGroupFlow": {"id": "root"}})
        if path == "/process-groups/root/process-groups" and m == "GET":
            return httpx.Response(
                200,
                json={"processGroups": [{"id": "old", "component": {"name": "retail-pricing"}}]},
            )
        if path == "/flow/process-groups/old/status":
            snap = {"aggregateSnapshot": {"activeThreadCount": 0}}
            return httpx.Response(200, json={"processGroupStatus": snap})
        if path.startswith("/process-groups/old/empty-all-connections-requests"):
            return httpx.Response(200, json={"dropRequest": {"id": "dr", "finished": True}})
        if path.endswith("/controller-services") and m == "GET":
            state = "DISABLED" if "/old/" in path else "ENABLED"
            return httpx.Response(
                200,
                json={"controllerServices": [{"component": {"name": "oltp-pool", "state": state}}]},
            )
        if path == "/process-groups/old" and m == "GET":
            return httpx.Response(200, json={"revision": {"version": 3}})
        if path == "/flow/parameter-contexts":
            return httpx.Response(200, json={"parameterContexts": []})
        if path == "/parameter-contexts" and m == "POST":
            self.context_body = json.loads(r.content)
            return httpx.Response(201, json={"id": "pc"})
        if path == "/process-groups/root/process-groups/upload":
            self.upload = r.content
            return httpx.Response(201, json={"id": "new"})
        if path == "/flow/process-groups/new" and m == "GET":
            flow = {
                "processGroups": [],
                "processors": self.processors,
                "inputPorts": [],
                "outputPorts": [],
            }
            crumb = {"breadcrumb": {"name": "retail-pricing"}}
            return httpx.Response(
                200, json={"processGroupFlow": {"breadcrumb": crumb, "flow": flow}}
            )
        if path == "/process-groups/new" and m == "GET":
            return httpx.Response(200, json={"revision": {"version": 0}, "component": {}})
        return ok


def _run(tmp_path: Path, fake: FakeNifi, monkeypatch: pytest.MonkeyPatch, **kw: Any) -> str:
    monkeypatch.setenv("CH_PW", "pw")
    flow = tmp_path / "flow.json"
    flow.write_text('{"flowContents": {}}', encoding="utf-8")
    c = Client(
        "https://127.0.0.1:1/nifi-api", "u", SecretStr("p"), transport=httpx.MockTransport(fake)
    )
    return d.deploy(c, flow, "ci", _params(tmp_path), wait_s=0, **kw)


def test_deploy_replaces_the_group_in_plan_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    proc = {"component": {"name": "p", "type": "x.UpdateAttribute", "validationStatus": "VALID"}}
    fake = FakeNifi([proc])
    assert _run(tmp_path, fake, monkeypatch) == "new"
    order = [c for c in fake.calls if c[0] != "GET"]
    assert (
        order.index(("PUT", "/flow/process-groups/old"))
        < order.index(("DELETE", "/process-groups/old"))
        < order.index(("POST", "/parameter-contexts"))
        < order.index(("POST", "/process-groups/root/process-groups/upload"))
        < order.index(("PUT", "/flow/process-groups/new/controller-services"))
        < order.index(("PUT", "/flow/process-groups/new"))
    )
    params = {
        p["parameter"]["name"]: p["parameter"] for p in fake.context_body["component"]["parameters"]
    }
    assert fake.context_body["component"]["name"] == "retail-pricing-ci"
    assert params["ch_password"] == {"name": "ch_password", "value": "pw", "sensitive": True}
    assert b'name="groupName"' in fake.upload and b"retail-pricing" in fake.upload


def test_deploy_fails_listing_each_invalid_component(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bad = {
        "component": {
            "name": "fetch",
            "type": "x.GenerateTableFetch",
            "validationStatus": "INVALID",
            "validationErrors": ["'Table Name' is required"],
        }
    }
    disabled = {
        "component": {
            "name": "sink",
            "type": "x.UpdateAttribute",
            "state": "DISABLED",
            "validationStatus": "INVALID",
            "validationErrors": ["stale"],
        }
    }
    fake = FakeNifi([bad, disabled])
    monkeypatch.setattr(d.time, "sleep", lambda s: None)
    monkeypatch.setattr(d, "settled_invalid", lambda c, gid: d.invalid_components(c, gid))
    with pytest.raises(NifiError) as e:
        _run(tmp_path, fake, monkeypatch)
    assert "/retail-pricing/fetch: 'Table Name' is required" in str(e.value)
    assert "sink" not in str(e.value)
    assert ("PUT", "/flow/process-groups/new") not in fake.calls  # never started
