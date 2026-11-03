"""`retail-pricing nifi deploy`: docs/PLAN.md 6.1 steps 1-8 for one flow-definition JSON file."""

import json
import logging
import os
import socket
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

from retail_pricing.nifi.client import Client, NifiError
from retail_pricing.settings import ControlSettings

log = logging.getLogger(__name__)

GROUP = "retail-pricing"


# -- parameters ----------------------------------------------------------------------------------


def load_params(
    path: Path, environ: Mapping[str, str | None] | None = None
) -> tuple[dict[str, str], set[str]]:
    """Read `nifi/params/<env>.json`: plain strings, or `{"env": "VAR"}` for a sensitive value.

    Sensitive values come from the environment, then `.env`. Every unset one is listed at once.
    """
    if environ is None:
        environ = {**dotenv_values(".env"), **os.environ}
    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    values: dict[str, str] = {}
    sensitive: set[str] = set()
    missing: list[str] = []
    for name, spec in raw.items():
        if isinstance(spec, dict):
            sensitive.add(name)
            value = environ.get(spec["env"])
            if not value:
                missing.append(f"{name} (env {spec['env']})")
                continue
            values[name] = value
        else:
            values[name] = str(spec)
    if missing:
        raise NifiError(f"sensitive parameters without a value: {', '.join(missing)}")
    return values, sensitive


def upsert_parameter_context(
    c: Client, name: str, values: dict[str, str], sensitive: set[str]
) -> str:
    """Create the context, or update it through an update request (which restarts users)."""
    params = [
        {"parameter": {"name": k, "value": v, "sensitive": k in sensitive}}
        for k, v in sorted(values.items())
    ]
    found = [
        pc
        for pc in c.get("/flow/parameter-contexts")["parameterContexts"]
        if pc["component"]["name"] == name
    ]
    if not found:
        body = {
            "revision": {"version": 0, "clientId": c.client_id},
            "component": {"name": name, "parameters": params},
        }
        return str(c.post("/parameter-contexts", json=body)["id"])
    pc = found[0]
    pc_id = pc["id"]
    req = c.post(
        f"/parameter-contexts/{pc_id}/update-requests",
        json={
            "revision": pc["revision"],
            "id": pc_id,
            "component": {"id": pc_id, "name": name, "parameters": params},
        },
    )
    url = f"/parameter-contexts/{pc_id}/update-requests/{req['request']['requestId']}"
    done = c.poll(lambda: c.get(url), lambda r: r["request"]["complete"], "parameter update")
    c.delete(url)
    if reason := done["request"].get("failureReason"):
        raise NifiError(f"parameter context {name} update failed: {reason}")
    return str(pc_id)


# -- groups --------------------------------------------------------------------------------------


def root_id(c: Client) -> str:
    return str(c.get("/flow/process-groups/root")["processGroupFlow"]["id"])


def child_group(c: Client, parent: str, name: str) -> dict[str, Any] | None:
    for pg in c.get(f"/process-groups/{parent}/process-groups")["processGroups"]:
        if pg["component"]["name"] == name:
            return dict(pg)
    return None


def _services(c: Client, gid: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = c.get(
        f"/flow/process-groups/{gid}/controller-services",
        params={"includeAncestorGroups": "false", "includeDescendantGroups": "true"},
    )["controllerServices"]
    return out


def _set_services(c: Client, gid: str, state: str) -> None:
    c.put(f"/flow/process-groups/{gid}/controller-services", json={"id": gid, "state": state})


def remove_group(c: Client, gid: str) -> None:
    """Stop, empty, disable services, then delete (step 2)."""
    c.put(f"/flow/process-groups/{gid}", json={"id": gid, "state": "STOPPED"})
    c.poll(
        lambda: c.get(f"/flow/process-groups/{gid}/status", params={"recursive": "false"}),
        lambda s: s["processGroupStatus"]["aggregateSnapshot"]["activeThreadCount"] == 0,
        "running threads to stop",
    )
    req = c.post(f"/process-groups/{gid}/empty-all-connections-requests")
    url = f"/process-groups/{gid}/empty-all-connections-requests/{req['dropRequest']['id']}"
    c.poll(lambda: c.get(url), lambda r: r["dropRequest"]["finished"], "queues to empty")
    c.delete(url)
    _set_services(c, gid, "DISABLED")
    c.poll(
        lambda: _services(c, gid),
        lambda svcs: all(s["component"]["state"] == "DISABLED" for s in svcs),
        "controller services to disable",
    )
    rev = c.get(f"/process-groups/{gid}")["revision"]["version"]
    c.delete(f"/process-groups/{gid}", params={"version": rev, "clientId": c.client_id})


def upload(c: Client, parent: str, flow: bytes, name: str) -> str:
    """POST the definition as multipart (step 4); returns the new group id."""
    pg = c.post(
        f"/process-groups/{parent}/process-groups/upload",
        files={"file": ("flow.json", flow, "application/json")},
        data={"groupName": name, "positionX": "0", "positionY": "0", "clientId": c.client_id},
    )
    return str(pg["id"])


def walk(c: Client, gid: str, path: str = "") -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield (group path, processGroupFlow.flow) for the group and every descendant."""
    flow = c.get(f"/flow/process-groups/{gid}")["processGroupFlow"]
    here = f"{path}/{flow['breadcrumb']['breadcrumb']['name']}"
    yield here, {"id": gid, **flow["flow"]}
    for child in flow["flow"]["processGroups"]:
        yield from walk(c, child["id"], here)


def bind_context(c: Client, gid: str, pc_id: str) -> None:
    pg = c.get(f"/process-groups/{gid}")
    if (pg["component"].get("parameterContext") or {}).get("id") == pc_id:
        return
    c.put(
        f"/process-groups/{gid}",
        json={
            "revision": pg["revision"],
            "component": {"id": gid, "parameterContext": {"id": pc_id}},
        },
    )


def _errors(entity: dict[str, Any]) -> list[str]:
    comp = entity.get("component", {})
    if comp.get("validationStatus") == "INVALID" or comp.get("validationErrors"):
        return [str(e) for e in comp.get("validationErrors") or ["INVALID"]]
    return []


def invalid_components(c: Client, gid: str) -> list[str]:
    """One line per invalid processor, port or controller service in the tree (step 6).

    DISABLED components are skipped: they never run, and NiFi does not revalidate them, so one
    uploaded before its connection existed keeps a stale "no upstream connection" error.
    """
    lines = []
    for path, flow in walk(c, gid):
        for kind in ("processors", "inputPorts", "outputPorts"):
            for e in flow[kind]:
                if e["component"].get("state") == "DISABLED":
                    continue
                lines += [f"{path}/{e['component']['name']}: {err}" for err in _errors(e)]
    for s in _services(c, gid):
        lines += [f"service {s['component']['name']}: {err}" for err in _errors(s)]
    return lines


def settled_invalid(
    c: Client, gid: str, timeout_s: float = 20.0, every_s: float = 2.0
) -> list[str]:
    """Invalid components once validation settles.

    NiFi validates in the background: right after an upload a component can still report the
    state it had before its connections existed, so a non-empty list is re-read until it clears
    or `timeout_s` passes.
    """
    deadline = time.monotonic() + timeout_s
    while (bad := invalid_components(c, gid)) and time.monotonic() < deadline:
        time.sleep(every_s)
    return bad


def enable_services(c: Client, gid: str, timeout_s: float = 120.0) -> None:
    """Enable every service and wait (step 5); an INVALID one fails with its errors."""
    _set_services(c, gid, "ENABLED")

    def settled(svcs: list[dict[str, Any]]) -> bool:
        return all(s["component"]["state"] == "ENABLED" or _errors(s) for s in svcs)

    svcs = c.poll(lambda: _services(c, gid), settled, "controller services", timeout_s)
    bad = [
        f"service {s['component']['name']}: {err}"
        for s in svcs
        if s["component"]["state"] != "ENABLED"
        for err in _errors(s)
    ]
    if bad:
        raise NifiError("controller services not enabled:\n  " + "\n  ".join(bad))


def wait_tcp(host: str, port: int, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            if time.monotonic() >= deadline:
                raise NifiError(
                    f"nothing listens on {host}:{port} after {timeout_s:.0f} s"
                ) from None
            time.sleep(1)


# -- registry ------------------------------------------------------------------------------------


def record_group_ids(control: ControlSettings, groups: dict[str, str]) -> int | None:
    """UPDATE registry.job.nifi_process_group_id by group name as the control owner (step 8).

    Returns the rows updated, or None (with a warning) when the control DB is not reachable or
    `control init` has not run.
    """
    import psycopg

    try:
        conn = psycopg.connect(
            control.conninfo(control.owner_user, control.owner_password), connect_timeout=5
        )
    except psycopg.OperationalError:
        log.warning("control DB not reachable; registry.job.nifi_process_group_id not written")
        return None
    with conn:
        row = conn.execute("SELECT to_regclass('registry.job')").fetchone()
        if row is None or row[0] is None:
            log.warning("registry.job missing (run control init); group ids not written")
            return None
        n = 0
        for name, gid in sorted(groups.items()):
            cur = conn.execute(
                "UPDATE registry.job SET nifi_process_group_id = %s WHERE process_group_name = %s",
                (gid, name),
            )
            n += cur.rowcount
        return n


# -- the whole deploy ----------------------------------------------------------------------------


def deploy(
    c: Client,
    flow_path: Path,
    env: str,
    params_path: Path,
    *,
    start: bool = True,
    trigger: tuple[str, int] | None = None,
    control: ControlSettings | None = None,
    wait_s: float = 300.0,
) -> str:
    """Replace the `retail-pricing` group with `flow_path`; returns the new group id."""
    values, sensitive = load_params(params_path)  # fail on unset secrets before touching NiFi
    flow = flow_path.read_bytes()
    c.login(wait_s=wait_s)  # 1
    root = root_id(c)
    if old := child_group(c, root, GROUP):  # 2
        log.info("removing the old group", extra={"group_id": old["id"]})
        remove_group(c, old["id"])
    pc_id = upsert_parameter_context(c, f"retail-pricing-{env}", values, sensitive)  # 3
    gid = upload(c, root, flow, GROUP)  # 4
    tree = list(walk(c, gid))
    for _, f in tree:
        bind_context(c, f["id"], pc_id)
    enable_services(c, gid)  # 5
    if bad := settled_invalid(c, gid):  # 6
        raise NifiError(f"{len(bad)} invalid component(s):\n  " + "\n  ".join(bad))
    if start:  # 7
        c.put(f"/flow/process-groups/{gid}", json={"id": gid, "state": "RUNNING"})
        listens = any(
            p["component"]["type"].endswith(".HandleHttpRequest")
            for _, f in tree
            for p in f["processors"]
        )
        if listens and trigger:
            wait_tcp(*trigger)
    if control is not None:  # 8
        groups = {path.rsplit("/", 1)[1]: f["id"] for path, f in tree}
        n = record_group_ids(control, groups)
        if n is not None:
            log.info("registry updated", extra={"jobs": n})
    return gid
