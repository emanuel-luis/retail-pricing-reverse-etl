"""The committed flow is current, and every component matches the pulled NiFi definitions."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from retail_pricing.nifi.builder import OUT, build, render
from retail_pricing.nifi.builder.dsl import load_definitions
from retail_pricing.nifi.builder.reverse import TARGETS

DEFS = load_definitions()
BY_TYPE = {d["type"]: d for d in DEFS.values()}
FLOW = build()


def groups(g: dict[str, Any] = FLOW["flowContents"]) -> Iterator[dict[str, Any]]:
    yield g
    for child in g["processGroups"]:
        yield from groups(child)


def components() -> Iterator[dict[str, Any]]:
    for g in groups():
        yield from g["processors"]
        yield from g["controllerServices"]


SERVICE_IDS = {s["identifier"] for g in groups() for s in g["controllerServices"]}


def test_committed_flow_is_current() -> None:
    assert Path(OUT).read_text(encoding="utf-8") == render(FLOW), (
        "run `uv run retail-pricing nifi build-flows` and commit nifi/flows/retail-pricing.json"
    )


def test_build_is_deterministic() -> None:
    assert render(build()) == render(FLOW)


@pytest.mark.parametrize("c", list(components()), ids=lambda c: c["name"])
def test_properties_match_the_definition(c: dict[str, Any]) -> None:
    d = BY_TYPE[c["type"]]
    assert c["bundle"] == {"artifact": d["artifact"], "group": d["group"], "version": d["version"]}
    known = d.get("propertyDescriptors") or {}
    for key, value in c["properties"].items():
        if key not in known:
            assert d.get("supportsDynamicProperties"), f"{key!r} is not a property of {d['type']}"
            continue
        allowed = [a["value"] for a in known[key].get("allowableValues") or []]
        if allowed and not value.startswith("#{"):
            assert value in allowed, f"{key}={value!r} not in {allowed}"
        if known[key].get("typeProvidedByValue"):
            assert value in SERVICE_IDS, f"{key} does not name a service of the flow"
    assert set(c["propertyDescriptors"]) == set(c["properties"])


def relationships(c: dict[str, Any]) -> set[str]:
    d = BY_TYPE[c["type"]]
    rels = {r["name"] for r in d.get("supportedRelationships") or []}
    if d.get("supportsDynamicRelationships"):  # RouteOnAttribute: one per dynamic property
        rels |= set(c["properties"]) - set(d.get("propertyDescriptors") or {})
    return rels


def test_relationships_exist_and_each_is_used_once() -> None:
    procs = {p["identifier"]: p for g in groups() for p in g["processors"]}
    used: dict[str, list[str]] = {
        pid: list(p["autoTerminatedRelationships"]) for pid, p in procs.items()
    }
    for g in groups():
        for conn in g["connections"]:
            src = conn["source"]
            if src["type"] == "PROCESSOR":
                assert conn["selectedRelationships"], src["name"]
                used[src["id"]] += conn["selectedRelationships"]
    for pid, p in procs.items():
        rels = relationships(p)
        assert set(used[pid]) <= rels, f"{p['name']}: unknown {set(used[pid]) - rels}"
        assert sorted(used[pid]) == sorted(set(used[pid])), f"{p['name']}: a relationship twice"
        assert set(used[pid]) == rels, f"{p['name']}: {rels - set(used[pid])} goes nowhere"


def test_connections_join_components_of_the_flow() -> None:
    ids = {
        c["identifier"]
        for g in groups()
        for c in g["processors"] + g["inputPorts"] + g["outputPorts"]
    }
    for g in groups():
        for conn in g["connections"]:
            assert conn["source"]["id"] in ids and conn["destination"]["id"] in ids


def test_every_group_has_the_context_and_stage_groups_are_barriers() -> None:
    by_name = {g["name"]: g for g in groups()}
    assert all(g["parameterContextName"] == "retail-pricing-local" for g in by_name.values())
    stages = {"10 extract", "20 load", "30 transform", "50 reconcile"}
    stages |= {f"{40 + i} reverse {t}" for i, t in enumerate(TARGETS)}
    assert stages <= set(by_name)
    for name in stages:
        assert by_name[name]["flowFileConcurrency"] == "SINGLE_FLOWFILE_PER_NODE"
        assert by_name[name]["flowFileOutboundPolicy"] == "BATCH_OUTPUT"


def test_group_names_match_registry_jobs() -> None:
    from retail_pricing.control.seed import JOBS

    jobs = {j.process_group_name for j in JOBS}
    assert len(jobs) == 9
    assert {g["name"] for g in FLOW["flowContents"]["processGroups"]} - jobs == {"90 finalize"}


def test_clickhouse_inserts_use_one_insert_pipeline() -> None:
    # Two pipelines broke the 1.2 GiB etl query limit on sale_line at SF10 (DECISIONS 143).
    urls = [
        c["properties"]["HTTP URL"]
        for c in components()
        if c.get("type", "").endswith("InvokeHTTP") and "max_insert_threads" in str(c["properties"])
    ]
    assert urls and all("max_insert_threads=1" in u for u in urls), urls
