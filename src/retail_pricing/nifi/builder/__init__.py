"""Flow builder: `retail-pricing nifi build-flows` writes nifi/flows/retail-pricing.json."""

import json
from pathlib import Path
from typing import Any

from retail_pricing.nifi.builder import (
    extract,
    finalize,
    gate,
    load,
    reconcile,
    reverse,
    transform,
    trigger,
)
from retail_pricing.nifi.builder.common import TOP, add_services, attrs
from retail_pricing.nifi.builder.dsl import DEFINITIONS, Group, load_definitions

CONTEXT = "retail-pricing-local"  # deploy points every group at retail-pricing-<env>
PARAMS = Path("nifi/params/local.json")
OUT = Path("nifi/flows/retail-pricing.json")


def build(definitions: Path = DEFINITIONS, params: Path = PARAMS) -> dict[str, Any]:
    top = Group(load_definitions(definitions), "retail-pricing", TOP, CONTEXT)
    add_services(top)
    _, fin_in = finalize.build(top)
    trig = trigger.build(top)
    ext = extract.build(top)
    top.connect(trig.port("cycle"), ext.port("in"))
    after_extract = gate.build(top, "extract", ext.port("out"), fin_in)
    ld = load.build(top)
    top.connect(after_extract, ld.port("in"), "ok")
    after_load = gate.build(
        top,
        "load",
        ld.port("out"),
        fin_in,
        (
            "commit_watermarks",
            "select table_name, high_value::text from registry.commit_watermarks(${cycle_id})",
        ),
    )
    tr = transform.build(top)
    top.connect(after_load, tr.port("in"), "success")
    nxt = gate.build(top, "transform", tr.port("out"), fin_in)
    for target in reverse.TARGETS:  # chained: prices land before the labels (DECISIONS 21)
        rev = reverse.build(top, target)
        top.connect(nxt, rev.port("in"), "ok")
        nxt = gate.build(top, f"reverse.{target}", rev.port("out"), fin_in)
    rec = reconcile.build(top)
    top.connect(nxt, rec.port("in"), "ok")
    after_rec = gate.build(
        top,
        "reconcile",
        rec.port("out"),
        fin_in,
        ("reconcile", "select passed, failed_checks from audit.reconcile(${cycle_id})"),
    )
    # A failed check fails the cycle at reconcile (audit.reconcile has raised the alert).
    result = attrs(
        top,
        "gate reconcile: result",
        {"reconcile_passed": "$[0].passed", "failed_checks": "$[0].failed_checks"},
    )
    passed = top.proc(
        "RouteOnAttribute",
        "gate reconcile: passed",
        {"passed": "${reconcile_passed:equals('true')}"},
    )
    differ = top.proc(
        "UpdateAttribute",
        "gate reconcile: checks differ",
        {
            "cycle.status": "failed",
            "failed_stage": "reconcile",
            "cycle.error": "${failed_checks:replaceNull('?')} reconcile check(s) differ:"
            " see audit.reconcile_result",
        },
    )
    top.connect(after_rec, result, "success")
    top.connect(result, passed, "matched")
    top.connect(result, differ, "unmatched", "failure")
    top.connect(passed, fin_in, "passed")
    top.connect(passed, differ, "unmatched")
    top.connect(differ, fin_in, "success")
    raw: dict[str, Any] = json.loads(params.read_text(encoding="utf-8"))
    parameters = [
        {"name": k, "provided": False, "sensitive": True}
        if isinstance(v, dict)
        else {"name": k, "provided": False, "sensitive": False, "value": str(v)}
        for k, v in sorted(raw.items())
    ]
    return {
        "externalControllerServices": {},
        "flowContents": top.to_json(),
        "flowEncodingVersion": "1.0",
        "latest": False,
        "parameterContexts": {
            CONTEXT: {
                "componentType": "PARAMETER_CONTEXT",
                "inheritedParameterContexts": [],
                "name": CONTEXT,
                "parameters": parameters,
            }
        },
        "parameterProviders": {},
    }


def render(flow: dict[str, Any]) -> str:
    return json.dumps(flow, indent=2, sort_keys=True) + "\n"


def write(out: Path = OUT) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(build()), encoding="utf-8", newline="\n")
    return out
