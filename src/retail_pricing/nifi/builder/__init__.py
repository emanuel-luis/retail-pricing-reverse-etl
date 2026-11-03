"""Flow builder: `retail-pricing nifi build-flows` writes nifi/flows/retail-pricing.json."""

import json
from pathlib import Path
from typing import Any

from retail_pricing.nifi.builder import extract, finalize, gate, load, trigger
from retail_pricing.nifi.builder.common import TOP, add_services
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
    # PR 5 seam: 30 transform, 40-43 reverse and 50 reconcile go here, each with its gate
    # (the reconcile gate also calls audit.reconcile), chained from `after_load` to finalize.
    top.connect(after_load, fin_in, "success")
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
