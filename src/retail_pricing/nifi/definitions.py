"""Pull the processor and controller-service definitions the flow builder checks against."""

import json
from pathlib import Path
from typing import Any

from retail_pricing.nifi.client import Client, NifiError

# Every type docs/PLAN.md 6.4 and 6.5 use, by simple name.
PROCESSORS = (
    "EvaluateJsonPath",
    "ExecuteSQLRecord",
    "GenerateFlowFile",
    "GenerateTableFetch",
    "HandleHttpRequest",
    "HandleHttpResponse",
    "InvokeHTTP",
    "LogMessage",
    "PutDatabaseRecord",
    "PutS3Object",
    "PutSQL",
    "ReplaceText",
    "RetryFlowFile",
    "RouteOnAttribute",
    "SplitJson",
    "UpdateAttribute",
)
CONTROLLER_SERVICES = (
    "AWSCredentialsProviderControllerService",
    "DBCPConnectionPool",
    "JsonRecordSetWriter",
    "JsonTreeReader",
    "ParquetRecordSetWriter",
    "StandardHttpContextMap",
)


def _by_simple_name(types: list[dict[str, Any]], wanted: tuple[str, ...]) -> dict[str, Any]:
    found = {str(t["type"]).rsplit(".", 1)[1]: t for t in types}
    missing = sorted(set(wanted) - set(found))
    if missing:
        raise NifiError(f"types not installed in this NiFi: {', '.join(missing)}")
    return {name: found[name] for name in wanted}


def pull(client: Client, out_dir: Path) -> list[Path]:
    """Write `<out_dir>/<SimpleType>.json` (sorted keys) for every type; return the paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs = [
        ("processor", client.get("/flow/processor-types")["processorTypes"], PROCESSORS),
        (
            "controller-service",
            client.get("/flow/controller-service-types")["controllerServiceTypes"],
            CONTROLLER_SERVICES,
        ),
    ]
    written = []
    for kind, types, wanted in jobs:
        for name, t in _by_simple_name(types, wanted).items():
            b = t["bundle"]
            body = client.get(
                f"/flow/{kind}-definition/{b['group']}/{b['artifact']}/{b['version']}/{t['type']}"
            )
            path = out_dir / f"{name}.json"
            path.write_text(
                json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
            )
            written.append(path)
    return written
