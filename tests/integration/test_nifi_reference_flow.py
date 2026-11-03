"""Deploy the spike's reference flow to the running NiFi (marker `integration`).

Needs `docker compose up -d --wait nifi` and a `.env` copied from `.env.example`.
"""

from pathlib import Path

import pytest

from retail_pricing.nifi.client import connect
from retail_pricing.nifi.deploy import deploy, invalid_components
from retail_pricing.settings import NifiSettings

pytestmark = pytest.mark.integration


def test_reference_flow_deploys_with_no_invalid_component() -> None:
    with connect(NifiSettings()) as c:
        gid = deploy(
            c,
            Path("nifi/spike/reference-flow.json"),
            "local",
            Path("nifi/params/local.json"),
            start=False,
            wait_s=120,
        )
        assert invalid_components(c, gid) == []
        services = c.get(
            f"/flow/process-groups/{gid}/controller-services",
            params={"includeAncestorGroups": "false", "includeDescendantGroups": "true"},
        )["controllerServices"]
        assert len(services) == 7
        assert {s["component"]["state"] for s in services} == {"ENABLED"}
