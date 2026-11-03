import json
from pathlib import Path

import pytest

from retail_pricing.report import (
    TPC_NOTICE,
    build,
    grafana_url,
    parse_mem_mib,
    parse_steps,
    peak_memory,
    render_md,
    write_report,
)

STEPS = json.dumps(
    [{"step": "data all", "ms": 1500, "ok": True}, {"step": "oltp load", "ms": 62000}]
)


def test_parse_steps() -> None:
    assert parse_steps(STEPS) == [
        {"step": "data all", "seconds": 1.5, "ok": True},
        {"step": "oltp load", "seconds": 62.0, "ok": True},
    ]
    assert parse_steps("[]") == []
    for bad in ('{"a": 1}', '[{"step": "x"}]', "[1]", "not json"):
        with pytest.raises((ValueError, TypeError)):
            parse_steps(bad)


def test_memory_parsing() -> None:
    assert parse_mem_mib("512MiB / 15.6GiB") == 512.0
    assert parse_mem_mib("1.5GiB / 15.6GiB") == 1536.0
    assert parse_mem_mib("1048576B / 1GiB") == 1.0
    assert parse_mem_mib("n/a") is None
    text = "\n".join(  # noqa: FLY002
        [
            '{"Name": "oltp", "MemUsage": "100MiB / 8GiB"}',
            '{"Name": "oltp", "MemUsage": "300MiB / 8GiB"}',
            '{"Name": "nifi", "MemUsage": "2GiB / 8GiB"}',
            "garbage",
            '{"Name": "oltp", "MemUsage": "200MiB / 8GiB"}',
        ]
    )
    assert peak_memory(text) == {"nifi": 2048.0, "oltp": 300.0}


def sample(mem: dict[str, float] | None = None) -> dict:  # type: ignore[type-arg]
    nodes = [
        {
            "cycle_id": 1,
            "unique_id": f"model.p.m{i}",
            "status": "success",
            "execution_time_s": float(i),
            "rows_affected": i * 10,
        }
        for i in range(12)
    ] + [
        {
            "cycle_id": 1,
            "unique_id": "test.p.g01_x",
            "status": "pass",
            "execution_time_s": 99.0,
            "rows_affected": None,
        }
    ]
    return build(
        parse_steps(STEPS),
        [{"cycle_id": 1, "job_id": "extract", "status": "succeeded", "duration_s": 12.345}],
        [
            {
                "cycle_id": 1,
                "job_id": "extract",
                "item": "t1",
                "rows": 1000,
                "bytes": 5_000_000,
                "seconds": 2.0,
            },
            {
                "cycle_id": 1,
                "job_id": "load",
                "item": "t1",
                "rows": 1000,
                "bytes": None,
                "seconds": 0.0,
            },
        ],
        nodes,
        [
            {
                "cycle_id": 1,
                "target": "zone_price",
                "batches": 2,
                "sent": 500,
                "applied": 400,
                "unchanged": 50,
                "conflicts": 40,
                "rejects": 10,
                "duration_s": 5.0,
            }
        ],
        [{"cycle_id": 1, "target": "zone_price", "checks": 3, "passed": 3}],
        mem,
        "run-sf1",
    )


def test_build_shape_and_rates() -> None:
    rep = sample()
    assert list(rep) == [
        "run_dir", "notice", "host_steps", "host_total_seconds", "stages", "tables",
        "dbt_top_models", "reverse", "reconcile", "peak_memory_mib",
    ]  # fmt: skip
    assert rep["notice"] == TPC_NOTICE
    assert rep["host_total_seconds"] == 63.5
    assert rep["stages"][0]["seconds"] == 12.35
    e, load = rep["tables"]
    assert (e["mb"], e["rows_per_s"], e["mb_per_s"]) == (5.0, 500.0, 2.5)
    assert (load["mb"], load["rows_per_s"], load["mb_per_s"]) == (None, None, None)
    # Only models, longest first, capped at 10.
    assert [m["model"] for m in rep["dbt_top_models"]] == [f"m{i}" for i in range(11, 1, -1)]
    assert rep["reverse"][0]["rows_per_s"] == 100.0
    assert rep["peak_memory_mib"] is None


def test_markdown_has_the_notice_and_sections(tmp_path: Path) -> None:
    rep = sample({"nifi": 2048.0})
    md = render_md(rep)
    assert TPC_NOTICE in md
    for h in ("Host steps", "Stage durations", "Extract and load", "Top 10 dbt", "Reverse",
              "Reconcile", "Peak memory"):  # fmt: skip
        assert f"## {h}" in md
    assert "Grafana" not in md
    assert "Grafana: http://127.0.0.1:3000" in render_md(rep, grafana_url(3000))
    assert "## Peak memory" not in render_md(sample())
    write_report(tmp_path, rep, None)
    assert (
        json.loads((tmp_path / "timings.json").read_text(encoding="utf-8"))["run_dir"] == "run-sf1"
    )
    assert TPC_NOTICE in (tmp_path / "timings.md").read_text(encoding="utf-8")


def test_grafana_url_only_with_a_port() -> None:
    assert grafana_url(None) is None
