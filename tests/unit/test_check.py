from typing import Any

import pytest

from retail_pricing.check import (
    applied_hold_proposed,
    applied_match_audit,
    batches_balance,
    conflicts_agree,
    cycles_succeeded,
    has_conflicts,
    no_guardrail_errors,
    parse_cycles,
    reconciles_passed,
    render,
    run_checks,
)


def test_parse_cycles() -> None:
    assert parse_cycles("1-3") == [1, 2, 3]
    assert parse_cycles("2") == [2]
    assert parse_cycles("3,1") == [1, 3]
    for bad in ("x", "3-1", "0"):
        with pytest.raises(ValueError):
            parse_cycles(bad)


def test_cycles_succeeded() -> None:
    ok = {"cycle_id": 1, "status": "succeeded"}
    bad = {"cycle_id": 2, "status": "failed", "failed_stage": "load", "error": "boom"}
    assert cycles_succeeded([ok], [1]) == []
    assert cycles_succeeded([ok, bad], [1, 2, 3]) == [
        "cycle 2 is failed (stage load: boom)",
        "cycle 3 does not exist",
    ]


def test_reconciles_passed() -> None:
    r = {"cycle_id": 1, "target": "zone_price", "check_name": "transit", "passed": True}
    assert reconciles_passed([r], [1]) == []
    assert reconciles_passed([{**r, "passed": False}], [1]) == [
        "cycle 1 reconcile failed: zone_price transit zone 0 category 0"
    ]
    assert reconciles_passed([r], [1, 2]) == ["cycle 2 has no reconcile result"]


def batch(**kw: Any) -> dict[str, Any]:
    base = {"cycle_id": 1, "target": "zone_price", "batch_no": 1, "sent": 10, "applied": 6}
    return {**base, "unchanged": 2, "conflicts": 1, "rejects": 1, **kw}


def test_batches_balance() -> None:
    assert batches_balance([batch()], [1]) == []
    fails = batches_balance([batch(sent=11)], [1])
    assert len(fails) == 1 and "sent 11 but" in fails[0]
    assert batches_balance([], [1]) == ["cycle 1 has no reverse batch"]


def test_no_guardrail_errors() -> None:
    nodes = [
        {"cycle_id": 1, "unique_id": "test.retail_pricing.g01_change_within_cap", "status": "pass"},
        {"cycle_id": 1, "unique_id": "test.retail_pricing.g02_margin_floor", "status": "fail"},
        {"cycle_id": 2, "unique_id": "test.retail_pricing.g05_valid_endings", "status": "error"},
        {"cycle_id": 2, "unique_id": "test.retail_pricing.not_null_x.abc", "status": "fail"},
        {"cycle_id": 2, "unique_id": "model.retail_pricing.g09_like", "status": "error"},
    ]
    assert no_guardrail_errors(nodes) == [
        "cycle 1 test.retail_pricing.g02_margin_floor: fail",
        "cycle 2 test.retail_pricing.g05_valid_endings: error",
    ]


def test_conflicts_agree() -> None:
    k = (2, "zone_price")
    assert conflicts_agree({k: 50}, {k: 50}, {k: 50}, [2]) == []
    # The mart is not compared for a cycle that is not complete yet.
    assert conflicts_agree({k: 50}, {k: 50}, {}, []) == []
    assert conflicts_agree({k: 50}, {k: 49}, {k: 50}, [2]) == [
        "cycle 2 zone_price: OLTP conflicts 50 but audit 49"
    ]
    assert conflicts_agree({k: 50}, {k: 50}, {k: 40}, [2]) == [
        "cycle 2 zone_price: OLTP conflicts 50 but mart_conflicts 40"
    ]
    assert conflicts_agree({}, {}, {k: 1}, [2]) == [
        "cycle 2 zone_price: OLTP conflicts 0 but mart_conflicts 1"
    ]


def test_applied_match_audit() -> None:
    k = (1, "zone_price")
    assert applied_match_audit({k: 5}, {k: 5}, [1]) == []
    assert applied_match_audit({k: 4}, {k: 5}, [1]) == [
        "cycle 1 zone_price: mart applied 4 but audit 5"
    ]
    assert applied_match_audit({k: 4}, {k: 5}, [2]) == []


def test_applied_hold_proposed() -> None:
    mart = [{"cycle_id": 1, "product_id": 7, "zone_id": 2, "proposed_price": "9.99"}]
    held = {"last_cycle_id": 1, "product_id": 7, "zone_id": 2, "price": "9.99"}
    assert applied_hold_proposed(mart, [held]) == []
    assert applied_hold_proposed(mart, [{**held, "price": "10.49"}]) == [
        "cycle 1 zone_price 7:2: OLTP price 10.49 but proposed 9.99"
    ]
    assert applied_hold_proposed([], [held]) == [
        "cycle 1 zone_price 7:2: held in the OLTP but not applied in the mart"
    ]
    # Overwritten later: not held by the cycle any more, so nothing to compare.
    assert applied_hold_proposed(mart, []) == []


def test_has_conflicts() -> None:
    assert has_conflicts({(2, "zone_price"): 3}, 2) == []
    assert len(has_conflicts({(2, "zone_price"): 0}, 2)) == 1
    assert len(has_conflicts({(1, "zone_price"): 3}, 2)) == 1


def good_data() -> dict[str, Any]:
    k = (1, "zone_price")
    return {
        "cycles": [{"cycle_id": 1, "status": "succeeded"}],
        "reconcile": [{"cycle_id": 1, "target": "zone_price", "check_name": "t", "passed": True}],
        "batches": [batch()],
        "dbt_nodes": [],
        "oltp_conflicts": {k: 1},
        "audit_conflicts": {k: 1},
        "mart_conflicts": {},
        "mart_applied": {},
        "audit_applied": {},
        "mart_zone_applied": [],
        "oltp_held": [],
    }


def test_run_checks_all_pass_and_expect_conflicts() -> None:
    res = run_checks(good_data(), [1], 1)
    assert all(not f for f in res.values())
    assert "cycle 1 has conflicts" in res
    assert "FAIL" not in render(res)


def test_run_checks_reports_failures() -> None:
    data = good_data()
    data["cycles"] = [{"cycle_id": 1, "status": "failed"}]
    res = run_checks(data, [1], None)
    assert res["every cycle succeeded"] == ["cycle 1 is failed"]
    assert render(res).startswith("FAIL  every cycle succeeded\n        cycle 1 is failed")


def test_render_caps_the_list() -> None:
    out = render({"x": [f"f{i}" for i in range(25)]})
    assert "and 15 more" in out
