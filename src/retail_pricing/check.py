"""`retail-pricing check --cycles 1-3`: the end-of-demo assertions (PLAN 11 step 10).

Each assertion is a small function over rows (lists of dicts) that returns a list of failure
messages, empty when it holds. `load` reads the rows from the databases; nothing else does I/O.
Exit code: 0 when every assertion holds, 1 with the list of failures otherwise.
"""

import re
from collections import defaultdict
from collections.abc import Iterable
from decimal import Decimal
from typing import Annotated, Any

import typer

Rows = list[dict[str, Any]]
Counts = dict[tuple[int, str], int]
MAX_SHOWN = 10
GUARDRAIL_ID = re.compile(r"^test\.[^.]+\.g\d+_")


def parse_cycles(spec: str) -> list[int]:
    """'1-3' -> [1, 2, 3]; '2' -> [2]; '1,3' -> [1, 3]."""
    out: list[int] = []
    for part in spec.split(","):
        lo, _, hi = part.strip().partition("-")
        try:
            a, b = int(lo), int(hi or lo)
        except ValueError:
            raise ValueError(f"bad cycle spec {spec!r}: use 1-3, 2 or 1,3") from None
        if a < 1 or b < a:
            raise ValueError(f"bad cycle spec {spec!r}: use 1-3, 2 or 1,3")
        out.extend(range(a, b + 1))
    return sorted(set(out))


def cycles_succeeded(cycles: Rows, wanted: list[int]) -> list[str]:
    by_id = {c["cycle_id"]: c for c in cycles}
    fails = []
    for n in wanted:
        c = by_id.get(n)
        if c is None:
            fails.append(f"cycle {n} does not exist")
        elif c["status"] != "succeeded":
            fails.append(
                f"cycle {n} is {c['status']}"
                + (f" (stage {c.get('failed_stage')}: {c.get('error')})" if c.get("error") else "")
            )
    return fails


def reconciles_passed(results: Rows, wanted: list[int]) -> list[str]:
    fails = []
    for n in wanted:
        mine = [r for r in results if r["cycle_id"] == n]
        if not mine:
            fails.append(f"cycle {n} has no reconcile result")
        fails += [
            f"cycle {n} reconcile failed: {r['target']} {r['check_name']}"
            f" zone {r.get('zone_id', 0)} category {r.get('category_id', 0)}"
            for r in mine
            if not r["passed"]
        ]
    return fails


def batches_balance(batches: Rows, wanted: list[int]) -> list[str]:
    """sent = applied + unchanged + conflicts + rejects, for every batch."""
    fails = []
    for n in wanted:
        mine = [b for b in batches if b["cycle_id"] == n]
        if not mine:
            fails.append(f"cycle {n} has no reverse batch")
        for b in mine:
            parts = b["applied"] + b["unchanged"] + b["conflicts"] + b["rejects"]
            if b["sent"] != parts:
                fails.append(
                    f"cycle {n} {b['target']} batch {b['batch_no']}: sent {b['sent']} but"
                    f" applied+unchanged+conflicts+rejects = {parts}"
                )
    return fails


def no_guardrail_errors(nodes: Rows) -> list[str]:
    return [
        f"cycle {r['cycle_id']} {r['unique_id']}: {r['status']}"
        for r in nodes
        if GUARDRAIL_ID.match(r["unique_id"]) and r["status"] in ("error", "fail")
    ]


def conflicts_agree(oltp: Counts, audit: Counts, mart: Counts, mart_cycles: list[int]) -> list[str]:
    """OLTP pricing_apply.conflict = audit.reverse_batch sum = mart_conflicts.

    The mart is complete for a cycle only once the next cycle extracted the apply tables, so
    it is compared for `mart_cycles` only. Absent keys count as 0.
    """
    fails = []
    for key in sorted(set(oltp) | set(audit)):
        o, a = oltp.get(key, 0), audit.get(key, 0)
        if o != a:
            fails.append(f"cycle {key[0]} {key[1]}: OLTP conflicts {o} but audit {a}")
    for key in sorted(k for k in set(oltp) | set(mart) if k[0] in mart_cycles):
        o, m = oltp.get(key, 0), mart.get(key, 0)
        if o != m:
            fails.append(f"cycle {key[0]} {key[1]}: OLTP conflicts {o} but mart_conflicts {m}")
    return fails


def applied_match_audit(mart: Counts, audit: Counts, cycles: list[int]) -> list[str]:
    """Applied rows per cycle and target in mart_proposed_vs_applied = audit.reverse_batch."""
    fails = []
    for key in sorted(k for k in set(mart) | set(audit) if k[0] in cycles):
        m, a = mart.get(key, 0), audit.get(key, 0)
        if m != a:
            fails.append(f"cycle {key[0]} {key[1]}: mart applied {m} but audit {a}")
    return fails


def applied_hold_proposed(mart_applied: Rows, oltp_held: Rows, cycles: list[int]) -> list[str]:
    """Zone prices the engine applied and nobody changed since hold the proposed price.

    oltp_held: pricing.zone_price rows whose last_cycle_id is one of the cycles; the engine is
    the only writer of last_cycle_id, and any later write moves or clears it. Such a row must
    have an applied mart row of the same cycle with the same proposed price. Only `cycles`
    (those whose marts are complete, see mart_cycles) are checked: the last cycle's apply
    outcome reaches ClickHouse with the next cycle's extract.
    """
    proposed = {
        (m["cycle_id"], m["product_id"], m["zone_id"]): Decimal(str(m["proposed_price"]))
        for m in mart_applied
    }
    fails = []
    for h in oltp_held:
        if h["last_cycle_id"] not in cycles:
            continue
        key = (h["last_cycle_id"], h["product_id"], h["zone_id"])
        label = f"cycle {key[0]} zone_price {key[1]}:{key[2]}"
        if key not in proposed:
            fails.append(f"{label}: held in the OLTP but not applied in the mart")
        elif proposed[key] != Decimal(str(h["price"])):
            fails.append(f"{label}: OLTP price {h['price']} but proposed {proposed[key]}")
    return fails


def has_conflicts(audit: Counts, cycle: int) -> list[str]:
    if sum(n for (c, _), n in audit.items() if c == cycle) > 0:
        return []
    return [f"cycle {cycle} has no conflicts (the edits did not land after the snapshot)"]


def run_checks(
    data: dict[str, Any], wanted: list[int], expect_conflicts_in: int | None
) -> dict[str, list[str]]:
    """Every assertion over the rows `load` returned, by name, in report order."""
    out = {
        "every cycle succeeded": cycles_succeeded(data["cycles"], wanted),
        "every reconcile passed": reconciles_passed(data["reconcile"], wanted),
        "sent = applied + unchanged + conflicts + rejects": batches_balance(
            data["batches"], wanted
        ),
        "no guardrail error": no_guardrail_errors(data["dbt_nodes"]),
        "conflicts agree (OLTP, audit, mart_conflicts)": conflicts_agree(
            data["oltp_conflicts"],
            data["audit_conflicts"],
            data["mart_conflicts"],
            mart_cycles(wanted),
        ),
        "mart applied counts match the audit": applied_match_audit(
            data["mart_applied"], data["audit_applied"], mart_cycles(wanted)
        ),
        "applied zone prices hold the proposed price": applied_hold_proposed(
            data["mart_zone_applied"], data["oltp_held"], mart_cycles(wanted)
        ),
    }
    if expect_conflicts_in is not None:
        out[f"cycle {expect_conflicts_in} has conflicts"] = has_conflicts(
            data["audit_conflicts"], expect_conflicts_in
        )
    return out


def mart_cycles(wanted: list[int]) -> list[int]:
    """Cycles whose marts are complete: every wanted cycle but the last one."""
    return wanted[:-1]


def render(results: dict[str, list[str]]) -> str:
    lines = []
    for name, fails in results.items():
        lines.append(f"{'FAIL' if fails else 'PASS'}  {name}")
        lines += [f"        {f}" for f in fails[:MAX_SHOWN]]
        if len(fails) > MAX_SHOWN:
            lines.append(f"        ... and {len(fails) - MAX_SHOWN} more")
    return "\n".join(lines)


def _counts(rows: Iterable[Iterable[Any]]) -> Counts:
    out: Counts = defaultdict(int)
    for cycle_id, target, n in rows:
        out[(int(cycle_id), str(target))] += int(n)
    return dict(out)


def load(wanted: list[int]) -> dict[str, Any]:
    """Read every row the assertions need from the control DB, the OLTP and ClickHouse."""
    import psycopg
    from psycopg.rows import dict_row

    from retail_pricing.clickhouse.client import connect
    from retail_pricing.settings import ClickHouseSettings, ControlSettings, OltpSettings

    ids = tuple(wanted)
    cs, os_ = ControlSettings(), OltpSettings()
    with psycopg.connect(cs.conninfo(cs.owner_user, cs.owner_password), row_factory=dict_row) as c:

        def q(sql: str) -> Rows:
            return c.execute(sql, (list(ids),)).fetchall()

        data: dict[str, Any] = {
            "cycles": q(
                "SELECT cycle_id, status, failed_stage, error FROM audit.cycle"
                " WHERE cycle_id = ANY(%s)"
            ),
            "reconcile": q(
                "SELECT cycle_id, target, zone_id, category_id, check_name, passed"
                " FROM audit.reconcile_result WHERE cycle_id = ANY(%s)"
            ),
            "batches": q(
                "SELECT cycle_id, target, batch_no, sent, applied, unchanged, conflicts, rejects"
                " FROM audit.reverse_batch WHERE cycle_id = ANY(%s)"
            ),
            "dbt_nodes": q(
                "SELECT cycle_id, unique_id, status FROM audit.dbt_node_result"
                " WHERE cycle_id = ANY(%s)"
            ),
        }
        totals = q(
            "SELECT cycle_id, target, applied, conflicts FROM audit.reverse_batch"
            " WHERE cycle_id = ANY(%s)"
        )
    data["audit_conflicts"] = _counts((t["cycle_id"], t["target"], t["conflicts"]) for t in totals)
    data["audit_applied"] = _counts((t["cycle_id"], t["target"], t["applied"]) for t in totals)

    with psycopg.connect(
        os_.conninfo(os_.owner_user, os_.owner_password), row_factory=dict_row
    ) as o:
        data["oltp_conflicts"] = _counts(
            (r["cycle_id"], r["target"], r["n"])
            for r in o.execute(
                "SELECT cycle_id, target, count(*) AS n FROM pricing_apply.conflict"
                " WHERE cycle_id = ANY(%s) GROUP BY 1, 2",
                (list(ids),),
            ).fetchall()
        )
        data["oltp_held"] = o.execute(
            "SELECT last_cycle_id, product_id, zone_id, price FROM pricing.zone_price"
            " WHERE last_cycle_id = ANY(%s)",
            (list(ids),),
        ).fetchall()

    in_list = ", ".join(str(int(i)) for i in ids)
    with connect(ClickHouseSettings()) as ch:
        data["mart_conflicts"] = _counts(
            ch.rows(
                "SELECT cycle_id, target, count() FROM marts.mart_conflicts"
                f" WHERE cycle_id IN ({in_list}) GROUP BY cycle_id, target"
            )
        )
        data["mart_applied"] = _counts(
            ch.rows(
                "SELECT cycle_id, target, count() FROM marts.mart_proposed_vs_applied"
                f" WHERE outcome = 'applied' AND cycle_id IN ({in_list}) GROUP BY cycle_id, target"
            )
        )
        data["mart_zone_applied"] = [
            {
                "cycle_id": int(str(r[0])),
                "product_id": int(str(r[1])),
                "zone_id": int(str(r[2])),
                "proposed_price": r[3],
            }
            for r in ch.rows(
                "SELECT cycle_id, product_id, zone_id, toString(proposed_price)"
                " FROM marts.mart_proposed_vs_applied WHERE outcome = 'applied'"
                f" AND target = 'zone_price' AND cycle_id IN ({in_list})"
            )
        ]
    return data


def check_cmd(
    cycles: Annotated[str, typer.Option(help="Cycles to check, e.g. 1-3.")] = "1-3",
    expect_conflicts_in: Annotated[
        int | None, typer.Option(help="Also require conflicts > 0 in this cycle.")
    ] = None,
) -> None:
    """Assert the demo's invariants over the audit DB, the OLTP and the ClickHouse marts."""
    try:
        wanted = parse_cycles(cycles)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None
    results = run_checks(load(wanted), wanted, expect_conflicts_in)
    typer.echo(render(results))
    failed = sum(1 for f in results.values() if f)
    if failed:
        typer.echo(f"{failed} of {len(results)} checks failed", err=True)
        raise typer.Exit(1)
    typer.echo(f"all {len(results)} checks passed")
