"""Pure decisions of the workload generator; `run.py` does the I/O (PLAN 9)."""

import random
from collections.abc import Collection, Sequence
from decimal import Decimal

from retail_pricing.data.synth import Grid
from retail_pricing.settings import WorkloadSettings

MEAN_LINES = 4
COVER_MIN_UNITS = 7  # ponytail: fixed receipt trigger, not cover_min days x sales rate


def rates(s: WorkloadSettings) -> dict[str, float]:
    """Transactions per second by type; returns are a share of the lines sold."""
    return {
        "sale": s.sales_per_second,
        "return": s.sales_per_second * MEAN_LINES * s.return_share,
        "price_edit": s.price_edits_per_second,
        "override_edit": s.override_edits_per_second,
        "supplier_cost": s.cost_rows_per_minute / 60,
        "label_print": s.label_batches_per_second,
    }


def next_event(rng: random.Random, per_second: dict[str, float], workers: int) -> tuple[float, str]:
    """(seconds to wait, transaction type) for one of `workers` threads sharing the rates."""
    live = {k: v for k, v in per_second.items() if v > 0}
    delay = rng.expovariate(sum(live.values()) / workers)
    kind = rng.choices(list(live), weights=list(live.values()))[0]
    return delay, kind


def line_count(rng: random.Random) -> int:
    """1..8 lines, mean about 4."""
    return min(8, max(1, round(rng.triangular(1, 8, 3))))


def edited_price(rng: random.Random, price: Decimal, grid: Grid) -> Decimal:
    """A manual edit: +-1..8%, snapped to the ending grid, never equal to the old price."""
    pct = Decimal(str(round(rng.uniform(0.01, 0.08), 4))) * rng.choice((-1, 1))
    new = grid.round(price * (1 + pct))
    if new is None or new <= 0 or new == price:
        new = grid.next_above(price)
    if new is None:
        raise ValueError(f"no grid price above {price}")
    return new


def receipt_qty(rng: random.Random, on_hand: int) -> int | None:
    """Units to receive when stock runs low, else None."""
    return rng.randint(20, 60) if on_hand < COVER_MIN_UNITS else None


def pick_edit_keys[K: tuple[int, int]](
    keys: Sequence[K], kvi: Collection[int], count: int, kvi_first: bool, rng: random.Random
) -> list[K]:
    """`count` (product_id, zone_id) keys for scripted edits; with kvi_first KVI products lead."""
    ordered = sorted(keys)
    if not kvi_first:
        rng.shuffle(ordered)
        return ordered[:count]
    head = [k for k in ordered if k[0] in kvi]
    tail = [k for k in ordered if k[0] not in kvi]
    rng.shuffle(head)
    rng.shuffle(tail)
    return (head + tail)[:count]
