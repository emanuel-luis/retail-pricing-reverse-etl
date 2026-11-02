import random
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from retail_pricing.data.synth import Band, Grid
from retail_pricing.settings import WorkloadSettings
from retail_pricing.workload import logic

GRID = Grid(
    [
        Band(Decimal(0), Decimal(10), Decimal("0.49")),
        Band(Decimal(0), Decimal(10), Decimal("0.99")),
        Band(Decimal(10), Decimal(50), Decimal("0.99")),
        Band(Decimal(50), None, Decimal("0.90")),
    ]
)


@given(st.decimals(min_value="0.49", max_value="500", places=2), st.integers(0, 10_000))
def test_edited_price_is_on_grid_close_and_different(price: Decimal, seed: int) -> None:
    new = logic.edited_price(random.Random(seed), price, GRID)
    assert new != price and new > 0
    assert GRID.round(new) == new
    assert abs(new - price) <= price * Decimal("0.08") + 10  # one grid step at most beyond 8%


def test_rates_and_events_follow_the_settings() -> None:
    r = logic.rates(WorkloadSettings(sales_per_second=20, return_share=0.02))
    assert r["sale"] == 20 and r["return"] == 20 * 4 * 0.02
    rng = random.Random(1)
    kinds = [logic.next_event(rng, r, workers=8)[1] for _ in range(20_000)]
    assert 0.8 < kinds.count("sale") / len(kinds) / (20 / sum(r.values())) < 1.2
    zero = logic.next_event(rng, {"sale": 1.0, "return": 0.0}, workers=1)
    assert zero[1] == "sale"


def test_line_count_is_between_1_and_8_with_mean_about_4() -> None:
    rng = random.Random(3)
    n = [logic.line_count(rng) for _ in range(10_000)]
    assert min(n) == 1 and max(n) == 8
    assert 3.6 < sum(n) / len(n) < 4.4


def test_receipt_only_when_stock_is_low() -> None:
    rng = random.Random(0)
    assert logic.receipt_qty(rng, logic.COVER_MIN_UNITS) is None
    qty = logic.receipt_qty(rng, 0)
    assert qty is not None and qty >= 20


def test_pick_edit_keys_puts_kvi_first_and_is_deterministic() -> None:
    keys = [(p, z) for p in range(1, 21) for z in (1, 2)]
    kvi = {3, 7}
    picked = logic.pick_edit_keys(keys, kvi, 6, True, random.Random(7))
    assert {k[0] for k in picked[:4]} == kvi
    assert picked == logic.pick_edit_keys(list(reversed(keys)), kvi, 6, True, random.Random(7))
    assert len(logic.pick_edit_keys(keys, kvi, 6, False, random.Random(7))) == 6
