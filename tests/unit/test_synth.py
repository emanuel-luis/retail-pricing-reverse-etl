from datetime import date, timedelta
from decimal import Decimal

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from retail_pricing.data import synth
from retail_pricing.data.build import create_macros, grid_lookup

BANDS = [
    synth.Band(Decimal(0), Decimal(10), Decimal("0.49")),
    synth.Band(Decimal(0), Decimal(10), Decimal("0.99")),
    synth.Band(Decimal(10), Decimal(50), Decimal("0.99")),
    synth.Band(Decimal(50), None, Decimal("0.90")),
]
GRID = synth.Grid(BANDS)
# Brute-force grid up to 200.00 for property checks.
POINTS = sorted(
    {
        Decimal(n) + b.ending
        for b in BANDS
        for n in range(201)
        if Decimal(n) + b.ending >= b.min_price
        and (b.max_price is None or Decimal(n) + b.ending < b.max_price)
    }
)
cents = st.integers(min_value=0, max_value=19_000).map(lambda c: Decimal(c) / 100)


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    create_macros(c)
    return c


@settings(max_examples=200, deadline=None)
@given(
    key=st.one_of(st.integers(min_value=-(10**12), max_value=10**12), st.text(max_size=20)),
    salt=st.text(min_size=1, max_size=12),
)
def test_u_matches_duckdb(con: duckdb.DuckDBPyConnection, key: int | str, salt: str) -> None:
    row = con.execute("SELECT u(?, ?)", [key, salt]).fetchone()
    assert row is not None
    assert row[0] == synth.u(key, salt)
    assert 0 <= synth.u(key, salt) < 1


def test_date_shift_keeps_weekdays_and_maps_the_sales_window(
    con: duckdb.DuckDBPyConnection,
) -> None:
    assert synth.SHIFT_DAYS == 23 * 364
    assert synth.shift_date(date(1998, 1, 2)) == date(2020, 12, 4)
    assert synth.shift_date(date(2003, 1, 2)) == date(2025, 12, 4) == synth.ANCHOR
    assert synth.ANCHOR.weekday() == 3  # Thursday
    for d in (date(1998, 1, 2), date(2000, 2, 29), date(2003, 1, 2)):
        assert synth.shift_date(d).weekday() == d.weekday()
        row = con.execute("SELECT shift_date(?::DATE)", [d]).fetchone()
        assert row is not None and row[0] == synth.shift_date(d)
    assert synth.shift_date(date(1998, 1, 2)) - timedelta(days=1) < date(2020, 12, 4)


def test_zone_assignment() -> None:
    assert [synth.zone_count(n) for n in (1, 2, 5, 6, 10, 12, 51, 102)] == [1, 1, 1, 2, 3, 3, 8, 8]
    assert [synth.store_zone(s, 3) for s in range(1, 8)] == [1, 2, 3, 1, 2, 3, 1]
    assert synth.price_factor(1) == Decimal("0.98")
    assert synth.price_factor(5) == Decimal("0.96")
    assert synth.price_factor(9) == Decimal("1.04")


def test_pack_units_from_size() -> None:
    assert synth.pack_units("petite") == Decimal("0.5")
    assert synth.pack_units("extra large") == 4
    assert synth.pack_units("economy") == 6
    assert synth.pack_units("N/A") == 1
    assert synth.pack_units(None) == 1
    assert synth.pack_units("unknown") == 1


def test_kvi_counts() -> None:
    assert synth.kvi_count(90) == 5
    assert synth.kvi_count(9_000) == 180
    assert synth.kvi_count(9_001) == 181
    assert synth.kvi_tier1_count(90) == 1
    assert synth.kvi_tier1_count(9_000) == 45


def test_ending_grid_examples() -> None:
    assert GRID.round(Decimal("0.10")) == Decimal("0.49")
    assert GRID.round(Decimal("0.74")) == Decimal("0.99")  # tie goes up
    assert GRID.round(Decimal("0.73")) == Decimal("0.49")
    assert GRID.round(Decimal("10.20")) == Decimal("9.99")
    assert GRID.round(Decimal("10.50")) == Decimal("10.99")
    assert GRID.round(Decimal("50.20")) == Decimal("49.99")
    assert GRID.round(Decimal("50.50")) == Decimal("50.90")
    assert GRID.floor(Decimal("0.48")) is None
    assert GRID.floor(Decimal("12.00")) == Decimal("11.99")
    assert GRID.ceil(Decimal("49.995")) == Decimal("50.90")
    assert GRID.next_above(Decimal("9.99")) == Decimal("10.99")
    assert GRID.next_above(Decimal("0.49")) == Decimal("0.99")


@given(v=cents)
def test_ending_grid_matches_brute_force(v: Decimal) -> None:
    below = [p for p in POINTS if p <= v]
    above = [p for p in POINTS if p >= v]
    assert GRID.floor(v) == (below[-1] if below else None)
    assert GRID.ceil(v) == above[0]
    assert GRID.next_above(v) == next(p for p in POINTS if p > v)
    r = GRID.round(v)
    assert r in POINTS
    assert all(abs(r - v) <= abs(p - v) for p in POINTS)


def test_grid_lookup_matches_grid() -> None:
    floors, rounds = grid_lookup(GRID, 12_000)
    for c in range(12_001):
        v = Decimal(c) / 100
        f, r = GRID.floor(v), GRID.round(v)
        assert floors[c] == (None if f is None else str(f))
        assert rounds[c] == (None if r is None else str(r))


def test_ladder_repair_fixes_pack_and_tier() -> None:
    pack = synth.Ladder("PACK", ((1, 1, Decimal(1)), (2, 2, Decimal(2)), (3, 3, Decimal(6))))
    tier = synth.Ladder("TIER", ((2, 1, Decimal(2)), (4, 2, Decimal(2)), (5, 3, Decimal(2))))
    prices = {1: Decimal("1.49"), 2: Decimal("3.99"), 3: Decimal("9.99"), 4: Decimal("3.49"),
              5: Decimal("3.99")}  # fmt: skip
    assert not synth.ladder_holds(pack, prices) and not synth.ladder_holds(tier, prices)
    fixed = synth.repair_zone(prices, [tier, pack], GRID)
    assert synth.ladder_holds(pack, fixed) and synth.ladder_holds(tier, fixed)
    assert fixed == {1: Decimal("2.49"), 2: Decimal("3.99"), 3: Decimal("9.99"),
                     4: Decimal("4.49"), 5: Decimal("4.99")}  # fmt: skip


@settings(max_examples=200, deadline=None)
@given(
    data=st.lists(
        st.tuples(st.sampled_from([0, 1, 2]), st.integers(1, 3), st.sampled_from(POINTS[:120])),
        min_size=2,
        max_size=10,
    )
)
def test_ladder_repair_converges_and_only_raises(data: list[tuple[int, int, Decimal]]) -> None:
    sizes = [Decimal("0.5"), Decimal(2), Decimal(6)]
    prices = {pid: price for pid, (_, _, price) in enumerate(data)}
    pack = synth.Ladder(
        "PACK", tuple((pid, s + 1, sizes[s]) for pid, (s, _, _) in enumerate(data) if pid % 2 == 0)
    )
    tier = synth.Ladder(
        "TIER", tuple((pid, t, Decimal(1)) for pid, (_, t, _) in enumerate(data) if pid % 2 == 1)
    )
    fixed = synth.repair_zone(prices, [pack, tier], GRID)
    assert synth.ladder_holds(pack, fixed) and synth.ladder_holds(tier, fixed)
    assert all(fixed[p] >= prices[p] for p in fixed)
    assert all(fixed[p] in POINTS or fixed[p] > POINTS[-1] for p in fixed)
