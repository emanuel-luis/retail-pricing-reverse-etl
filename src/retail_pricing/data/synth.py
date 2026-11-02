"""Deterministic synthesis rules shared by the DuckDB build, the tests and the workload.

Every random choice goes through `u(key, salt)`, which the build also defines as a DuckDB
macro; the unit tests prove both give the same values.
"""

import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from itertools import groupby

ANCHOR = date(2025, 12, 4)  # Thursday; the workload's clock continues from the next day
SHIFT_DAYS = 8372  # 23 x 364: weekdays kept, 1998-01-02..2003-01-02 -> 2020-12-04..2025-12-04

PACK_UNITS: dict[str, Decimal] = {
    "petite": Decimal("0.5"),
    "small": Decimal(1),
    "medium": Decimal(2),
    "large": Decimal(3),
    "extra large": Decimal(4),
    "economy": Decimal(6),
    "N/A": Decimal(1),
}

Price = Decimal | Fraction | int


def u(key: object, salt: str) -> float:
    """Uniform [0, 1) from md5(salt:key). Keys are ints or strings (DuckDB casts to VARCHAR)."""
    digest = hashlib.md5(f"{salt}:{key}".encode(), usedforsecurity=False).digest()
    return (int.from_bytes(digest, "little") % 1_000_000) / 1e6


def shift_date(d: date) -> date:
    return d + timedelta(days=SHIFT_DAYS)


def zone_count(stores: int) -> int:
    """Store zones: min(8, max(1, round(stores / 4))), rounding half up."""
    return min(8, max(1, (stores + 2) // 4))


def store_zone(store_id: int, zones: int) -> int:
    return (store_id - 1) % zones + 1


def price_factor(zone_no: int) -> Decimal:
    return Decimal("0.96") + Decimal("0.02") * (zone_no % 5)


def pack_units(size: str | None) -> Decimal:
    return PACK_UNITS.get(size or "", Decimal(1))


def kvi_count(products: int) -> int:
    """Top 2% of products (ceil), at least 5."""
    return max(5, (2 * products + 99) // 100)


def kvi_tier1_count(products: int) -> int:
    """Top 0.5% of products (ceil), at least 1."""
    return max(1, (products + 199) // 200)


@dataclass(frozen=True)
class Band:
    """Prices n + ending (n integer) inside [min_price, max_price); max_price None = unbounded."""

    min_price: Decimal
    max_price: Decimal | None
    ending: Decimal


def _dec(p: Fraction) -> Decimal:
    return (Decimal(p.numerator) / Decimal(p.denominator)).quantize(Decimal("0.01"))


class Grid:
    """The price-ending grid from `rules.price_ending_band`, in closed form (no upper bound)."""

    def __init__(self, bands: Iterable[Band]) -> None:
        self.bands = tuple(bands)

    def _ceil(self, v: Fraction, strict: bool) -> Fraction | None:
        best: Fraction | None = None
        for b in self.bands:
            e, lo = Fraction(b.ending), Fraction(b.min_price)
            n = max(math.floor(v - e) + 1 if strict else math.ceil(v - e), math.ceil(lo - e))
            p = n + e
            if (b.max_price is None or p < Fraction(b.max_price)) and (best is None or p < best):
                best = p
        return best

    def floor(self, v: Price) -> Decimal | None:
        """Largest grid price <= v."""
        v, best = Fraction(v), None
        for b in self.bands:
            e = Fraction(b.ending)
            p = math.floor(v - e) + e
            if b.max_price is not None and p >= Fraction(b.max_price):
                p = math.ceil(Fraction(b.max_price) - e) - 1 + e
            if p >= Fraction(b.min_price) and (best is None or p > best):
                best = p
        return None if best is None else _dec(best)

    def ceil(self, v: Price) -> Decimal | None:
        """Smallest grid price >= v."""
        p = self._ceil(Fraction(v), strict=False)
        return None if p is None else _dec(p)

    def next_above(self, v: Price) -> Decimal | None:
        """Smallest grid price > v."""
        p = self._ceil(Fraction(v), strict=True)
        return None if p is None else _dec(p)

    def round(self, v: Price) -> Decimal | None:
        """Nearest grid price; a tie goes up."""
        f, c = self.floor(v), self.ceil(v)
        if f is None or c is None:
            return c if f is None else f
        return c if Fraction(c) - Fraction(v) <= Fraction(v) - Fraction(f) else f


@dataclass(frozen=True)
class Ladder:
    """A PACK or TIER ladder: members are (product_id, rank, pack_units)."""

    kind: str
    members: tuple[tuple[int, int, Decimal], ...]


def _by_rank(lad: Ladder, reverse: bool) -> list[list[tuple[int, int, Decimal]]]:
    ordered = sorted(lad.members, key=lambda m: (m[1], m[0]), reverse=reverse)
    return [list(g) for _, g in groupby(ordered, key=lambda m: m[1])]


def _fix_pack(prices: dict[int, Decimal], lad: Ladder, grid: Grid) -> bool:
    """Unit price non-increasing by rank: raise smaller packs to the largest bigger-pack unit."""
    changed, best = False, None
    for group in _by_rank(lad, reverse=True):
        if best is not None:
            for pid, _, pack in group:
                need = best * Fraction(pack)
                if prices[pid] < need:
                    prices[pid], changed = _must(grid.ceil(need)), True
        units = [Fraction(prices[pid]) / Fraction(pack) for pid, _, pack in group]
        best = max(units) if best is None else max(best, *units)
    return changed


def _fix_tier(prices: dict[int, Decimal], lad: Ladder, grid: Grid) -> bool:
    """Price strictly increasing by tier (at least one grid step): raise the higher tiers."""
    changed, best = False, None
    for group in _by_rank(lad, reverse=False):
        if best is not None:
            for pid, _, _ in group:
                if prices[pid] <= best:
                    prices[pid], changed = _must(grid.next_above(best)), True
        top = max(prices[pid] for pid, _, _ in group)
        best = top if best is None else max(best, top)
    return changed


def _must(p: Decimal | None) -> Decimal:
    if p is None:
        raise ValueError("no grid price above the requested value")
    return p


def repair_zone(
    prices: Mapping[int, Decimal], ladders: Sequence[Ladder], grid: Grid, max_rounds: int = 100
) -> dict[int, Decimal]:
    """Raise prices of one zone until every ladder holds (PACK then TIER, to a fixed point).

    Raises only move towards smaller packs and higher tiers, so the passes converge.
    """
    out = dict(prices)
    ordered = sorted(ladders, key=lambda lad: lad.kind)  # PACK before TIER
    for _ in range(max_rounds):
        changed = False
        for lad in ordered:
            fix = _fix_pack if lad.kind == "PACK" else _fix_tier
            changed |= fix(out, lad, grid)
        if not changed:
            return out
    raise RuntimeError(f"ladder repair did not converge in {max_rounds} rounds")


def ladder_holds(lad: Ladder, prices: Mapping[int, Decimal]) -> bool:
    for a in lad.members:
        for b in lad.members:
            if a[1] >= b[1]:
                continue
            pa, pb = prices[a[0]], prices[b[0]]
            if lad.kind == "PACK" and pa * b[2] < pb * a[2]:  # unit(a) < unit(b)
                return False
            if lad.kind == "TIER" and pa >= pb:
                return False
    return True
