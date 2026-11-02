"""pricing_apply.apply_batch outcomes, replay, label dependency, locking and reconciliation."""

import random
import threading
import uuid
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from retail_pricing.settings import OltpSettings

pytestmark = pytest.mark.sql

STORE = 1


@pytest.fixture
def cycle() -> int:
    return uuid.uuid4().int % 10**12


def _apply(conn: psycopg.Connection, cycle: int, target: str, batch: int = 1) -> tuple[Any, ...]:
    row = conn.execute(
        "SELECT * FROM pricing_apply.apply_batch(%s, %s, %s)", (cycle, target, batch)
    ).fetchone()
    assert row is not None
    return tuple(row)


def _store_zone(conn: psycopg.Connection) -> int:
    row = conn.execute("SELECT zone_id FROM master.store WHERE store_id = %s", (STORE,)).fetchone()
    assert row is not None
    return int(row[0])


def _zone_rows(conn: psycopg.Connection, n: int, zone: int) -> list[tuple[int, Decimal, int]]:
    """n (product_id, price, version) of a zone, in a random slice so tests do not collide."""
    rows = conn.execute(
        "SELECT product_id, price, version FROM pricing.zone_price WHERE zone_id = %s"
        " ORDER BY product_id",
        (zone,),
    ).fetchall()
    start = random.randrange(len(rows) - n)
    return rows[start : start + n]


def _stage_zone(conn: psycopg.Connection, cycle: int, rows: list[tuple[Any, ...]]) -> None:
    """rows: (product_id, zone_id, new_price, expected_version)."""
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO pricing_apply.zone_price_stage"
            " (cycle_id, product_id, zone_id, batch_no, new_price, expected_version, reason_code)"
            " VALUES (%s, %s, %s, 1, %s, %s, 'test')",
            [(cycle, *r) for r in rows],
        )


def _statuses(
    conn: psycopg.Connection, table: str, cycle: int
) -> dict[tuple[Any, ...], tuple[str, str | None]]:
    keys = {
        "zone_price_stage": "product_id, zone_id",
        "shelf_label_stage": "store_id, product_id",
        "store_override_stage": "store_id, product_id",
        "promo_price_stage": "promo_id, product_id, zone_id",
    }[table]
    out = {}
    for row in conn.execute(
        f"SELECT {keys}, status, status_detail FROM pricing_apply.{table} WHERE cycle_id = %s".encode(),
        (cycle,),
    ).fetchall():
        out[tuple(row[:-2])] = (row[-2], row[-1])
    return out


def test_zone_price_outcomes_replay_and_labels(oltp_owner: psycopg.Connection, cycle: int) -> None:
    conn, zone = oltp_owner, _store_zone(oltp_owner)
    (p1, pr1, v1), (p2, pr2, v2), (p3, pr3, v3), (p4, _, v4) = _zone_rows(conn, 4, zone)
    _stage_zone(
        conn,
        cycle,
        [
            (p1, zone, pr1 + Decimal("0.50"), v1),  # applied
            (p2, zone, pr2, v2),  # unchanged
            (p3, zone, pr3 + Decimal("0.50"), v3 + 100),  # conflict
            (p4, zone, Decimal(0), v4),  # rejected: price
            (999_999, zone, Decimal(1), 1),  # rejected: unknown key
        ],
    )
    assert _apply(conn, cycle, "zone_price") == ("zone_price", 1, 5, 1, 1, 1, 2, False)
    assert _apply(conn, cycle, "zone_price") == ("zone_price", 1, 5, 1, 1, 1, 2, True)

    st = _statuses(conn, "zone_price_stage", cycle)
    assert st[(p1, zone)] == ("applied", None)
    assert st[(p2, zone)] == ("unchanged", None)
    assert st[(p3, zone)] == ("conflict", "version_changed")
    assert st[(p4, zone)] == ("rejected", "price_not_positive")
    assert st[(999_999, zone)] == ("rejected", "unknown_key")

    row = conn.execute(
        "SELECT price, version, last_cycle_id, updated_by FROM pricing.zone_price"
        " WHERE product_id = %s AND zone_id = %s",
        (p1, zone),
    ).fetchone()
    assert row == (pr1 + Decimal("0.50"), v1 + 1, cycle, "pricing-engine")
    assert conn.execute(
        "SELECT target, product_id, zone_id, old_price, new_price, reason_code"
        " FROM pricing.price_change WHERE cycle_id = %s",
        (cycle,),
    ).fetchall() == [("zone_price", p1, zone, pr1, pr1 + Decimal("0.50"), "test")]
    assert conn.execute(
        "SELECT key_text, expected_version, actual_version, actual_price"
        " FROM pricing_apply.conflict WHERE cycle_id = %s",
        (cycle,),
    ).fetchall() == [(f"{p3}:{zone}", v3 + 100, v3, pr3)]

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO pricing_apply.shelf_label_stage (cycle_id, store_id, product_id, batch_no,"
            " price, due_date, source_target, zone_id)"
            " VALUES (%s, %s, %s, 1, %s, DATE '2025-12-05', 'zone_price', %s)",
            [(cycle, STORE, p, Decimal("9.99"), zone) for p in (p1, p2, p3)],
        )
    assert _apply(conn, cycle, "shelf_label") == ("shelf_label", 1, 3, 1, 0, 0, 2, False)
    st = _statuses(conn, "shelf_label_stage", cycle)
    assert st[(STORE, p1)] == ("applied", None)
    assert st[(STORE, p2)] == ("rejected", "price_not_applied")  # equal price, not this cycle's
    assert st[(STORE, p3)] == ("rejected", "price_not_applied")
    assert conn.execute(
        "SELECT product_id, status FROM pricing.shelf_label_queue WHERE cycle_id = %s", (cycle,)
    ).fetchall() == [(p1, "queued")]


def test_store_override_create_update_and_lost_race(
    oltp_owner: psycopg.Connection, cycle: int
) -> None:
    conn = oltp_owner
    free = conn.execute(
        "SELECT s.product_id FROM inventory.store_stock s WHERE s.store_id = %s AND NOT EXISTS"
        " (SELECT 1 FROM pricing.store_price_override o WHERE o.store_id = s.store_id"
        "  AND o.product_id = s.product_id AND o.valid_to IS NULL) ORDER BY random() LIMIT 1",
        (STORE,),
    ).fetchone()
    taken = conn.execute(
        "SELECT product_id, version, price FROM pricing.store_price_override"
        " WHERE store_id = %s AND valid_to IS NULL ORDER BY product_id LIMIT 1",
        (STORE,),
    ).fetchone()
    assert free is not None and taken is not None
    stage = (
        "INSERT INTO pricing_apply.store_override_stage (cycle_id, store_id, product_id, batch_no,"
        " new_price, expected_version) VALUES (%s, %s, %s, %s, %s, %s)"
    )
    conn.execute(stage, (cycle, STORE, free[0], 1, Decimal("3.49"), 0))
    conn.execute(stage, (cycle, STORE, taken[0], 1, Decimal("3.49"), 0))
    assert _apply(conn, cycle, "store_override") == ("store_override", 1, 2, 1, 0, 1, 0, False)
    st = _statuses(conn, "store_override_stage", cycle)
    assert st[(STORE, taken[0])] == ("conflict", "override_exists")
    assert conn.execute(
        "SELECT price, last_cycle_id, updated_by FROM pricing.store_price_override"
        " WHERE store_id = %s AND product_id = %s AND valid_to IS NULL",
        (STORE, free[0]),
    ).fetchone() == (Decimal("3.49"), cycle, "pricing-engine")

    # The next cycle updates the override it just created.
    nxt = cycle + 1
    conn.execute(stage, (nxt, STORE, free[0], 1, Decimal("3.99"), 1))
    assert _apply(conn, nxt, "store_override") == ("store_override", 1, 1, 1, 0, 0, 0, False)


def test_promo_price_must_stay_below_regular(oltp_owner: psycopg.Connection, cycle: int) -> None:
    conn = oltp_owner
    rows = conn.execute(
        "SELECT p.promo_id, p.product_id, p.zone_id, p.version, z.price FROM pricing.promo_price p"
        " JOIN pricing.zone_price z USING (product_id, zone_id) ORDER BY random() LIMIT 2"
    ).fetchall()
    (a, b) = rows
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO pricing_apply.promo_price_stage (cycle_id, promo_id, product_id, zone_id,"
            " batch_no, new_price, expected_version) VALUES (%s, %s, %s, %s, 1, %s, %s)",
            [(cycle, *a[:3], a[4], a[3]), (cycle, *b[:3], b[4] - Decimal("0.01"), b[3])],
        )
    assert _apply(conn, cycle, "promo_price") == ("promo_price", 1, 2, 1, 0, 0, 1, False)
    assert _statuses(conn, "promo_price_stage", cycle)[tuple(a[:3])] == (
        "rejected",
        "promo_not_below_regular",
    )


def _checksum(rows: list[tuple[int, int, int, Decimal]]) -> tuple[int, int, int]:
    cents = [(k1, k2, int(p * 100)) for k1, k2, _, p in rows]
    return (
        len(cents),
        sum(c for _, _, c in cents),
        sum(((k1 * 1000003 + k2) % 1000000007) * c for k1, k2, c in cents),
    )


def test_reconcile_matches_an_independent_checksum(
    oltp_owner: psycopg.Connection, cycle: int
) -> None:
    conn, zone = oltp_owner, _store_zone(oltp_owner)
    rows = _zone_rows(conn, 6, zone)
    _stage_zone(conn, cycle, [(p, zone, pr + Decimal("0.10"), v) for p, pr, v in rows[:5]])
    _stage_zone(conn, cycle, [(p, zone, pr + Decimal("0.10"), v + 9) for p, pr, v in rows[5:]])
    _apply(conn, cycle, "zone_price")

    cat = dict(
        conn.execute(
            "SELECT p.product_id, c.category_id FROM master.product p"
            " JOIN master.product_class c USING (class_id)"
        ).fetchall()
    )
    stage = conn.execute(
        "SELECT product_id, zone_id, status, new_price FROM pricing_apply.zone_price_stage"
        " WHERE cycle_id = %s",
        (cycle,),
    ).fetchall()
    actual = {
        (p, z): price
        for p, z, price in conn.execute(
            "SELECT product_id, zone_id, price FROM pricing.zone_price WHERE last_cycle_id = %s",
            (cycle,),
        ).fetchall()
    }
    expected: dict[tuple[int, int, str], tuple[int, int, int]] = {}
    for c in sorted({cat[p] for p, *_ in stage}):
        mine = [r for r in stage if cat[r[0]] == c]
        transit = [(p, z, 0, price) for p, z, _, price in mine]
        applied = [(p, z, 0, actual[(p, z)]) for p, z, s, _ in mine if s == "applied"]
        expected[(zone, c, "transit")] = _checksum(transit)
        expected[(zone, c, "applied")] = _checksum(applied)

    got = {
        (z, c, check): (n, int(s), int(w))
        for _, z, c, check, n, s, w in conn.execute(
            "SELECT * FROM pricing_apply.reconcile_actual(%s, 'zone_price')", (cycle,)
        ).fetchall()
    }
    assert got == expected
    assert sum(v[0] for k, v in got.items() if k[2] == "applied") == 5


def test_single_row_edits_during_apply_never_deadlock(
    oltp_owner: psycopg.Connection, oltp_settings: OltpSettings, cycle: int
) -> None:
    s = oltp_settings
    keys = oltp_owner.execute(
        "SELECT product_id, zone_id, price, version FROM pricing.zone_price"
    ).fetchall()
    errors: list[BaseException] = []
    done = threading.Event()

    def edit(seed: int) -> None:
        rng = random.Random(seed)
        with psycopg.connect(s.conninfo(s.owner_user, s.owner_password), autocommit=True) as c:
            c.execute("SET app.actor = 'store-ops'")
            while not done.is_set():
                p, z, *_ = rng.choice(keys)
                try:
                    c.execute(
                        "UPDATE pricing.zone_price SET price = price + 0.01"
                        " WHERE product_id = %s AND zone_id = %s",
                        (p, z),
                    )
                except psycopg.Error as e:
                    errors.append(e)

    threads = [threading.Thread(target=edit, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    try:
        for i in range(5):
            _stage_zone(
                oltp_owner,
                cycle + i,
                [(p, z, pr + Decimal("0.05"), v) for p, z, pr, v in keys],
            )
            received = _apply(oltp_owner, cycle + i, "zone_price")[2]
            assert received == len(keys)
    finally:
        done.set()
        for t in threads:
            t.join()
    assert errors == []
