"""`workload run` and `workload edit`: short OLTP transactions as role `workload` (PLAN 9).

Each worker thread owns one connection (actor `store-ops`) and a `random.Random(seed + n)`.
Every transaction touches at most one owned pricing row; multi-row stock updates go in
product order, so workers never deadlock each other.
"""

import logging
import random
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from types import FrameType
from typing import Any

import psycopg
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from retail_pricing.data.synth import Band, Grid
from retail_pricing.settings import OltpSettings, WorkloadSettings
from retail_pricing.workload import logic

log = logging.getLogger(__name__)
CENT = Decimal("0.01")


@dataclass(frozen=True)
class Context:
    grid: Grid
    stores: list[tuple[int, int]]  # (store_id, zone_id)
    assortment: dict[int, list[int]]  # store_id -> product ids in stock
    products: list[int]
    zone_keys: list[tuple[int, int]]
    kvi_keys: list[tuple[int, int]]


def connect(oltp: OltpSettings) -> psycopg.Connection:
    conn = psycopg.connect(oltp.conninfo(oltp.workload_user, oltp.workload_password))
    conn.execute("SET app.actor = 'store-ops'")
    conn.commit()
    return conn


def load_context(conn: psycopg.Connection) -> Context:
    def rows(q: str) -> list[tuple[int, ...]]:
        return conn.execute(q.encode()).fetchall()

    bands = conn.execute("SELECT min_price, max_price, ending FROM rules.price_ending_band")
    grid = Grid(Band(*b) for b in bands.fetchall())
    stores = [(s, z) for s, z in rows("SELECT store_id, zone_id FROM master.store ORDER BY 1")]
    assortment: dict[int, list[int]] = {}
    for s, p in rows("SELECT store_id, product_id FROM inventory.store_stock ORDER BY 1, 2"):
        assortment.setdefault(s, []).append(p)
    keys = [
        (p, z) for p, z in rows("SELECT product_id, zone_id FROM pricing.zone_price ORDER BY 1, 2")
    ]
    kvi = {p for (p,) in rows("SELECT product_id FROM rules.kvi")}
    conn.commit()
    return Context(
        grid=grid,
        stores=[s for s in stores if s[0] in assortment],
        assortment=assortment,
        products=sorted({p for p, _ in keys}),
        zone_keys=keys,
        kvi_keys=[k for k in keys if k[0] in kvi],
    )


def _business_date(conn: psycopg.Connection) -> date:
    row = conn.execute("SELECT business_date FROM ops.business_clock WHERE id = 1").fetchone()
    assert row is not None
    return row[0]  # type: ignore[no-any-return]


def tx_sale(conn: psycopg.Connection, rng: random.Random, ctx: Context) -> None:
    store_id, zone_id = rng.choice(ctx.stores)
    pool = ctx.assortment[store_id]
    pids = sorted(rng.sample(pool, min(logic.line_count(rng), len(pool))))
    bd = _business_date(conn)
    priced = conn.execute(
        """
        SELECT z.product_id, coalesce(o.price, z.price),
               (SELECT c.unit_cost FROM pricing.supplier_cost c WHERE c.product_id = z.product_id
                 ORDER BY c.valid_from DESC LIMIT 1)
          FROM pricing.zone_price z
          LEFT JOIN pricing.store_price_override o
            ON o.store_id = %s AND o.product_id = z.product_id AND o.valid_to IS NULL
         WHERE z.zone_id = %s AND z.product_id = ANY(%s)
         ORDER BY z.product_id
        """,
        (store_id, zone_id, pids),
    ).fetchall()
    lines = [(p, rng.randint(1, 3), price, cost) for p, price, cost in priced]
    total = sum((q * price for _, q, price, _ in lines), Decimal(0))
    row = conn.execute(
        "INSERT INTO sales.sale (channel, store_id, business_date, sold_at, total_amount)"
        " VALUES ('STORE', %s, %s, now(), %s) RETURNING sale_id",
        (store_id, bd, total),
    ).fetchone()
    assert row is not None
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO sales.sale_line (sale_id, line_no, product_id, quantity, unit_price,"
            " discount_amount, net_amount, unit_cost) VALUES (%s, %s, %s, %s, %s, 0, %s, %s)",
            [
                (row[0], n, p, q, price, q * price, (cost or price * Decimal("0.7")).quantize(CENT))
                for n, (p, q, price, cost) in enumerate(lines, 1)
            ],
        )
    for p, q, _, _ in lines:  # product order: the lock order
        _move_stock(conn, rng, store_id, p, -q, "SALE", bd)


def _move_stock(
    conn: psycopg.Connection,
    rng: random.Random,
    store_id: int,
    product_id: int,
    qty: int,
    kind: str,
    bd: date,
) -> None:
    row = conn.execute(
        "UPDATE inventory.store_stock SET on_hand = greatest(on_hand + %s, 0)"
        " WHERE store_id = %s AND product_id = %s RETURNING on_hand",
        (qty, store_id, product_id),
    ).fetchone()
    movements = [(kind, qty)]
    if row is not None and (receive := logic.receipt_qty(rng, row[0])):
        conn.execute(
            "UPDATE inventory.store_stock SET on_hand = on_hand + %s, last_received_date = %s"
            " WHERE store_id = %s AND product_id = %s",
            (receive, bd, store_id, product_id),
        )
        movements.append(("RECEIPT", receive))
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO inventory.stock_movement"
            " (store_id, product_id, movement_type, qty, business_date)"
            " VALUES (%s, %s, %s, %s, %s)",
            [(store_id, product_id, k, q, bd) for k, q in movements],
        )


def tx_return(conn: psycopg.Connection, rng: random.Random, ctx: Context) -> None:
    row = conn.execute(
        "SELECT l.sale_line_id, l.product_id, l.quantity, l.net_amount, s.store_id"
        "  FROM sales.sale_line l JOIN sales.sale s USING (sale_id)"
        " WHERE s.store_id IS NOT NULL ORDER BY l.sale_line_id DESC OFFSET %s LIMIT 1",
        (rng.randrange(500),),
    ).fetchone()
    if row is None:
        return
    line_id, product_id, quantity, net, store_id = row
    qty = rng.randint(1, quantity)
    bd = _business_date(conn)
    conn.execute(
        "INSERT INTO sales.sale_return (sale_line_id, quantity, amount, business_date)"
        " VALUES (%s, %s, %s, %s)",
        (line_id, qty, (net * qty / quantity).quantize(CENT), bd),
    )
    _move_stock(conn, rng, store_id, product_id, qty, "RETURN", bd)


def tx_supplier_cost(conn: psycopg.Connection, rng: random.Random, ctx: Context) -> None:
    factor = Decimal(str(round(1.002 + rng.uniform(-0.01, 0.01), 4)))
    conn.execute(
        "INSERT INTO pricing.supplier_cost (product_id, valid_from, supplier_id, unit_cost)"
        " SELECT product_id, (SELECT business_date FROM ops.business_clock WHERE id = 1),"
        "        supplier_id, round(unit_cost * %s, 4)"
        "   FROM pricing.supplier_cost WHERE product_id = %s"
        "  ORDER BY valid_from DESC LIMIT 1"
        " ON CONFLICT DO NOTHING",
        (factor, rng.choice(ctx.products)),
    )


def edit_zone_price(
    conn: psycopg.Connection, rng: random.Random, grid: Grid, key: tuple[int, int]
) -> None:
    row = conn.execute(
        "SELECT price FROM pricing.zone_price WHERE product_id = %s AND zone_id = %s FOR UPDATE",
        key,
    ).fetchone()
    if row is None:
        return
    conn.execute(
        "UPDATE pricing.zone_price SET price = %s,"
        " effective_from = (SELECT business_date FROM ops.business_clock WHERE id = 1)"
        " WHERE product_id = %s AND zone_id = %s",
        (logic.edited_price(rng, row[0], grid), *key),
    )


def tx_price_edit(conn: psycopg.Connection, rng: random.Random, ctx: Context) -> None:
    keys = ctx.kvi_keys if ctx.kvi_keys and rng.random() < 0.5 else ctx.zone_keys
    edit_zone_price(conn, rng, ctx.grid, rng.choice(keys))


def tx_override_edit(conn: psycopg.Connection, rng: random.Random, ctx: Context) -> None:
    n = conn.execute(
        "SELECT count(*) FROM pricing.store_price_override WHERE valid_to IS NULL"
    ).fetchone()
    if not n or not n[0]:
        return
    row = conn.execute(
        "SELECT override_id, price FROM pricing.store_price_override WHERE valid_to IS NULL"
        " ORDER BY override_id OFFSET %s LIMIT 1 FOR UPDATE",
        (rng.randrange(n[0]),),
    ).fetchone()
    if row is not None:
        conn.execute(
            "UPDATE pricing.store_price_override SET price = %s WHERE override_id = %s",
            (logic.edited_price(rng, row[1], ctx.grid), row[0]),
        )


def tx_label_print(conn: psycopg.Connection, rng: random.Random, ctx: Context) -> None:
    conn.execute(
        "UPDATE pricing.shelf_label_queue SET status = 'printed' WHERE label_id IN"
        " (SELECT label_id FROM pricing.shelf_label_queue WHERE status = 'queued'"
        "   ORDER BY label_id LIMIT 50 FOR UPDATE SKIP LOCKED)"
    )


TRANSACTIONS: dict[str, Callable[[psycopg.Connection, random.Random, Context], None]] = {
    "sale": tx_sale,
    "return": tx_return,
    "price_edit": tx_price_edit,
    "override_edit": tx_override_edit,
    "supplier_cost": tx_supplier_cost,
    "label_print": tx_label_print,
}


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.tx = Counter(
            "workload_transactions", "Committed transactions", ["type"], registry=self.registry
        )
        self.errors = Counter(
            "workload_errors", "Failed transactions", ["type"], registry=self.registry
        )
        self.latency = Histogram(
            "workload_transaction_seconds", "Transaction latency", ["type"], registry=self.registry
        )
        self.business_date = Gauge(
            "workload_business_date_seconds",
            "Business date as a Unix timestamp (00:00 UTC)",
            registry=self.registry,
        )

    def set_date(self, d: date) -> None:
        self.business_date.set(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp())

    def summary(self) -> dict[str, int]:
        out = {}
        for metric in self.registry.collect():
            for s in metric.samples:
                if s.name in ("workload_transactions_total", "workload_errors_total"):
                    key = s.labels["type"] + ("_errors" if "errors" in s.name else "")
                    out[key] = int(s.value)
        out["errors"] = sum(v for k, v in out.items() if k.endswith("_errors"))
        return out


def _worker(
    n: int,
    settings: WorkloadSettings,
    oltp: OltpSettings,
    ctx: Context,
    metrics: Metrics,
    stop: threading.Event,
) -> None:
    rng = random.Random(settings.seed + n)
    per_second = logic.rates(settings)
    with connect(oltp) as conn:
        while True:
            delay, kind = logic.next_event(rng, per_second, settings.workers)
            if stop.wait(delay):
                return
            t0 = time.perf_counter()
            try:
                with conn.transaction():
                    TRANSACTIONS[kind](conn, rng, ctx)
            except psycopg.Error:
                metrics.errors.labels(kind).inc()
                log.warning("workload transaction failed", extra={"type": kind}, exc_info=True)
                continue
            metrics.latency.labels(kind).observe(time.perf_counter() - t0)
            metrics.tx.labels(kind).inc()


def run(
    settings: WorkloadSettings, oltp: OltpSettings, duration: float | None = None
) -> dict[str, int]:
    """Run until SIGTERM/SIGINT or `duration` seconds; returns counts per type and errors."""
    stop = threading.Event()
    metrics = Metrics()
    server, _ = start_http_server(settings.metrics_port, registry=metrics.registry)
    previous: dict[signal.Signals, Any] = {}
    if threading.current_thread() is threading.main_thread():

        def _stop(signum: int, frame: FrameType | None) -> None:
            stop.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, _stop)
    try:
        with connect(oltp) as conn:
            ctx = load_context(conn)
            metrics.set_date(_business_date(conn))
            conn.commit()
            threads = [
                threading.Thread(
                    target=_worker, args=(n, settings, oltp, ctx, metrics, stop), daemon=True
                )
                for n in range(settings.workers)
            ]
            for t in threads:
                t.start()
            log.info("workload started", extra={"workers": settings.workers})
            end = None if duration is None else time.monotonic() + duration
            next_day = time.monotonic() + settings.day_seconds
            while not stop.is_set():
                now = time.monotonic()
                if end is not None and now >= end:
                    break
                if now >= next_day:
                    with conn.transaction():
                        row = conn.execute(
                            "UPDATE ops.business_clock SET business_date = business_date + 1"
                            " WHERE id = 1 RETURNING business_date"
                        ).fetchone()
                    if row:
                        metrics.set_date(row[0])
                        log.info("business day advanced", extra={"business_date": row[0]})
                    next_day += settings.day_seconds
                stop.wait((next_day if end is None else min(next_day, end)) - now)
            stop.set()
            for t in threads:
                t.join()
    finally:
        stop.set()
        server.shutdown()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    summary = metrics.summary()
    log.info("workload stopped", extra=summary)
    return summary


def edit(count: int, kvi: bool, seed: int, oltp: OltpSettings) -> int:
    """Scripted manual edits of `count` zone prices (KVI first with `kvi`), one per transaction."""
    rng = random.Random(seed)
    with connect(oltp) as conn:
        ctx = load_context(conn)
        kvi_ids = {p for p, _ in ctx.kvi_keys}
        keys = logic.pick_edit_keys(ctx.zone_keys, kvi_ids, count, kvi, rng)
        for key in keys:
            with conn.transaction():
                edit_zone_price(conn, rng, ctx.grid, key)
    log.info("zone prices edited", extra={"count": len(keys), "kvi": kvi, "seed": seed})
    return len(keys)
