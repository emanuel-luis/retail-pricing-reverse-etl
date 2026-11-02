# Implementation plan

This is the contract every pull request follows. A change that contradicts it updates this file
and `docs/DECISIONS.md` in the same PR. Values marked "est." are estimates. The first PR that
measures one replaces the estimate with the measured value.

Scope: a laptop-scale ELT plus reverse-load loop for retail pricing. TPC-DS data generated locally
plus deterministic synthetic tables feed a busy OLTP Postgres. Apache NiFi extracts it to Parquet
on S3-compatible storage, and ClickHouse plus dbt compute new prices. NiFi writes only the changed
rows back to the OLTP. A control/audit Postgres records every job, run, step and batch, and
Prometheus and Grafana monitor the loop.

Non-goals: hard-delete propagation, schema evolution without a reset, NiFi clustering, TLS between
internal services (NiFi keeps its built-in HTTPS), cloud deployment, SF100 execution (described
only), and a real notification channel for alerts.

TPC notice (repeat it wherever timings appear): the data comes from the TPC-DS data generator,
run locally through DuckDB's `tpcds` extension. These are not TPC-DS benchmark results and are
not comparable to published TPC Benchmark results. dsdgen is never redistributed and generated
data is never committed.

---

## 1. Architecture and services

```
workload --> [oltp Postgres] <-- apply functions <-- NiFi reverse groups <-- ClickHouse reverse.*
                  |                                                              ^
            NiFi extract (JDBC, paged) --> Parquet --> [s3 RustFS] --> NiFi load --+ (CH pulls via s3())
                                                                                  |
                                       NiFi transform --> dbt-runner --> dbt build on ClickHouse
NiFi (every step) --> [control-db Postgres: registry + audit] --> Grafana pipeline overview
Prometheus <-- NiFi (bearer token from nifi-token), ClickHouse :9363, postgres_exporter x2,
               dbt-runner /metrics, workload /metrics
```

A cycle is one pass of the loop: trigger, extract, load, transform, reverse (4 ordered targets),
reconcile, finalize. The next cycle's extract brings back the prices the previous cycle applied.

Compose project name `retail-pricing-reverse-etl`. There is no `container_name` and every
service has a healthcheck. Volumes are named. `.env` sets `COMPOSE_PROFILES=core` so a bare
`docker compose up` starts the core loop. Every host port comes from `.env` and binds to
`127.0.0.1`, in the 2xxxx range (sibling repos use 1xxxx).

| Service | Image (pinned tag, digest added in PR) | Profile | Purpose | Host port var = default | mem_limit | Memory settings |
|---|---|---|---|---|---|---|
| oltp | postgres:18.6-alpine | core | Store-operations OLTP, db `retail` | OLTP_PG_PORT=25432 | 1280m | shared_buffers=256MB, effective_cache_size=768MB, work_mem=16MB, maintenance_work_mem=128MB, max_connections=60, max_wal_size=4GB, shared_preload_libraries=pg_stat_statements, track_io_timing=on |
| control-db | postgres:18.6-alpine | core | Registry and audit, db `control` | CONTROL_PG_PORT=25433 | 256m | shared_buffers=32MB, work_mem=4MB, max_connections=40 |
| s3 | rustfs/rustfs:1.0.4 | core | S3-compatible raw zone, bucket `retail-raw` | S3_API_PORT=29000, S3_CONSOLE_PORT=29001 | 384m | none |
| clickhouse | clickhouse/clickhouse-server:26.3.62.3 | core | Raw tables, dbt models, reverse diffs | CLICKHOUSE_HTTP_PORT=28123 | 2304m | server: max_server_memory_usage_to_ram_ratio=0.8, mark_cache_size=256MiB, index_mark_cache_size=64MiB, uncompressed cache off, background_pool_size=4. Profile `etl`: max_memory_usage=1.2GB, max_bytes_before_external_group_by=512MiB, max_bytes_before_external_sort=512MiB, max_threads=4, join_algorithm=grace_hash,parallel_hash |
| nifi | retail-pricing-nifi:local (built from apache/nifi:2.12.0) | core | All flows | NIFI_HTTPS_PORT=28443 (UI/API), NIFI_TRIGGER_PORT=28081 (cycle trigger) | 2048m | NIFI_JVM_HEAP_INIT=NIFI_JVM_HEAP_MAX=1280m |
| dbt-runner | retail-pricing-app:local | core | Runs `dbt build` on request | DBT_RUNNER_PORT=28580 | 512m | dbt threads=2 |
| init | retail-pricing-app:local | core (one-shot) | Control DDL plus registry seed, ClickHouse raw DDL, bucket | none | 256m | none |
| prometheus | prom/prometheus:v3.15.1 | monitoring | Metrics | PROMETHEUS_PORT=29090 | 384m | retention 7d / 1GB |
| grafana | grafana/grafana:13.2.5 | monitoring | Dashboards, alert rules | GRAFANA_PORT=23000 | 320m | GF_PLUGINS_PREINSTALL=grafana-clickhouse-datasource@4.23.2 |
| pg-exporter-oltp | prometheuscommunity/postgres-exporter:v0.20.1 | monitoring | OLTP metrics | none | 64m | none |
| pg-exporter-control | same | monitoring | Control DB metrics | none | 64m | none |
| nifi-token | retail-pricing-app:local | monitoring | Refreshes the NiFi JWT into a shared file | none | 96m | none |
| workload | retail-pricing-app:local | workload | Concurrent OLTP traffic | none (metrics 9108 internal) | 256m | none |

Totals: core long-running 6,784 MiB (about 6.6 GiB), plus init 256 MiB while it runs. Monitoring
adds 992 MiB and workload 256 MiB, so everything together is about 7.8 GiB. SF10 uses the same
limits: ClickHouse spills to disk, NiFi pages, and Postgres reads from disk. Anything bigger is
reported, never fixed by raising limits or editing `.wslconfig`.

The app image is built once by `init` (`image: retail-pricing-app:local`). Every other app
service sets the same `image` with `pull_policy: never`, because compose 2.40 refuses two services
building one tag. NiFi has its own Dockerfile.

The host CLI (`uv run retail-pricing ...`) runs data generation, the OLTP load, NiFi deploy, cycle
triggers, checks and reports over the published ports. That keeps 3 GB of Parquet off Windows
bind mounts and keeps DuckDB outside the Docker VM memory budget. Containers run only services.

---

## 2. Repository layout

```
.github/workflows/ci.yml              lint, test-fast, sql, dbt, e2e, diagrams
.github/workflows/security.yml        exists (pip-audit, zizmor)
.github/dependabot.yml
compose.yaml  .env.example  Makefile  pyproject.toml  uv.lock  .python-version  .dockerignore
README.md  CLAUDE.md  CHANGELOG.md  LICENSE
docker/app.Dockerfile                 python:3.12-slim multi-stage, uv sync --locked --no-dev --extra dbt, uid 10001
docker/nifi.Dockerfile                apache/nifi:2.12.0 + JDBC jar + Parquet NARs, sha256-verified
config/clickhouse/config.d/*.xml      memory, prometheus, named collection raw_s3 (from_env)
config/clickhouse/users.d/*.xml       users nifi, dbt, grafana; profile etl (from_env passwords)
config/postgres/oltp.conf  control.conf
config/prometheus/prometheus.yml
config/grafana/provisioning/{datasources,dashboards,alerting}/*.yaml
config/grafana/dashboards/{pipeline-overview,pricing-outcomes,platform}.json
dbt/                                  dbt_project.yml, profiles.yml (env_var only), models/, macros/, tests/
nifi/flows/retail-pricing.json        GENERATED by `retail-pricing nifi build-flows`; CI diffs it
nifi/params/{local,ci}.json           non-sensitive parameters + env-var names of sensitive ones
nifi/definitions/*.json               processor definitions pulled from NiFi 2.12 (builder test fixtures)
nifi/spike/reference-flow.json        hand-built flow exported from NiFi (pins the JSON shape)
scripts/demo.sh  scripts/heavy.sh  scripts/sample-stats.sh
src/retail_pricing/
  cli.py settings.py logging.py health.py
  data/ (generate.py build.py manifest.py synth.py)
  oltp/ (load.py)  control/ (init.py)  clickhouse/ (client.py init.py load_raw.py)
  nifi/ (client.py definitions.py builder/ deploy.py token.py)
  dbt_runner/ (server.py jobs.py)  workload/ (generator.py edits.py metrics.py)
  cycle.py check.py report.py
  sql/duckdb/*.sql  sql/oltp/*.sql  sql/control/*.sql  sql/clickhouse/*.sql   (package data)
tests/unit/  tests/sql/  tests/clickhouse/  tests/integration/  tests/fixtures/
docs/PLAN.md DECISIONS.md RESULTS.md RUNBOOK.md diagrams/{_style,architecture,cycle,oltp-model,dbt-dag,monitoring}.d2 (+ .svg)
data/ runs/                           git-ignored (generated data, run reports)
```

Python baseline: 3.12, uv with a committed lock, a PEP 735 `dev` group (ruff, mypy, pytest,
pytest-timeout, hypothesis) and an optional extra `dbt` (dbt-core 1.10.x, dbt-clickhouse 1.10.3).
Runtime dependencies: typer, pydantic-settings, psycopg[binary] 3, duckdb==2.0.0, pyarrow, httpx,
boto3 (bucket creation and checks), prometheus-client. ruff runs with line length 100 and extends
the defaults with BLE, I, UP, B and SIM. mypy is strict on `src`. pytest runs with
`-ra --strict-markers --strict-config` and a timeout. Markers `sql`, `clickhouse`, `integration`
and `slow` are deselected by default. Logs are JSON lines with UTC timestamps, and `cycle_id` and
`run_id` come from contextvars. Secrets are `SecretStr`.

---

## 3. Data

### 3.1 Generation (`retail-pricing data generate --sf N`)
DuckDB 2.0.0 runs `INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=N)` against a file database
`data/tpcds/sf=N/tpcds.duckdb`, which lets the buffer manager evict. It uses memory_limit from
settings (default 4GB) and 8 threads. The tables needed are copied to
`data/tpcds/sf=N/<table>.parquet` (zstd), then the `.duckdb` file is deleted. `manifest.json`
records the DuckDB and extension versions, SF, each table's row count and file sha256, and each
table's content hash (below). If the manifest exists and its counts match, the step is skipped.
For SF0.01, 1 and 10 the counts are checked against the known values; a mismatch is an error.

Content hash, order-independent and engine-version-tolerant: `count(*)` plus
`sum(md5_number_lower(CAST(t AS VARCHAR)))::HUGEINT % 2^64` over `FROM <relation> t`, written as
hex. `CAST(t AS VARCHAR)` renders the whole row as a struct (session TimeZone UTC); the sum of
UBIGINT values is an exact HUGEINT. Note that `hash(t.*)` is invalid in DuckDB 2.0,
`struct_pack(COLUMNS(*))` expands to one struct per column (not one per row), and
`sum(md5_number(...))` returns a lossy DOUBLE. Parquet is read with `hive_partitioning = false`
so the `sf=<N>` directory never becomes a column. The file sha256 detects corruption. The content
hash is the reproducibility claim.

### 3.2 Build (`retail-pricing data build --sf N`)
DuckDB SQL files in `sql/duckdb/` (one per OLTP table, in FK order) read the TPC-DS Parquet and
write `data/oltp/sf=N/<schema>__<table>.parquet`. The columns are exactly the OLTP table's,
in DDL order, including `version=1`, `updated_at=<anchor> 00:00 UTC` and `last_cycle_id=NULL`, so
the same files feed both the Postgres COPY and the CI-only ClickHouse load. A manifest records row
counts, content hashes, dropped-row counts per rule and the anchor.

Deterministic randomness: `u(key, salt) = (md5_number(salt || ':' || key) % 1000000) / 1e6`. It
depends only on stable inputs and is implemented once in DuckDB SQL and once in Python
(`synth.py`, used by tests and the workload). A unit test proves both give the same values.

Date anchor: every TPC-DS date shifts by 8,372 days (23 x 364, so weekdays and seasons are kept).
Sales 1998-01-02..2003-01-02 become 2020-12-04..2025-12-04. Anchor business date: 2025-12-04
(Thursday). The workload's business clock continues from 2025-12-05. Pricing always uses
`as_of` = the cycle's business date read from the OLTP at extract time, never the wall clock.
`updated_at` (wall clock) only drives watermarks.

Null rule: a row whose required key is null (sold date, item, quantity) is dropped and counted in
the manifest. A null optional FK becomes NULL, except a null `ss_store_sk`, which goes to store
`1 + floor(u(ticket,'store') * stores)` so volume is kept.
### 3.3 TPC-DS to OLTP mapping

| OLTP table | Source | Rule |
|---|---|---|
| master.category | item.i_category_id/i_category | 10 rows + id 99 "Other" for nulls |
| master.product_class | item (i_category_id, i_class_id, i_class) | dense id per (category, class) |
| master.brand | item.i_brand_id/i_brand | tier synth: 1 private label (u<0.15), 3 premium (u>0.85), else 2 national |
| master.supplier | item.i_manufact_id/i_manufact | 1 supplier per manufacturer |
| master.product | item current version (i_rec_end_date null) per i_item_id | product_id = dense rank of i_item_id; code = i_item_id; class, brand, supplier; size = i_size; pack_units from size (petite 0.5, small 1, medium 2, large 3, extra large 4, economy 6, N/A 1) |
| master.price_zone | synth from store count | Z = min(8, max(1, round(stores/4))) store zones + 1 WEB zone; price_factor = 0.96 + 0.02*(zone_no mod 5) |
| master.store | store current versions | zone = ((store_sk-1) mod Z)+1; price_adjust_pct = 0 for 70%, else 1..4% |
| master.warehouse | warehouse | as is |
| master.customer | customer + customer_address | name, email, city, state, zip |
| inventory.warehouse_stock | inventory last weekly snapshot per (warehouse, item) | item mapped to product; on_hand = inv_quantity_on_hand (null to 0) |
| inventory.store_stock | synth | assortment = (store, product) sold in the last 365 days; on_hand = round(avg daily units 90d * cover), cover = 3 + 87*u; last_received_date = anchor - floor(180*u) |
| inventory.stock_movement | synth | one RECEIPT per store_stock row at last_received_date; grows with the workload |
| pricing.zone_price | product x zone | i_current_price * zone price_factor, rounded to the ending grid, ladder-repaired so seed prices are monotone |
| pricing.store_price_override | synth | 2% of store_stock rows of stores with price_adjust_pct > 0: zone price * (1 + adj), ending-rounded |
| pricing.promotion | promotion | re-timed: start = anchor - 14 + floor(42*u), length 7..28 days; discount_pct 10..30% |
| pricing.promo_item | promotion.p_item_sk + synth | the TPC-DS item + 4..19 more products of the same class (by u) |
| pricing.promo_price | promo_item x zone | regular price * (1 - discount_pct), rounded down to the ending grid |
| pricing.supplier_cost | item SCD versions + synth | each version's i_wholesale_cost capped at 0.85*price is the base; 24 monthly points before the anchor, drift +0.2%/month, noise +-1%, a 5..10% supplier jump for 10% of products; 3% of products seeded below min margin |
| pricing.price_history | item SCD versions x zone | valid_from/valid_to from i_rec_start/end_date (shifted), source 'seed' |
| pricing.price_change, pricing.shelf_label_queue | none | empty; written by apply |
| market.competitor | synth | 3 rows: discounter (bias -4%), regional (+1%), premium (+5%) |
| market.competitor_price | synth | all KVI + 25% of products (by u); each competitor in ~70% of zones; weekly for 8 weeks; our seed price * (1+bias+noise +-3%), ending .x9 |
| rules.category_rule | synth per category | target margin 25..45%, min margin = target - 10 pts, max_change_pct 5% (3% for categories with >5% KVI), cover_min 7 / cover_max 60 days, aging_max 120 days, promo_policy keep_depth (70%) or exclude |
| rules.kvi | sales | top 2% products by 365-day store units (min 5); tier 1 = top 0.5%; competitor_index_target 0.98 (tier 1) / 1.00 |
| rules.price_ending_band | static | [0,10): .49,.99; [10,50): .99; [50,inf): .90 |
| rules.ladder_group, rules.ladder_member | synth | PACK: (class, brand) groups with >= 2 distinct pack_units, rank by pack_units. TIER: per (class, size) one product per brand tier (smallest u), groups with >= 2 tiers, rank by tier |
| sales.sale | store_sales by (store, ticket); web_sales by order | channel STORE/WEB; WEB uses zone WEB; business_date shifted; sold_at = date + hash time 08:00..22:00 |
| sales.sale_line | store_sales, web_sales lines | sale_line_id dense; product via item_sk to i_item_id; unit_price = sales_price, discount, net, unit_cost = wholesale_cost; promo_id when promo_sk maps |
| sales.sale_return | store_returns, web_returns | linked to the line by (ticket/order, item) |
| ops.business_clock | static | one row: business_date = anchor |

The catalog channel is dropped: it has no store or zone, and keeping it would add about 30% volume
with no pricing signal.

### 3.4 Expected row counts
TPC-DS inputs are known (see the facts in DECISIONS). OLTP outputs are estimates until PR 1
measures them.

| Table | SF0.01 | SF1 | SF10 |
|---|---|---|---|
| product | ~90 est. | 9,000 | 51,000 |
| store / warehouse / zones (incl. WEB) | 1 / 1 / 2 | 12 / 5 / 4 | 102 / 10 / 9 |
| sale_line | ~36k est. | ~3.6M (2,880,404 + 719,384 minus nulls) | ~36.0M (28,800,991 + 7,197,566) |
| sale (headers) | ~3.5k est. | ~0.33M est. | ~3.3M est. |
| sale_return | ~3.6k est. | ~0.36M (287,514 + 71,763) | ~3.59M |
| zone_price | ~180 | 36,000 | 459,000 |
| store_stock | ~90 | ~0.1M est. | ~3.5M est. |
| warehouse_stock | ~90 | 45,000 | 510,000 |
| supplier_cost | ~2.2k | 216,000 | 1,224,000 |
| price_history | ~360 | 72,000 | 918,000 |
| competitor_price | ~0.5k est. | ~0.26M est. | ~3.3M est. |
| promo_price | ~60 est. | ~12k est. | ~45k est. |

---

## 4. OLTP (database `retail`)

Schemas: `master`, `inventory`, `pricing`, `market`, `rules`, `sales`, `ops`, `pricing_apply`.
Every table has `version bigint not null default 1` and `updated_at timestamptz not null default
now()`. The generic trigger `ops.touch_row()` (BEFORE UPDATE) sets `NEW.version = OLD.version + 1`
and `NEW.updated_at = now()`. Every extracted table has a btree index on `updated_at`. PKs are
dense bigint/int identities, so GenerateTableFetch can partition on them. FKs are indexed. There
are no hard deletes: overrides end by `valid_to`.

Owned-by-pricing tables (`zone_price`, `store_price_override`, `promo_price`) also carry
`last_cycle_id bigint` and `updated_by text`. Trigger `pricing.track_price()` (BEFORE UPDATE)
sets `updated_by = coalesce(current_setting('app.actor', true), 'unknown')` and sets
`last_cycle_id = NULL` unless the actor is `pricing-engine`. An AFTER UPDATE trigger on
`zone_price` closes the open `price_history` row and inserts a new one with source
`engine`/`manual` and the cycle id.

Key tables:
- `master.product(product_id PK, product_code UNIQUE, class_id FK, brand_id FK, supplier_id FK, size, pack_units numeric(5,2), is_active)`
- `master.store(store_id PK, store_code UNIQUE, zone_id FK, state, price_adjust_pct numeric(5,4))`
- `inventory.store_stock(store_id, product_id) PK, on_hand int, last_received_date date`; `inventory.warehouse_stock(warehouse_id, product_id) PK`; `inventory.stock_movement(movement_id PK, store_id, product_id, movement_type, qty, business_date)`, index (store_id, product_id, business_date)
- `pricing.zone_price(product_id, zone_id) PK, price numeric(10,2) CHECK > 0, effective_from date`
- `pricing.store_price_override(override_id PK, store_id, product_id, price, valid_from, valid_to)`, UNIQUE (store_id, product_id) WHERE valid_to IS NULL
- `pricing.promotion(promo_id PK, promo_code, start_date, end_date, discount_pct, channel)`; `pricing.promo_item(promo_id, product_id) PK`; `pricing.promo_price(promo_id, product_id, zone_id) PK, promo_price`
- `pricing.supplier_cost(product_id, valid_from) PK, supplier_id, unit_cost numeric(12,4)`
- `pricing.price_history(history_id PK, product_id, zone_id, price, valid_from timestamptz, valid_to, source, cycle_id)`, index (product_id, zone_id, valid_from)
- `pricing.price_change(change_id PK, cycle_id, target, product_id, zone_id, store_id, promo_id, old_price, new_price, reason_code, applied_at)`
- `pricing.shelf_label_queue(label_id PK, cycle_id, store_id, product_id, price, promo_price, due_date, status queued|printed)`, UNIQUE (cycle_id, store_id, product_id)
- `market.competitor`, `market.competitor_price(observation_id PK, competitor_id, product_id, zone_id, observed_date, price)`
- `rules.category_rule(category_id PK, ...)`, `rules.kvi(product_id PK, kvi_tier, competitor_index_target)`, `rules.price_ending_band`, `rules.ladder_group`, `rules.ladder_member(ladder_group_id, product_id) PK, rank`
- `sales.sale(sale_id PK, channel, store_id, customer_id, business_date, sold_at, total_amount)`, index (business_date); `sales.sale_line(sale_line_id PK, sale_id FK, line_no, product_id, quantity, unit_price, discount_amount, net_amount, unit_cost, promo_id)`, index (sale_id), (product_id); `sales.sale_return(return_id PK, sale_line_id FK, quantity, amount, business_date)`
- `ops.business_clock(id PK CHECK id=1, business_date)`

### 4.1 Staging and apply (`pricing_apply`)
Staging tables, one per target, are written by NiFi with PutDatabaseRecord INSERT_IGNORE:
- `zone_price_stage(cycle_id, product_id, zone_id) PK, batch_no, new_price, old_price, expected_version, reason_code, row_hash, status, status_detail, processed_at`
- `store_override_stage(cycle_id, store_id, product_id) PK, ... expected_version (0 = create)`
- `promo_price_stage(cycle_id, promo_id, product_id, zone_id) PK, ...`
- `shelf_label_stage(cycle_id, store_id, product_id) PK, price, promo_price, due_date, source_target, zone_id`
- `batch(cycle_id, target, batch_no) PK, received, applied, unchanged, conflicts, rejects, processed_at`, the idempotency record
- `conflict(cycle_id, target, key_text) PK, expected_version, actual_version, proposed_price, actual_price, actual_updated_by, detected_at`

`pricing_apply.apply_batch(p_cycle_id bigint, p_target text, p_batch_no int) RETURNS TABLE(target text, batch_no int, received int, applied int, unchanged int, conflicts int, rejects int, replayed boolean)` is SECURITY DEFINER, owned by `oltp_owner`, EXECUTE granted to `nifi`. It is one set-based transaction:
1. `INSERT INTO batch ... ON CONFLICT DO NOTHING RETURNING`. If no row comes back, the batch was processed before: return the stored counts with `replayed = true`. This makes it idempotent per cycle and retry-safe.
2. `set_config('app.actor', 'pricing-engine', true)`.
3. Lock the target rows of the batch `ORDER BY key FOR UPDATE`. A fixed lock order means no deadlocks with single-row workload edits.
4. Reject: price <= 0, unknown key, or promo price >= regular price. Status `rejected`, with a detail.
5. Unchanged: the target already has `last_cycle_id = p_cycle_id` (an earlier partial attempt) or `price = new_price`.
6. Apply: `UPDATE target SET price = s.new_price, last_cycle_id = p_cycle_id FROM stage s WHERE key match AND t.version = s.expected_version AND t.price IS DISTINCT FROM s.new_price`. The version comes from the trigger. Insert `pricing.price_change` rows for the applied keys.
7. Conflict: the rest, where the version differs. Insert into `conflict` with the actual version, price and `updated_by`.
8. Write the statuses back to the stage rows and the counts to `batch`, and return them.

Target specifics: `store_override` with `expected_version = 0` inserts a new open override (ON
CONFLICT DO NOTHING; a lost race is a conflict). `shelf_label` applies a row only if its source
price row (zone or override stage for the same cycle and key) has status `applied` or `unchanged`
with `last_cycle_id = cycle`. Otherwise it is rejected with `price_not_applied`. It inserts into
`shelf_label_queue` ON CONFLICT DO NOTHING.

`pricing_apply.reconcile_actual(p_cycle_id bigint, p_target text) RETURNS TABLE(target, zone_id, category_id, check_name, rows bigint, sum_cents numeric, wsum numeric)` returns two checks per group:
- `transit`: all staged rows with their proposed values.
- `applied`: target rows with `last_cycle_id = cycle`, current values, joined to stage rows with status applied.

The checksum works the same in both engines: rows, `sum(price_cents)`,
`sum(((key1*1000003 + key2) % 1000000007) * price_cents)`.

Roles: `oltp_owner`, `workload` (DML on sales, inventory, pricing manual edits, ops),
`nifi` (SELECT on extracted tables, INSERT/SELECT on stage tables, EXECUTE on the apply and
reconcile functions), `exporter` (pg_monitor). Passwords come from env. `oltp load` creates the
roles with psycopg parameters.

Load (`retail-pricing oltp load --sf N`) applies `010_tables.sql` (PKs only), then COPYs each
Parquet as CSV produced by pyarrow in record batches, streamed to `COPY ... FROM STDIN (FORMAT
csv)`. After that it applies `020_constraints.sql`, `030_indexes.sql`, `040_triggers.sql`,
`050_apply.sql` and `060_roles.sql`, resets the identity sequences and runs ANALYZE. It times
each phase. The DDL is idempotent (IF NOT EXISTS, CREATE OR REPLACE) and there is no migration
tool. A schema change means `make down` (DECISIONS).

---
## 5. Control and audit (database `control`)

`registry` schema:
- `job(job_id text PK, process_group_name, nifi_process_group_id, source, target, schedule_interval interval NULL, owner, sla interval, enabled bool, stage_order int, batch_rows int NULL)`. Seed jobs: `cycle` (schedule), `extract`, `load`, `transform`, `reverse.zone_price` (batch_rows 1000), `reverse.store_override` (1000), `reverse.promo_price` (1000), `reverse.shelf_label` (5000), `reconcile`. Deploy fills `nifi_process_group_id`. A disabled job's stage plan is empty and its run ends `skipped`.
- `extract_table(table_name PK, enabled, pk_column, select_list, page_rows, load_order)`. The extract list is data, not flow: adding a table needs no flow change.
- `watermark(table_name PK, high_value timestamptz, cycle_id, updated_at)`.

`audit` schema:
- `cycle(cycle_id bigint identity PK, mode full|incremental, requested_by, business_date, status running|succeeded|failed, failed_stage, error, started_at, ended_at)`
- `job_run(run_id uuid PK, cycle_id FK, job_id FK, status running|succeeded|failed|skipped, started_at, ended_at, rows_in, rows_out, bytes, error)`
- `extract_plan(cycle_id, table_name) PK, mode, wm_low, wm_high, page_rows`
- `step_event(event_id bigserial PK, run_id, step_id uuid, step_name, item, event started|succeeded|failed|retried, rows, bytes, detail jsonb, at)`. Append-only, and the only table NiFi writes item-level rows into. Trigger `audit.apply_step_event()` upserts `audit.step(step_id PK, run_id, step_name, item, status, started_at, ended_at, rows, bytes, attempts, error)`.
- `dbt_node_result(run_id, unique_id) PK, cycle_id, resource_type, status, failures, execution_time_s, rows_affected, message`
- `reverse_batch(cycle_id, target, batch_no) PK, run_id, sent, applied, unchanged, conflicts, rejects, replayed, at`
- `reconcile_expected / reconcile_actual(cycle_id, target, zone_id, category_id, check_name) PK, rows, sum_cents, wsum`; `reconcile_result(same PK, passed bool, detail)`
- `alert(alert_id PK, cycle_id, run_id, severity, kind, message, raised_at, resolved_at)`
- Views for Grafana: `v_cycle_stage_duration`, `v_rows_per_stage`, `v_reverse_totals`, `v_sla`.

Functions, called by NiFi with ExecuteSQLRecord on the cycle token (token content is disposable):
- `registry.start_cycle(p_mode text, p_requested_by text, p_rerun_cycle_id bigint) RETURNS (cycle_id, status started|busy|rerun)`. Takes `pg_advisory_xact_lock`, and refuses when another cycle is running. A rerun resets a failed cycle to running with the same id.
- `registry.start_cycle_if_due() RETURNS SETOF (cycle_id)`. Returns a row only if job `cycle` is enabled, has a schedule, no cycle is running and the last start is older than the interval.
- `audit.start_run(p_cycle_id, p_job_id) RETURNS (run_id uuid, enabled bool)`
- `registry.extract_plan(p_cycle_id, p_wm_high timestamptz, p_business_date date, p_mode) RETURNS SETOF plan rows`. Idempotent: if a plan exists for the cycle it returns it, so a rerun extracts the same window. `wm_low = watermark - overlap (default 2 minutes)`. Mode is full when there is no watermark. It also sets `cycle.business_date`.
- `audit.load_plan(p_cycle_id, p_extract_run_id)`: per table, the S3 prefix, expected rows (sum of extract step rows) and file count, only for tables with rows.
- `audit.stage_plan(p_cycle_id, p_job_id)`: the generic one-item plan for transform and the 4-target plan for reconcile.
- `audit.end_run(p_run_id) RETURNS (status, failed_steps, open_steps)`: a run fails if any step failed or is still open. It fills rows/bytes and raises `alert` (kind stage_failed) on failure.
- `registry.commit_watermarks(p_cycle_id)`: called only after load succeeds.
- `audit.reconcile(p_cycle_id) RETURNS (passed bool, failed_checks int)`: compares expected and actual per check and writes `reconcile_result`. Failure raises alert kind reconcile_failed.
- `audit.finish_cycle(p_cycle_id, p_status, p_failed_stage, p_error)`

Which NiFi step writes what:

| Row | Written by |
|---|---|
| audit.cycle insert | `00 trigger` ExecuteSQLRecord start_cycle (HTTP or schedule path) |
| audit.job_run insert | first processor of each stage group, start_run |
| audit.extract_plan, cycle.business_date | `10 extract` ExecuteSQLRecord extract_plan |
| step_event started | each item's PutSQL after the plan split |
| step_event succeeded (rows, bytes, s3 key) | `10 extract` PutSQL after PutS3Object |
| step_event succeeded (written_rows) | `20 load` PutSQL after the InvokeHTTP row check |
| step_event failed / retried | PutSQL on the retries_exceeded / retry paths, every stage |
| registry.watermark | parent gate after `20 load` (commit_watermarks) |
| dbt_node_result | `30 transform` PutDatabaseRecord (UPSERT) from dbt-runner results |
| reverse_batch | `4x reverse` PutSQL (INSERT ON CONFLICT DO UPDATE) after apply_batch |
| reconcile_expected / reconcile_actual | `50 reconcile` PutDatabaseRecord (UPSERT) |
| reconcile_result, alert | parent gate after `50 reconcile` (audit.reconcile) |
| job_run end, alert | parent gate after each stage (end_run) |
| cycle end | `90 finalize` (finish_cycle) |

Roles: `control_owner`, `nifi` (DML on audit, registry via functions), `grafana_ro` (SELECT on
audit and registry views), `exporter`.

---

## 6. NiFi

### 6.1 Image and deploy
`docker/nifi.Dockerfile`: a downloader stage (curl) fetches
`org.postgresql:postgresql:42.7.14`, `nifi-parquet-nar:2.12.0` and
`nifi-hadoop-libraries-nar:2.12.0` from repo.maven.apache.org and checks each against a sha256
pinned in the Dockerfile ARGs, recorded at first download and cross-checked with Maven's
published checksum. The final stage is `FROM apache/nifi:2.12.0@sha256:...`. It copies the jar to
`/opt/nifi/nifi-current/jdbc/` and the NARs to `/opt/nifi/nifi-current/lib/` (loaded at boot,
with no autoload race), with `--chown=nifi:nifi`, and runs as the image's non-root user.
HEALTHCHECK: `bash -c '</dev/tcp/127.0.0.1/8443'` (no curl dependency). API readiness is checked
by deploy. Env: SINGLE_USER_CREDENTIALS_USERNAME/PASSWORD, NIFI_SENSITIVE_PROPS_KEY,
NIFI_WEB_PROXY_HOST=`127.0.0.1:28443,localhost:28443`, and heap. Volumes: conf, state, content,
flowfile, provenance and database repositories.

`retail-pricing nifi deploy --env local|ci` (httpx). TLS verification is off only for the
configured local NiFi URL, because NiFi's certificate is self-signed.
1. Wait until POST `/nifi-api/access/token` returns a JWT (up to 5 minutes).
2. Find the root group. If a group named `retail-pricing` exists: stop it (PUT `/flow/process-groups/{id}` STOPPED), empty its queues (POST `/process-groups/{id}/empty-all-connections-requests`, poll), disable its services, then DELETE it.
3. Create or update the parameter context `retail-pricing-<env>` from `nifi/params/<env>.json`. Sensitive entries name an env var; deploy fails if one is unset. Updates use POST `/parameter-contexts/{id}/update-requests` and poll.
4. Upload `nifi/flows/retail-pricing.json` with POST `/process-groups/{root}/process-groups/upload` (multipart: file, groupName, positionX, positionY, clientId). Bind the context with PUT `/process-groups/{id}`.
5. Enable the controller services (PUT `/flow/process-groups/{id}/controller-services` ENABLED), poll until all are ENABLED, and print the validation errors of any INVALID one.
6. Fail if any component in the tree is invalid, listing each component's validationErrors.
7. Start the group (PUT `/flow/process-groups/{id}` RUNNING) and wait until the trigger port accepts TCP.
8. Write the process group ids into `registry.job.nifi_process_group_id`.

`retail-pricing nifi build-flows` writes the definition deterministically: uuid5 ids from the
component path, sorted keys, fixed positions on a grid. `tests/unit/test_flows.py` regenerates it
and diffs it against the committed file, and checks every processor property key, allowable value
and auto-terminated relationship against `nifi/definitions/*.json`. Those definitions are pulled
from GET `/flow/processor-definition/{group}/{artifact}/{version}/{type}` by
`retail-pricing nifi definitions pull`. This catches typos offline.

### 6.2 First spike (start of PR 4; can run locally any time after PR 1)
In a running custom image, build by hand a mini group with every processor type the builder uses,
then export it (GET `/process-groups/{id}/download`) to `nifi/spike/reference-flow.json`. Verify
and record each result in DECISIONS:
- (a) The definition JSON shape: top-level keys, `flowContents`, bundle coordinates, property key names, scheduling fields, and how parameter contexts and sensitive values are represented. The builder is pinned to it.
- (b) Round trip: build, upload, download, and compare normalized output.
- (c) A group with FlowFile Concurrency `Single FlowFile Per Node` and Outbound Policy `Batch Output` holds the SplitJson `original` at the output port until every split is auto-terminated.
- (d) ParquetRecordSetWriter output with Use Avro Logical Types read by ClickHouse `s3()`: Decimal, DateTime64(3), Date32 and Nullable types.
- (e) GenerateTableFetch accepts EL from FlowFile attributes in Table Name, Columns to Return, Additional WHERE Clause and Column for Value Partitioning.
- (f) PutSQL `SQL Statement` with `sql.args.N.*` attributes.
- (g) InvokeHTTP response headers as attributes (`X-ClickHouse-Summary`).
- (h) JsonTreeReader on ClickHouse JSONEachRow.
- (i) ExecuteSQLRecord with Max Rows Per Flow File, Set Auto Commit false and Fetch Size streams from Postgres.

### 6.3 Parameter context `retail-pricing-<env>`
`oltp_jdbc_url`, `oltp_user`, `oltp_password` (S), `control_jdbc_url`, `control_user`,
`control_password` (S), `jdbc_driver_dir`, `s3_endpoint=http://s3:9000`, `s3_bucket`,
`s3_access_key` (S), `s3_secret_key` (S), `ch_url=http://clickhouse:8123`, `ch_user`,
`ch_password` (S), `dbt_runner_url=http://dbt-runner:8580`, `trigger_port=8081`,
`page_rows=250000`, `fetch_size=10000`, `extract_concurrency=4`, `retry_max=3`,
`retry_penalty=30 sec`, `dbt_poll_penalty=10 sec`, `dbt_poll_max=720`, `ch_read_timeout=30 min`.
`ci.json` lowers `page_rows` to 5000 so paging is exercised at SF0.01.

### 6.4 Controller services (in group `retail-pricing`)
`oltp-pool` and `control-pool` (DBCPConnectionPool: PostgreSQL driver from `jdbc_driver_dir`, max
total 8/4, validation query `select 1`); `s3-credentials` (AWSCredentialsProviderControllerService);
`json-reader` (JsonTreeReader); `json-writer` (JsonRecordSetWriter, output as array);
`parquet-writer` (ParquetRecordSetWriter: SNAPPY, row group 32 MB, schema inherited from the
record); `http-context` (StandardHttpContextMap).
### 6.5 Process groups
Each stage group follows one template, generated by a builder function:

```
in -> ExecuteSQLRecord[control] start_run -> EvaluateJsonPath(run_id) -> <plan query> -> SplitJson($.*)
   SplitJson.original -> out            (held by Batch Output until the group is empty)
   SplitJson.split    -> EvaluateJsonPath(item attrs) -> PutSQL step_event started -> <item work>
        -> PutSQL step_event succeeded -> auto-terminate
   any failure -> RetryFlowFile(max #{retry_max}, penalty #{retry_penalty}) -> retry: back to the failed processor
        retries_exceeded -> PutSQL step_event failed -> auto-terminate
```

Group settings: FlowFile Concurrency `Single FlowFile Per Node`, Outbound Policy `Batch Output`.
This is the barrier and fan-in: the token leaves only when every item has succeeded or been
recorded as failed. Cycles serialize for free, and the audit DB holds item status, so nothing
in NiFi has to count fragments. At the parent level, between groups, a gate runs
`ExecuteSQLRecord[control] audit.end_run(run_id)`, then EvaluateJsonPath and RouteOnAttribute.
`succeeded` or `skipped` continue; `failed` goes to UpdateAttribute (`cycle.status=failed`,
`failed_stage`) and on to `90 finalize`. The gate after load also calls `commit_watermarks`. The
gate after reconcile calls `audit.reconcile`.

Fallback if spike (c) fails: Wait/Notify. The token goes to Wait (signal `${run_id}`, target
count `${fragment.count}`), each item ends in Notify, and a MapCacheServer plus
MapCacheClientService are added.

Group `00 trigger`, not a barrier:
- HTTP path: HandleHttpRequest (port `#{trigger_port}`, allowed path `/cycles`, POST only, `http-context`) -> EvaluateJsonPath (`mode`, `rerun_cycle_id`) -> RouteOnAttribute (mode matches `^(full|incremental)$`, rerun id `^\d*$`, else ReplaceText 400 -> HandleHttpResponse 400) -> ExecuteSQLRecord[control] `select * from registry.start_cycle(...)` -> EvaluateJsonPath(`cycle_id`, `status`) -> ReplaceText JSON body -> HandleHttpResponse (202 started/rerun, 409 busy) -> RouteOnAttribute started -> out.
- Schedule path: GenerateFlowFile (every 60 s, `mode=incremental`) -> ExecuteSQLRecord `registry.start_cycle_if_due()` -> RouteOnAttribute `executesql.row.count > 0` -> EvaluateJsonPath -> out. The registry interval, not a NiFi cron, decides.

Group `10 extract`:
- Before the plan: ExecuteSQLRecord[oltp] `select now() as wm_high, business_date from ops.business_clock` -> EvaluateJsonPath. The plan is ExecuteSQLRecord[control] `registry.extract_plan(...)`.
- Item work: RouteOnAttribute on `mode`.
  - full: GenerateTableFetch (Table Name `${table_name}`, Columns `${select_list}`, Additional WHERE `updated_at <= '${wm_high}'`, Column for Value Partitioning `${pk_column}`, Partition Size `${page_rows}`, no max-value columns, so no processor state) -> UpdateAttribute (`page=${fragment.index}`).
  - incremental: ReplaceText (`SELECT ${select_list} FROM ${table_name} WHERE updated_at > '${wm_low}' AND updated_at <= '${wm_high}'`), with `page=0`.
- Then ExecuteSQLRecord[oltp] (SQL from content, Record Writer `parquet-writer`, Use Avro Logical Types true, Max Rows Per Flow File `#{page_rows}`, Fetch Size `#{fetch_size}`, Set Auto Commit false, Concurrent Tasks `#{extract_concurrency}`) -> RouteOnAttribute (`executesql.row.count == 0` goes to the zero-row success event) -> UpdateAttribute (key `raw/${table_name}/cycle=${cycle_id}/run=${run_id}/part-${page}-${fragment.index}.parquet`) -> PutS3Object (bucket, key, Endpoint Override `#{s3_endpoint}`, path-style access, region us-east-1, `s3-credentials`).
- Steps are recorded per page. Item = `<table>#<page>-<fragment>`.

Group `20 load`:
- Plan: ExecuteSQLRecord[control] `audit.load_plan(cycle_id, extract_run_id)`.
- Item work: ReplaceText body `INSERT INTO raw.${table_name} SELECT *, ${cycle_id}, now64(3) FROM s3(raw_s3, url='http://s3:9000/#{s3_bucket}/${prefix}*.parquet', format='Parquet')` -> InvokeHTTP POST `#{ch_url}/?wait_end_of_query=1&max_insert_threads=2` (basic auth `#{ch_user}`/`#{ch_password}`, read timeout `#{ch_read_timeout}`) -> UpdateAttribute (`written_rows=${X-ClickHouse-Summary:jsonPath('$.written_rows')}`) -> RouteOnAttribute (`written_rows` equals `expected_rows`, else failed with detail).
- InvokeHTTP Retry and Failure go to RetryFlowFile; No Retry goes straight to failed.

Group `30 transform`:
- Plan: one item.
- Item work: ReplaceText JSON `{cycle_id, run_id, as_of, command:"build"}` -> InvokeHTTP POST `#{dbt_runner_url}/runs`. The runner is idempotent per cycle: it returns the existing job if one is running or succeeded. Then EvaluateJsonPath(`job_id`).
- Poll: InvokeHTTP GET `/runs/${job_id}` -> EvaluateJsonPath(`job_status`) -> RouteOnAttribute. `running` goes to RetryFlowFile (attribute `dbt.poll`, max `#{dbt_poll_max}`, penalty `#{dbt_poll_penalty}`) and back to the GET. `succeeded` or `failed` goes to InvokeHTTP GET `/runs/${job_id}/results` -> PutDatabaseRecord[control] UPSERT `audit.dbt_node_result` -> RouteOnAttribute(job_status) -> step succeeded or failed (message).
- retries_exceeded is a timeout and records the step as failed.

Groups `40 reverse zone_price`, `41 reverse store_override`, `42 reverse promo_price`, `43 reverse shelf_label` share one template parameterized by target. Chaining the groups enforces the order.
- Plan: InvokeHTTP POST ClickHouse `SELECT batch_no, count() AS rows FROM reverse.rev_<target> WHERE cycle_id = ${cycle_id} GROUP BY batch_no ORDER BY batch_no FORMAT JSON`, then SplitJson `$.data.*`. Zero batches means zero items and the run succeeds.
- Item work: ReplaceText `SELECT <stage columns> FROM reverse.rev_<target> WHERE cycle_id=… AND batch_no=… FORMAT JSONEachRow SETTINGS output_format_json_quote_64bit_integers=0` -> InvokeHTTP POST ClickHouse -> PutDatabaseRecord[oltp] (`json-reader`, INSERT_IGNORE into `pricing_apply.<target>_stage`, Max Batch Size 1000, Rollback On Failure true) -> ExecuteSQLRecord[oltp] `select * from pricing_apply.apply_batch(${cycle_id}, '<target>', ${batch_no})` -> EvaluateJsonPath(counts) -> PutSQL[control] upsert `audit.reverse_batch` -> step succeeded.

Group `50 reconcile`:
- Plan: the 4 targets.
- Item work: InvokeHTTP POST ClickHouse `SELECT * FROM reverse.rev_reconcile_expected WHERE cycle_id=… AND target='…' FORMAT JSONEachRow` -> PutDatabaseRecord[control] UPSERT `audit.reconcile_expected` -> ExecuteSQLRecord[oltp] `pricing_apply.reconcile_actual(...)` with `json-writer` -> PutDatabaseRecord[control] UPSERT `audit.reconcile_actual`.
- The parent gate then runs `audit.reconcile`.

Group `90 finalize`: ExecuteSQLRecord[control] `audit.finish_cycle(...)`, then terminate.

Retry and idempotency summary: S3 keys include the extract run id, so a rerun never reads stale
files. Raw tables are ReplacingMergeTree, so a re-load is harmless. dbt cycle models delete their
own cycle first. Staging inserts are INSERT_IGNORE, and apply_batch replays. A crashed cycle is
rerun with the same id: `retail-pricing cycle trigger --rerun <id>`.

---

## 7. ClickHouse

Image env: `CLICKHOUSE_USER=ch_admin`, password, `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` (not
named `default`). Config, all from_env:
- `config.d/memory.xml`
- `config.d/prometheus.xml` (port 9363, metrics, events, asynchronous metrics)
- `config.d/named-collections.xml`: `raw_s3` with url `http://s3:9000/<bucket>/`, access_key_id and secret_access_key from env, overridable url
- `users.d/*.xml`:
  - `nifi`: profile etl; grants INSERT and SELECT on raw.*, SELECT on reverse.*, S3, NAMED COLLECTION raw_s3
  - `dbt`: profile etl; SELECT on raw, ALL on stg, int, pricing, reverse, marts
  - `grafana`: readonly=2; SELECT on pricing, reverse, marts

Fallback: create users and grants by SQL in `ch init` if XML grants with from_env misbehave.

Databases: `raw` (created by `ch init`), `stg`, `int`, `pricing`, `reverse`, `marts` (created by
dbt through a `generate_schema_name` override that uses the custom name verbatim).

Raw tables: `raw.<schema>__<table>`. Columns are the OLTP columns in DDL order (bigint to Int64,
numeric(p,s) to Decimal(p,s), text to String or LowCardinality(String) for codes and statuses,
timestamptz to DateTime64(3,'UTC'), date to Date32, nullable to Nullable), followed by
`_cycle_id UInt64, _loaded_at DateTime64(3,'UTC')`; `version` is `UInt64` (a safe ReplacingMergeTree
version type). Engine `ReplacingMergeTree(version) ORDER BY
<pk>`, with no partitioning (the partition key must be immutable per row). Staging reads with
`FINAL`. Overlap duplicates and re-loads collapse to the highest version. Contract test: the OLTP
`information_schema` column order equals the `system.columns` order minus the meta columns (runs
in e2e; the dbt job proves the Parquet shape against ClickHouse).

`retail-pricing ch init` applies `sql/clickhouse/*.sql` (idempotent). `ch load-raw --sf N --cycle
C` (dev and CI only) runs `INSERT INTO raw.t SELECT *, C, now64(3) FROM
file('oltp/sf=N/<t>.parquet', Parquet)`, reading the bind mount `./data:/var/lib/clickhouse/user_files:ro`.
That is the same statement shape NiFi sends with `s3()`.

---
## 8. dbt project (`dbt/`)

dbt-core 1.10.x plus dbt-clickhouse 1.10.3 (the adapter documents support up to 1.10). There are
no packages, so no network is needed at build time; the few generic tests live in `macros/`. The
profile `retail_pricing` reads host, port 8123, http driver, user `dbt` and password from env.
Vars: `cycle_id` (int, required; 0 is rejected unless `--vars ci: true`), `as_of` (date,
required), `batch_size` (1000), `label_batch_size` (5000), `label_lead_days` (2),
`history_days` (400), `elasticity_min_obs` (200).

Cycle-scoped models are `incremental` with `incremental_strategy='append'`, MergeTree
`ORDER BY (cycle_id, <keys>)`, and `pre_hook: {% if is_incremental() %}DELETE FROM {{ this }}
WHERE cycle_id = {{ var('cycle_id') }}{% endif %}` (lightweight delete). A rerun of a cycle
replaces its rows.

### 8.1 Models
Staging (`stg`, views): one per raw table, `FINAL`, renamed and typed. `stg_apply_outcome` unions
the 4 stage tables' status. `stg_apply_conflict`.

Intermediate (`int`, tables, current cycle only):
1. `int_sales_daily`: product x zone x business_date over `history_days` up to as_of. Units, net revenue, average net unit price, cost; returns subtracted. Store channel via store.zone, web via the WEB zone.
2. `int_demand`: product x zone. units_28d, units_90d, units_365d, rate_28, rate_90, velocity_ratio = rate_28 / rate_90, seasonality_index = units in [as_of-365, as_of-337) divided by (units_365d * 28/365).
3. `int_weekly_price_qty`: product x zone x ISO week over 52 weeks. Average price and units.
4. `int_elasticity`: category. Within product x zone, demean ln(price) and ln(units+1); slope = covarPop / varPop, clamped to [-3.0, -0.3]; fallback -1.2 when n_obs < `elasticity_min_obs` or varPop < 1e-6; `is_fallback` flag.
5. `int_stock_cover`: product x zone. on_hand (stores of the zone; the WEB zone uses warehouses), days_of_cover = on_hand / greatest(rate_28, 0.01), aging_days = as_of - max(last_received_date), flags overstock / aged / low_cover from category_rule.
6. `int_cost_current`: product. Latest supplier_cost with valid_from <= as_of (argMax); cost_change_90d_pct.
7. `int_competitor_index`: product x zone. Median competitor price over the last 14 days (28-day fallback), n_competitors, index = current / median.
8. `int_current_state`: product x zone. Current regular price and version (the extract snapshot), active or upcoming promotion within `label_lead_days`, its depth, store overrides count, category rule, KVI tier, ladder membership.
9. `int_price_candidate`: product x zone. factor = 1 + s * (velocity +-2% when the ratio is > 1.2 or < 0.8; cover -3% when overstock or aged, +2% when low cover; seasonality +-1% when the index is > 1.2 or < 0.8), with s = least(1, 1/abs(elasticity)). cost_floor = cost / (1 - min_margin). KVI: candidate = competitor median * index_target. Non-KVI with a competitor: 0.7 * current * factor + 0.3 * median * 1.02. Otherwise: current * factor. A component reason array is kept.
10. `int_price_bounded`: product x zone. Holds first: promo with policy exclude (HOLD_PROMO), no cost (HOLD_NO_COST). lo = greatest(cost_floor, current * (1 - cap)), hi = current * (1 + cap). If cost_floor > hi, then lo = hi and the reason is FLOOR_LIMITED_BY_CAP. Ending: the nearest grid price in [lo, hi] (bands from `price_ending_band`); if none, HOLD_INFEASIBLE.
11. `int_price_laddered`: product x zone. Per ladder group and zone, ordered by rank, an `arrayFold` pass on the ending grid. PACK: unit price (price / pack_units) non-increasing. TIER: price non-decreasing by tier with a gap of at least one grid step. Each member is pushed to the nearest feasible grid price inside its own [lo, hi]. If any member is infeasible, the whole group holds (HOLD_LADDER) at its current prices. Changed members get LADDER_ADJUST.

Pricing outputs (`pricing`, incremental by cycle):
- `pricing_zone_price` (cycle x product x zone): old_price, new_price, changed, primary_reason, reasons Array, lo, hi, expected_version, elasticity.
- `pricing_store_override` (cycle x store x product, stores with an open override): new = new_zone * (old_override / old_zone), ending-rounded, capped by the same cap.
- `pricing_promo_price` (cycle x promo x product x zone; keep_depth categories whose regular price changed): new_promo = new_regular * (1 - depth), rounded down to the grid, PROMO_DEPTH_KEEP.
- `pricing_price_change_log` (cycle x target x key, changed rows only): old, new, change_pct, primary_reason, reasons, as_of.
- `pricing_margin_projection` (cycle x product x zone): units_28d_new = units_28d * (new/old)^elasticity; revenue and gross margin old vs new; deltas.
- `pricing_shelf_label` (cycle x store x product, effective shelf price changed): price (override if present, else zone), promo_price, due_date = as_of + label_lead_days, source_target.
- `pricing_price_history` (table, SCD2 per product x zone): from `stg_price_history` (OLTP trigger history: valid_from, valid_to, is_current, source, cycle_id). Not a dbt snapshot (DECISIONS).

Reason codes (accepted_values): COST_PASS_THROUGH, MARGIN_FLOOR, FLOOR_LIMITED_BY_CAP,
COMPETITOR_ALIGN_KVI, COMPETITOR_BLEND, VELOCITY_UP, VELOCITY_DOWN, LOW_COVER_UPLIFT,
OVERSTOCK_MARKDOWN, AGING_MARKDOWN, SEASONAL, CAPPED, LADDER_ADJUST, ENDING_ROUND,
PROMO_DEPTH_KEEP, HOLD_PROMO, HOLD_NO_COST, HOLD_INFEASIBLE, HOLD_LADDER. The primary reason is
the binding constraint if any (precedence MARGIN_FLOOR > FLOOR_LIMITED_BY_CAP > CAPPED >
LADDER_ADJUST), otherwise the largest component.

Rule order, binding: holds, then candidate, then bounds (floor, then cap), then ending, then
ladders (pack, then tier), then store overrides and promo recalculation (derived from the final
zone price), then labels.

Reverse diffs (`reverse`, incremental by cycle):
- `rev_zone_price`, `rev_store_override`, `rev_promo_price`, `rev_shelf_label`. Only rows where `cityHash64(<target columns as written to OLTP>)` differs from the hash of the snapshot row from `stg_*` (labels: every row of `pricing_shelf_label`). They carry expected_version (snapshot version, or 0 for new rows), row_hash, reason_code, and `batch_no = intDiv(row_number() OVER (ORDER BY keys) - 1, batch_size)`.
- `rev_reconcile_expected` (cycle x target x zone x category): the transit check from the rev rows, using the same formula as `reconcile_actual`.

Marts (`marts`, tables):
- `mart_price_change_impact` (cycle x zone x category x primary_reason): n_changed, n_up, n_down, avg_change_pct, projected revenue and margin delta.
- `mart_proposed_vs_applied` (cycle x target x key): proposed, outcome from `stg_apply_outcome` (applied, unchanged, conflict, rejected, not_sent), value now in the OLTP, overwritten_later flag. Complete for cycle N once cycle N+1 has extracted.
- `mart_conflicts` (cycle x target x key): expected vs actual version, proposed vs human price, editor, detected_at.

### 8.2 Tests
Generic: `unique_combination` (macro) on every grain, not_null on keys, accepted_values on reasons
and statuses, relationships for product and zone.

Singular guardrails (`tests/guardrails/`, severity error, so dbt build fails and the cycle stops
before reverse):
- g01 no change above the cap (zone, override, promo)
- g02 margin >= min unless FLOOR_LIMITED_BY_CAP with new = hi
- g03 pack ladder monotone (non-held groups)
- g04 tier ladder monotone
- g05 valid endings on every changed price
- g06 promo depth kept within one grid step
- g07 labels only for changed effective prices
- g08 reverse rows only where the hash changed
- g09 batches contiguous and bounded
- g10 hold rate <= 20% (severity warn)

Unit tests (`unit_tests:` in yml), verified first in PR 3:
- int_price_bounded: floor beats candidate, cap clamps, floor limited by cap, no feasible ending holds
- int_price_laddered: pack fix, tier fix, infeasible group holds
- int_elasticity: clamp and fallback
- pricing_promo_price: depth
- rev_zone_price: unchanged rows excluded, batch numbering
- primary-reason precedence

Fallback if unit tests do not work on ClickHouse: `tests/fixtures/*.csv` as seeds, models
switched by `var('fixture')`, and singular tests asserting the expected rows. The decision is
recorded in DECISIONS.

### 8.3 dbt runner
A stdlib `ThreadingHTTPServer`, one job at a time:
- `POST /runs {cycle_id, run_id, as_of, select?}`: returns 202 `{job_id}`. Same cycle running or succeeded returns that job; another cycle running returns 409.
- `GET /runs/{id}`: status, timings, counts.
- `GET /runs/{id}/results`: a JSON array shaped like `audit.dbt_node_result`, from `run_results.json`.
- `GET /healthz`, `GET /metrics`.

It runs `dbt build --vars ... --target-path target/cycle=<id> --log-format json` as a
subprocess. Job state is persisted to a volume; jobs that were running at startup are marked
failed.

### 8.4 CI without NiFi
`data generate --sf 0.01`, then `data build`, `docker compose up -d --wait clickhouse` (real
config; service containers cannot mount repo files), `ch init`, `ch load-raw --cycle 1`,
`dbt build --vars '{cycle_id: 1, as_of: 2025-12-04}'`, `ch load-raw --cycle 2`,
`dbt build --vars '{cycle_id: 2, ...}'` (exercises the incremental and pre-hook path).
Unit tests run inside `dbt build`.

---

## 9. Workload generator (`retail-pricing workload run`, profile `workload`)

It uses a psycopg pool, threads, `random.Random(seed + worker)` and `SET app.actor = 'store-ops'`.
Every transaction is short and touches at most one owned pricing row. Default rates, configurable
by env:
- sales 20/s, 1..8 lines (mean ~4), current prices, with a store_stock decrement and a SALE movement
- returns 2% of recent lines
- receipts when on_hand < cover_min * rate (RECEIPT, last_received_date)
- a new supplier_cost row 1/min
- manual zone_price edits 0.5/s (50% on KVI products, +-1..8%, snapped to the ending grid)
- store override edits 0.1/s
- label printing (queued to printed) 1 batch/s
- business clock +1 day every `WORKLOAD_DAY_SECONDS` (default 600)

Metrics: counters per transaction type, errors, latency histogram, current business date.

Conflicts are real: an edit that lands between a cycle's extract and its apply changes the
version, so apply_batch records a conflict. At SF1 a cycle takes a few minutes, which gives est.
50..150 conflicts. For a deterministic demo, `retail-pricing workload edit --count 50 --kvi --seed
7` is run while the cycle is in transform, after the snapshot. It edits 50 KVI zone prices.

---
## 10. Monitoring

Prometheus scrape jobs:
- `nifi`: https://nifi:8443/nifi-api/flow/metrics/prometheus, `authorization.credentials_file: /var/run/nifi-token/token`, `tls_config.insecure_skip_verify: true`, 30 s
- `clickhouse` (:9363)
- `postgres-oltp` and `postgres-control` (exporters :9187)
- `dbt-runner` (:8580)
- `workload` (:9108)
- `prometheus`

`nifi-token` (`retail-pricing nifi token-refresh --interval 6h`) writes the JWT atomically to a
named volume mounted read-only in Prometheus. Prometheus re-reads the file on every scrape, and
the token lasts about 8 h.

Grafana provisioning:
- Datasources: `prometheus`, `control` (postgres, user grafana_ro), `clickhouse` (grafana-clickhouse-datasource, http, user grafana).
- Dashboards: a file provider, with anonymous Viewer enabled.

1. Pipeline overview (control): cycles table (id, mode, business date, status, duration), stage duration per cycle (stacked bar), rows and bytes per stage and table (extract vs load), reverse outcomes per target (sent, applied, unchanged, conflicts, rejects), conflict rate trend, DQ results (pass/warn/error), reconciliation status, run duration vs SLA, open alerts, watermark lag per table.
2. Pricing outcomes (ClickHouse): changes per cycle by reason, average change % by category, up/down histogram, projected margin delta by zone, proposed vs applied vs conflicts per cycle, top conflicts, labels per store, held items by hold reason.
3. Platform (Prometheus): NiFi queued FlowFiles and bytes, active threads, bytes in/out per group, JVM heap; ClickHouse queries/s, memory tracking vs limit, parts, merges, S3 read bytes, external sort/group-by bytes; Postgres TPS, connections, locks, deadlocks, tuples written, cache hit, dead tuples; workload tx/s and errors; dbt job duration.

Alert rules (provisioned; visible in Grafana, with no external contact point; DECISIONS):

| Rule | Condition |
|---|---|
| CycleFailed | a cycle failed in the last 15 min |
| SlaBreach | run duration > its job SLA (running or finished) |
| ReconcileFailed | any failed check in the last cycle |
| DqErrors | dbt test errors in the last cycle |
| ConflictRateHigh | conflicts/sent > 5% in the last cycle (warning) |
| WatermarkLag | now - min(high_value) > 2 x schedule interval |
| TargetDown | `up == 0` for nifi, clickhouse, postgres-* for 2 min |
| NiFiBackpressure | queued bytes > 1 GB for 5 min |
| ClickHouseMemoryHigh | MemoryTracking > 90% of the server limit for 5 min |

---

## 11. Make targets and runs

The Makefile follows the sibling repos: bash with `-eu -o pipefail`, `##` help and
`MSYS_NO_PATHCONV=1`. Recipes are one-liners or call `scripts/*.sh`, so Git Bash users can paste
them.

Targets: `help install lint fmt test test-fast test-sql test-clickhouse test-integration data
load up up-monitoring down ps logs deploy flows cycle workload workload-stop demo heavy report
check d2-install diagrams diagrams-check diagrams-watch`. `SF ?= 1`. `data` = generate + build.
`load` = `oltp load`. `up` = `docker compose up -d --build --wait`. `down` = `down -v
--remove-orphans` across all profiles.

`make demo` (`scripts/demo.sh SF=${SF:-1}`). Each step is timed by the script into
`runs/<utc-ts>-sf<SF>/steps.json`.
1. Copy `.env.example` to `.env` if missing (warning: local-only secrets).
2. `retail-pricing data generate --sf $SF` (skipped when the manifest matches) and `data build`.
3. `docker compose --profile core --profile monitoring up -d --build --wait`.
4. `retail-pricing oltp load --sf $SF` (per-table COPY, constraints, indexes).
5. `retail-pricing nifi deploy --env local`.
6. Cycle 1: `retail-pricing cycle trigger --mode full --wait` (initial full load, first proposals and apply).
7. `docker compose --profile workload up -d workload`.
8. Cycle 2: `cycle trigger --mode incremental` without waiting, then `cycle wait-stage transform`, then `workload edit --count 50 --kvi --seed 7`, then `cycle wait`.
9. Cycle 3: incremental with `--wait`. Closes the loop for cycle 2.
10. Stop the workload. `retail-pricing check --cycles 1-3`: every cycle succeeded; reconcile passed; for every batch sent = applied + unchanged + conflicts + rejects; no guardrail errors; conflicts in OLTP = audit = `mart_conflicts`; `mart_proposed_vs_applied` for cycles 1-2 matches the OLTP; cycle 2 has > 0 conflicts.
11. `retail-pricing report --run-dir runs/...` writes `timings.json` and `timings.md` (host steps plus, from the audit DB, stage durations per cycle, extract and load per table with rows and MB/s, top 10 dbt models by time, reverse per target with rows/s, reconcile) and prints the Grafana URL.

`make heavy SF=10` (`scripts/heavy.sh`): the same steps with SF=10, the workload running through
all cycles, and `scripts/sample-stats.sh` sampling `docker stats` every 5 s into
`runs/.../stats.jsonl` (peak memory per container in the report). It does not inject edits. The
owner copies the results into `docs/RESULTS.md` and the README table with machine specs and the
TPC notice. SF100 is described in RESULTS (est. disk and time from the SF1 to SF10 scaling) and
never run by default.

`cycle trigger` POSTs to `http://127.0.0.1:${NIFI_TRIGGER_PORT}/cycles`, then polls
`audit.cycle`/`job_run` every 5 s and prints stage progress. Exit 0 when the cycle succeeded,
1 when it failed (failed steps listed), 2 on timeout.

---

## 12. CI and Dependabot

`ci.yml`: push to main, pull_request, workflow_dispatch, weekly schedule. Actions SHA-pinned,
`permissions: contents: read`, `persist-credentials: false`, `UV_LOCKED=1`, concurrency per
event and ref.

| Job name (required check) | Runs | Est. time | From PR |
|---|---|---|---|
| `lint (ruff + mypy)` | `make lint` | 1 min | 1 |
| `test-fast (unit)` | `make test-fast` (includes flows-current and dashboard JSON tests once they exist) | 2 min | 1 |
| `diagrams (D2 render check)` | `make d2-install diagrams-check` | 1 min | 1 |
| `sql (Postgres functions)` | `docker compose up -d --wait oltp control-db`, `data` SF0.01, `oltp load`, `control init`, `pytest -m sql` | 4 min | 2 |
| `dbt (ClickHouse, SF0.01)` | section 8.4, artifacts run_results.json on failure | 6 min | 3 |
| `e2e (compose loop, SF0.01)` | `make demo SF=0.01` with the `ci` params, then `pytest -m integration`; logs on failure; `make down` always | 15-25 min, timeout 40 | 4 (extract and load), 5 (full loop), 6 (monitoring) |

The DuckDB extension directory is cached by key `duckdb-2.0.0-tpcds`. `security.yml` stays as
is. After each PR merges, add its new job to branch protection (job names never change once
required).

Dependabot (portfolio policy): monthly schedule, a 7-day cooldown, one minor-and-patch group
per ecosystem, semver-major updates ignored (they move by hand), for `github-actions` (/), `uv`
(/), `docker` (/docker) and `docker-compose` (/), each ecosystem added when its files land.
`dependabot-auto-merge.yml` merges green minor and patch PRs through GitHub auto-merge once main
is protected. Manual pins with a comment in the file: NiFi NAR and JDBC versions and sha256
(Dockerfile ARGs), the Grafana plugin version, D2.

---

## 13. PR plan (sequential, each merged before the next starts)

PR 1 - Foundation and data. pyproject, uv.lock, src layout, Typer CLI skeleton, settings,
JSON logging, `data generate`/`data build` with manifests, all DuckDB mapping and synthesis SQL,
`synth.py` mirror, Makefile (install, lint, fmt, test*, data, diagrams*), CI lint/test-fast/
diagrams, dependabot, README skeleton, CLAUDE.md, CHANGELOG, DECISIONS, this PLAN,
`architecture.d2`. Done when `make data SF=0.01` and `SF=1` reproduce identical content hashes on
two runs, SF0.01/1/10 TPC-DS counts match, OLTP-shape counts are recorded in section 3.4, and
hypothesis tests cover `u()` parity, zone assignment, the ending grid, ladder repair and the date
shift. Verify: `make lint test-fast`, `make data SF=0.01` twice and diff the manifests.

PR 2 - OLTP, control DB, workload. All OLTP and control SQL, `oltp load`, `control init`,
compose with oltp, control-db and init (core) and workload, app Dockerfile, `.env.example`,
`workload run`/`edit`, `oltp-model.d2`, CI `sql`. Done when the `pytest -m sql` cases pass:
apply/unchanged/conflict/reject, replay returns the stored counts, label dependency, deadlock-free
with concurrent edits, reconcile_actual checksums, start_cycle busy, extract_plan idempotent,
end_run with open steps fails; SF1 loads with timings logged; the workload runs 10 min with no
errors. Verify: `make up load SF=1 workload`, `make test-sql`.

PR 3 - ClickHouse, dbt, dbt-runner. ClickHouse config and users, `ch init`, `ch load-raw`, the
complete dbt project with tests, dbt-runner service, compose adds clickhouse, s3 and dbt-runner,
`dbt-dag.d2`, CI `dbt`. The first task settles dbt unit tests on ClickHouse (or the fallback).
Done when the dbt job is green for cycles 1 and 2 at SF0.01, an SF1 build runs locally within the
memory limits with timings noted, and runner endpoint tests (unit, subprocess mocked) pass.
Verify: `make test-clickhouse`, CI dbt.

PR 4 - NiFi image, spike, builder, deploy, extract and load. Spike results in DECISIONS and
`nifi/spike/`, `nifi/definitions/`, the builder (template, trigger, extract, load, gates,
finalize), committed flow, deploy, `cycle trigger/wait`, CI `e2e` asserting that the cycles reach
load with matching extract and load counts and committed watermarks, plus the schema contract
test. Done when `make up deploy` and a full and an incremental cycle (workload on) are green
locally at SF1 and e2e is green at SF0.01. Verify: `make deploy cycle`, CI e2e.

PR 5 - Transform, reverse, reconcile, demo. Groups 30-50, the dbt-runner integration, `check`,
`report`, `scripts/demo.sh`, `cycle.d2`, e2e switched to `make demo SF=0.01` with all check
assertions, and a rerun test (stop NiFi mid-reverse, rerun the cycle, identical outcome). Done
when `make demo` passes at SF1 locally and at SF0.01 in CI. Verify: `make demo`, CI e2e.

PR 6 - Monitoring, heavy run, docs. Prometheus, nifi-token, exporters, Grafana datasources,
dashboards and alerts, `monitoring.d2`, `make heavy`, `sample-stats.sh`, an SF10 run with
`docs/RESULTS.md` and the README results table (TPC notice), RUNBOOK, CHANGELOG 0.1.0. e2e adds
the monitoring profile and asserts Prometheus targets up, Grafana datasources healthy, dashboards
and alert rules loaded. Done when the SF10 run completes within the limits (or the report states
where it did not). Verify: `make heavy SF=10`, CI e2e.

The NiFi spike (6.2) can run locally during PRs 2-3. Dashboards JSON drafts can start once PR 3's
marts exist. Neither is committed outside its PR.

---

## 14. Risks and fallbacks

| Risk | Fallback |
|---|---|
| Flow-definition JSON schema unverified | Spike 6.2 pins it from a real export, and builder tests validate against pulled processor definitions. If upload rejects builder output, generate from the exported reference by templating, or build through the REST component APIs |
| Batch Output barrier misbehaves | Wait/Notify with fragment counts plus a map cache (6.5) |
| Parquet NARs fail (classpath, codec) | AvroRecordSetWriter (embedded schema, logical types) and `format='Avro'` in the load SQL; the S3 layout is unchanged |
| dbt unit tests unsupported on ClickHouse | Seed fixtures plus `var('fixture')` plus singular tests (8.2) |
| ClickHouse OOM at SF10 | max_threads 2, lower external thresholds, grace_hash joins, narrow `history_days`, split int_sales_daily by month (incremental). Report the remaining peak |
| NiFi heap at SF10 | Lower `page_rows` to 100k, concurrency to 2, row group to 16 MB |
| OLTP load time at SF10 | Indexes after COPY, `synchronous_commit=off` only during load, measured and reported |
| CI e2e time (NiFi image ~1.1 GB, startup) | SF0.01 with ci params, timeout 40. If it is over 30 min regularly, run e2e on main, schedule and a `run-e2e` label, and the owner updates the required checks |
| GenerateTableFetch EL limits | ReplaceText-built paged SQL from an OLTP `ops.page_ranges(table, page_rows)` function |
| ClickHouse named-collection override or XML from_env grants | Full url in the collection per prefix, or SQL users by `ch init` |
| dbt-core / adapter drift | Pin dbt-core 1.10.x; Dependabot PRs for dbt need the dbt job green |
| RustFS healthcheck or API gaps | Probe with an HTTP GET on the API port from the app image; record the RustFS-specific behavior |

---

## 15. Low-impact decisions for docs/DECISIONS.md

- Host CLI for data, load, deploy and cycles: avoids slow Windows bind mounts and keeps DuckDB out of the VM budget.
- `COMPOSE_PROFILES=core` in `.env`: every service has a profile and `docker compose up` still works.
- Separate control Postgres: an OLTP under load must not stall the audit trail; it costs 256 MiB.
- Catalog channel dropped: no store or zone, 30% volume, no pricing signal.
- Date shift of 8,372 days (23 x 52 weeks): weekdays and seasons kept; anchor 2025-12-04.
- md5-based `u()` instead of DuckDB `hash()`: stable across versions and reproducible in Python.
- Order-independent content hash in manifests: Parquet bytes may differ across writer versions.
- No migration tool: idempotent DDL plus reset on schema change, fine for a demo.
- Watermark from the OLTP clock with a 2-minute overlap: commit-time skew is covered and duplicates collapse in ReplacingMergeTree.
- Watermarks committed after load, not after extract: a failed cycle re-extracts the same window.
- S3 key includes the extract run id: reruns never read stale parts.
- Full load paged by GenerateTableFetch; incremental as one streamed query split by Max Rows Per Flow File.
- SNAPPY Parquet: bundled codec, no native zstd dependency in NiFi.
- NARs in `lib/`, not autoload: loaded deterministically at boot.
- Audit item rows as append-only `step_event` via PutSQL; state via trigger: PutSQL never needs a result set.
- Functions called on the token with ExecuteSQLRecord: token content is disposable.
- Barrier by group concurrency plus Batch Output, item status in the audit DB: no fragment counting.
- One reverse group per target, chained: order prices before labels by construction.
- Basic auth to ClickHouse HTTP: InvokeHTTP's native sensitive property.
- `output_format_json_quote_64bit_integers=0` on reverse queries: bigint keys arrive as numbers.
- Cycle-scoped dbt models append plus a lightweight delete pre-hook: idempotent reruns without FINAL.
- SCD2 price history from the OLTP trigger, not a dbt snapshot: one history covers manual and engine changes.
- No dbt packages: no network at build time; two small generic test macros.
- Checksum = rows + sum(cents) + key-weighted sum: identical in Postgres and ClickHouse.
- Registry schedule is an interval polled by NiFi every minute: no cron parser.
- `make demo` default SF1, CI SF0.01: SF1 shows zones and ladders; SF0.01 fits CI.
- Prometheus skips TLS verification for NiFi only: self-signed certificate on the internal network.
- Alerts without a contact point: visible in Grafana and `audit.alert`; nothing to send to locally.
- ClickHouse and Postgres for CI started with compose, not service containers: service containers cannot mount repo config.
- Prometheus v3.15.1 and Grafana 13.2.5: current stable lines; Dependabot keeps minor and patch current.
- Hard deletes not propagated: the OLTP closes overrides by `valid_to`.