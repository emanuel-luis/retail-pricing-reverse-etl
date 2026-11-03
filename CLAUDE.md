# CLAUDE.md

ELT plus reverse ETL for retail pricing on a laptop: a busy OLTP Postgres, Apache NiFi extracting
it to Parquet on S3-compatible storage, ClickHouse and dbt computing new prices, and NiFi writing
only the changed rows back, with a Postgres registry/audit of every job and Prometheus + Grafana
over the loop. Public portfolio repo; the data is TPC-DS generated locally plus deterministic
synthetic tables. `docs/PLAN.md` is the contract and lists what each pull request adds.

## Commands

- `make install`: `uv sync --locked`.
- `make lint`: ruff check, ruff format --check, mypy strict on `src` (the CI lint job).
- `make test-fast`: pytest with the default selection (deselects `slow`, `sql`, `clickhouse`,
  `integration`).
- `make test-data`: `pytest -m slow`: generates TPC-DS SF0.01 twice in temp dirs, builds the OLTP
  seed and checks counts, schemas against the contract, FKs and determinism (about 10 s).
- `make data SF=<n>`: `retail-pricing data all --sf <n>` into `data/` (git-ignored): TPC-DS
  Parquet plus manifest under `data/tpcds/sf=<n>/`, the OLTP seed under `data/oltp/sf=<n>/`.
  SF1 or more is a heavy local run on the owner's machine (shared heavy lock).
- `make up` / `make down`: compose services with `--wait` (copies `.env.example` to `.env` when
  missing) / every profile down with volumes. `make ps`, `make logs`. Compose project
  `retail-pricing-reverse-etl`; today: `oltp` (127.0.0.1:25432, db `retail`), `control-db`
  (127.0.0.1:25433, db `control`), `clickhouse` (HTTP 127.0.0.1:28123; admin `ch_admin`,
  users `nifi`, `dbt`, `grafana`), `s3` (RustFS, 127.0.0.1:29000), `dbt-runner`
  (127.0.0.1:28580; builds the app image `retail-pricing-app:local`), `workload` (profile
  `workload`, reuses the image), and profile `monitoring` (`make monitoring`; `make demo` starts
  it too): `prometheus` (127.0.0.1:29090), `grafana` (127.0.0.1:23000, anonymous Viewer, admin
  from `.env`; provisioning in `config/grafana/`, three dashboards and nine alert rules),
  `pg-exporter-oltp`, `pg-exporter-control` and `nifi-token` (the app image refreshing the NiFi
  JWT Prometheus reads). A stack run on the owner's machine takes the heavy lock. Bind
  mounts point at the working copy that ran `up`: never remove a worktree whose stack runs.
- `make load SF=<n>`: `retail-pricing oltp load --sf <n>` (`--replace` drops and reloads).
  `make control-init`: `retail-pricing control init` (idempotent; seed from the contract).
  `make test-sql`: `pytest -m sql` against the running services (OLTP loaded at SF0.01; control
  tests use their own database `control_test`). `make workload` / `make workload-stop`.
  `retail-pricing workload edit --count N --kvi --seed S`: scripted manual price edits.
- `make ch-init` / `make s3-init`: ClickHouse databases and raw tables from the contract / the raw
  bucket. `make ch-load SF=<n> CYCLE=<c>`: seed Parquet into raw (`use_hive_partitioning=0`).
  `make dbt-build CYCLE=<c> AS_OF=<date>`: `retail-pricing dbt-runner trigger`, a POST to the
  runner, polled until it ends; exit 1 unless it succeeds. dbt runs inside the runner image (the
  optional `dbt` extra); `uv sync --locked --extra dbt` also installs it on the host.
  `make test-clickhouse`: `pytest -m clickhouse`. `make runner-logs`.
- `make flows`: `retail-pricing nifi build-flows` writes `nifi/flows/retail-pricing.json`
  (deterministic; `tests/unit/test_flows.py` regenerates and diffs it and checks every property
  against `nifi/definitions/`). `make deploy`: `nifi deploy --env local --flow ...` (secrets from
  env, fails on any invalid component, writes the group ids into `registry.job`). `make cycle
  MODE=full|incremental`: `retail-pricing cycle trigger --wait` (exit 0 succeeded, 1 failed,
  2 timeout); `retail-pricing cycle status`. `make test-integration`: `pytest -m integration`
  against the deployed flow (it switches the cycle schedule off while it runs). `retail-pricing
  nifi definitions pull` refreshes the processor definitions from a running NiFi; `nifi token`
  writes a JWT to a file. NiFi UI and API: https://127.0.0.1:28443/nifi (self-signed), trigger
  http://127.0.0.1:28081/cycles.
- `make demo [SF=1]`: `scripts/demo.sh` (shared steps in `scripts/lib.sh`): env check, data,
  stack, load, control init, `control schedule --off`, ch init, s3 init, deploy, cycle 1 full,
  workload on, cycle 2 with 50 KVI edits injected during transform, cycle 3, workload off,
  `retail-pricing check --cycles 1-3 --expect-conflicts-in 2`, `retail-pricing report`; each step
  timed into `runs/<utc>-sf<SF>/steps.json`. `make heavy SF=10`: `scripts/heavy.sh`, workload on
  throughout, `scripts/sample-stats.sh` samples `docker stats` into `stats.jsonl`. `make report`:
  `timings.json` and `timings.md` for the latest run. `retail-pricing control schedule --off |
  --every '10 minutes'`: the registry interval NiFi polls. `retail-pricing cycle wait-stage
  --stage transform [--cycle N]`.
- `make d2-install`, `make diagrams`, `make diagrams-check`, `make diagrams-watch NAME=<name>`:
  pinned D2 (`D2_VERSION`, sha256 `D2_SHA256`, Linux x86_64). On the owner's machine render through
  WSL: `wsl -d Ubuntu -e bash -lc 'cd /mnt/d/dev/Pessoal/retail-pricing-reverse-etl && make diagrams'`.
- Git Bash on Windows has no make: run the recipe lines by hand (`uv run ...`).
- CI (`.github/workflows/ci.yml`): `lint` gates `test-fast` (unit plus SF0.01 data) and `sql`
  (compose Postgres, SF0.01 load, control init, `pytest -m sql`) and `dbt` (ClickHouse, RustFS
  and the runner from compose, two cycles of `dbt build` through the runner,
  `pytest -m clickhouse`) and `e2e` (`make demo SF=0.01` with the `ci` parameters, then
  `pytest -m integration` on the same stack; the run directory always and NiFi's log on failure
  as artifacts); `diagrams` runs on its own;
  `UV_LOCKED=1`; actions pinned by SHA.
  `security.yml`: pip-audit and zizmor.
  Dependabot is monthly with a 7-day cooldown, minor and patch grouped, majors ignored;
  `dependabot-auto-merge.yml` merges green minor and patch updates.

## Repo map

- `docs/PLAN.md`: the contract (services, schemas, flows, models, CI, PR plan).
  `docs/DECISIONS.md`: decision log with reasons. `docs/RUNBOOK.md`: operations and recovery.
  `docs/diagrams/`: D2 sources and committed SVGs.
- `config/prometheus/prometheus.yml`, `config/grafana/` (provisioning: datasources, dashboard
  provider, alert rules; `dashboards/*.json` generated by `scripts/gen_dashboards.py`, kept
  equal by `tests/unit/test_monitoring.py`).
- `src/retail_pricing/`: `cli.py` (Typer, single entry point `retail-pricing`), `settings.py`
  (pydantic-settings per prefix), `logging.py` (JSON lines, `cycle_id`/`run_id` contextvars),
  `data/` (`contract.py`: every OLTP table's columns, the single source of truth for the Postgres
  and ClickHouse DDL; `generate.py`: TPC-DS through DuckDB's `tpcds` extension; `build.py`: the
  OLTP seed; `synth.py`: Python mirror of `u()` and the pure rules; `manifest.py`), `sql/duckdb/`
  (one mapping SQL per OLTP table, in FK order), `oltp/` (`ddl.py` renders tables, constraints
  and indexes from the contract; `load.py`), `sql/oltp/` (triggers, `pricing_apply` stage
  tables and `apply_batch` / `reconcile_actual`, roles), `control/` (`init.py`, `seed.py`: jobs
  and one `extract_table` row per contract table), `sql/control/` (registry, audit, functions,
  views, grants), `workload/` (`logic.py` pure decisions, `run.py` I/O and Prometheus metrics).
- `clickhouse/` (`client.py` httpx on the HTTP interface, `ddl.py` raw tables from the contract,
  `init.py`, `load_raw.py`), `s3/` (`s3 init`), `dbt_runner/` (`server.py` stdlib HTTP API,
  `jobs.py` dbt subprocess and job state, `client.py` the trigger), `config/clickhouse/`
  (`config.d`: memory, prometheus, named collection `raw_s3`; `users.d`: users and grants).
- `dbt/`: profiles read env only; `models/{staging,intermediate,pricing,reverse,marts}` with one
  yml per folder (grains, tests, unit tests); `tests/guardrails/g01..g10`; macros for schema
  names, generic tests, the ending grid and the checksum.
- `nifi/` package: `client.py` (httpx; bearer only, never the login cookie; TLS check off only
  for the local NiFi), `definitions.py`, `deploy.py`, `builder/` (`dsl.py` and one module per
  group: `trigger`, `extract`, `load`, `transform`, `reverse`, `reconcile`, `gate`, `finalize`);
  `cycle.py` (trigger, wait, wait-stage, status), `check.py` (the demo's end-to-end assertions,
  pure functions over rows), `report.py` (timings per run).
  Repo folder `nifi/`: `flows/` (generated, never edited by hand), `params/{local,ci}.json`
  (sensitive entries name env vars), `definitions/` (pulled from NiFi 2.12), `spike/` (the
  reference export that pinned the format). `scripts/nifi_spike.py` reruns the spike.
- `compose.yaml`, `docker/app.Dockerfile` (the one Python image, with the dbt project at
  `/home/app/dbt`), `docker/nifi.Dockerfile` (NiFi 2.12 plus the Parquet NARs and the PostgreSQL
  JDBC driver, sha256-checked), `.env.example`.
- `tests/unit/` (no data), `tests/slow/` (SF0.01 generation, marker `slow`), `tests/sql/`
  (marker `sql`, compose Postgres), `tests/clickhouse/` (marker `clickhouse`),
  `tests/integration/` (marker `integration`, deployed NiFi flow; `test_monitoring_stack.py`
  needs the monitoring profile).

## Invariants

- Generated data is never committed (`data/`, `runs/` are git-ignored) and dsdgen is never
  redistributed. Any timing shown anywhere says it is not a TPC-DS benchmark result and is not
  comparable to TPC Benchmark results.
- Randomness in the seed comes only from `u(key, salt)` (md5-based, same values in DuckDB SQL and
  Python); the same SF gives the same content hashes on every run.
- Dates: TPC-DS dates shift by 8,372 days; the anchor business date is 2025-12-04. Pricing uses
  the cycle's business date, never the wall clock.
- The OLTP seed Parquet columns are exactly `data/contract.py`, in order; change the contract, the
  build SQL and (later) the DDL together.
- DuckDB stays on 2.0.x: a minor update can change the generated rows (Dependabot ignores it).
- The NiFi flow JSON is generated: change the builder, run `make flows`, commit both. Every group
  sets its parameter context; every component carries `propertyDescriptors`; ids are uuid5 from
  the component path. Stage groups are barriers (Single FlowFile Per Node + Batch Output); item
  status lives in the audit database; `end_run` also fails a run whose planned items never
  started; watermarks commit only for tables extracted and loaded. Order: trigger, extract,
  load, transform, reverse (zone price, store override, promo price, shelf label), reconcile,
  finalize. A cycle's marts are complete only after the next cycle's extract.
- The OLTP applies engine prices only through `pricing_apply.apply_batch`: a row changes only when
  its `version` equals the extracted one; anything else is a recorded conflict, never an
  overwrite. A batch is idempotent per (cycle, target, batch). Workload edits run with
  `app.actor = 'store-ops'` and clear `last_cycle_id`.
- dbt models never read the wall clock: only `var('cycle_id')` and `var('as_of')`; money in
  integer cents or Decimal, never Float; cycle-scoped models delete their own cycle before
  appending, so a rerun replaces it. Guardrail errors stop the cycle before the reverse load.
- SECURITY DEFINER functions are owned by NOLOGIN roles with only the grants they need, never by
  the superuser the Postgres image creates.
- Secrets come from `.env` (copied from `.env.example`, local-only values); settings hold them as
  `SecretStr` and logs never print them.

## Conventions

- Python 3.12, uv, ruff (line length 100, `BLE`), mypy strict, pytest with Hypothesis.
- Conventional Commits in English; one PR per phase, each merged before the next starts.
- Stack, image builds and data generation at SF1 or more take the shared heavy lock
  (`D:/dev/Pessoal/.locks/heavy`) on the owner's machine and end with `docker compose down -v`.
- Host ports in the 2xxxx range, bound to 127.0.0.1; host clients use `127.0.0.1`, not
  `localhost` (IPv6 first on Windows).
- Generic retail names only; no employer names, hosts or identifiers anywhere in this repo.
- Diagrams are D2 only (no Mermaid). Each `.d2` starts with `...@_style`, uses its classes and
  never sets a fill color. About 25 nodes and 1,300 px wide at most. A diagram names only what the
  code has. Commit the `.d2` and the `.svg` together; never edit an SVG by hand.
