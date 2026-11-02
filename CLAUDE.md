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
- `make d2-install`, `make diagrams`, `make diagrams-check`, `make diagrams-watch NAME=<name>`:
  pinned D2 (`D2_VERSION`, sha256 `D2_SHA256`, Linux x86_64). On the owner's machine render through
  WSL: `wsl -d Ubuntu -e bash -lc 'cd /mnt/d/dev/Pessoal/retail-pricing-reverse-etl && make diagrams'`.
- Git Bash on Windows has no make: run the recipe lines by hand (`uv run ...`).
- CI (`.github/workflows/ci.yml`): `lint` gates `test-fast` (unit plus SF0.01 data); `diagrams`
  runs on its own; `UV_LOCKED=1`; actions pinned by SHA. `security.yml`: pip-audit and zizmor.
  Dependabot is monthly with a 7-day cooldown, minor and patch grouped, majors ignored;
  `dependabot-auto-merge.yml` merges green minor and patch updates.

## Repo map

- `docs/PLAN.md`: the contract (services, schemas, flows, models, CI, PR plan).
  `docs/DECISIONS.md`: decision log with reasons. `docs/diagrams/`: D2 sources and committed SVGs.
- `src/retail_pricing/`: `cli.py` (Typer, single entry point `retail-pricing`), `settings.py`
  (pydantic-settings per prefix), `logging.py` (JSON lines, `cycle_id`/`run_id` contextvars),
  `data/` (`contract.py`: every OLTP table's columns, the single source of truth for the Postgres
  and ClickHouse DDL; `generate.py`: TPC-DS through DuckDB's `tpcds` extension; `build.py`: the
  OLTP seed; `synth.py`: Python mirror of `u()` and the pure rules; `manifest.py`), `sql/duckdb/`
  (one mapping SQL per OLTP table, in FK order).
- `tests/unit/` (no data), `tests/slow/` (SF0.01 generation, marker `slow`).

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
