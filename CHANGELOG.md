# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- ClickHouse 26.3 and RustFS in compose: server memory limits, Prometheus endpoint, the `raw_s3`
  named collection, users `nifi`, `dbt` and `grafana` with least-privilege grants;
  `retail-pricing ch init` (raw tables rendered from the contract as ReplacingMergeTree by
  version), `ch load-raw` and `s3 init`.
- The dbt pricing procedure (dbt-core 1.12.6, dbt-clickhouse 1.10.3): 61 models from staging to
  the reverse diffs and marts, 139 generic tests, guardrails g01 to g10 and dbt unit tests on the
  rule logic.
- The dbt runner: an HTTP service that runs `dbt build` for a cycle (idempotent per cycle, job
  state on a volume, node results shaped for the audit database, Prometheus metrics), and
  `retail-pricing dbt-runner trigger`; the CI `dbt` job and the `dbt-dag` diagram.
- OLTP Postgres generated from the data contract (`retail-pricing oltp load --sf N`): COPY of
  the seed, constraints, indexes, version and history triggers, the `pricing_apply` stage tables
  and functions (`apply_batch` with version checks, conflicts and replay; `reconcile_actual`
  checksums) and the `workload`, `nifi` and `exporter` roles.
- Control/audit Postgres (`retail-pricing control init`): job registry, extract list and
  watermarks, cycles, runs, step events, dbt results, reverse batches, reconciliation, alerts,
  Grafana views and the functions NiFi will call.
- Workload generator (`retail-pricing workload run`, compose profile `workload`) with Prometheus
  metrics, and `workload edit` for scripted manual price edits.
- Compose services `oltp`, `control-db` and `workload`, the app image, `.env.example`, the CI
  `sql` job and the OLTP pricing and sales ERDs.
- TPC-DS generation through DuckDB's `tpcds` extension (`retail-pricing data generate --sf N`)
  and the deterministic OLTP seed (`retail-pricing data build --sf N`): store and web sales,
  returns, products with pack sizes and brand tiers, price zones, store stock, supplier cost
  history, competitor prices, promotions, margin rules, KVI flags and price ladders, each with a
  manifest of row counts and content hashes.
- `docs/PLAN.md` (the contract), `docs/DECISIONS.md`, the `data-prep` D2 diagram, CLAUDE.md.
- CI: `lint`, `test-fast` (unit plus SF0.01 data checks) and `diagrams`; `security.yml` with
  pip-audit and zizmor; Dependabot (monthly, minor and patch) with an auto-merge workflow.
