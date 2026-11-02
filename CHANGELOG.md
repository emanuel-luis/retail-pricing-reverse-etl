# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
