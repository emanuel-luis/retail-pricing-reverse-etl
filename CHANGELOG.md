# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- TPC-DS generation through DuckDB's `tpcds` extension (`retail-pricing data generate --sf N`)
  and the deterministic OLTP seed (`retail-pricing data build --sf N`): store and web sales,
  returns, products with pack sizes and brand tiers, price zones, store stock, supplier cost
  history, competitor prices, promotions, margin rules, KVI flags and price ladders, each with a
  manifest of row counts and content hashes.
- `docs/PLAN.md` (the contract), `docs/DECISIONS.md`, the `data-prep` D2 diagram, CLAUDE.md.
- CI: `lint`, `test-fast` (unit plus SF0.01 data checks) and `diagrams`; `security.yml` with
  pip-audit and zizmor; Dependabot (monthly, minor and patch) with an auto-merge workflow.
