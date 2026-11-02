# retail-pricing-reverse-etl

> ELT plus reverse ETL for retail pricing on a laptop: a busy OLTP Postgres, Apache NiFi
> extracting it incrementally to Parquet on S3-compatible storage, ClickHouse and dbt computing new
> product prices, and NiFi writing only the changed rows back, with every job, run, step and apply
> batch audited in Postgres and the loop monitored with Prometheus and Grafana.

![ci](https://img.shields.io/github/actions/workflow/status/emanuel-luis/retail-pricing-reverse-etl/ci.yml?branch=main&label=ci)
![license](https://img.shields.io/github/license/emanuel-luis/retail-pricing-reverse-etl)
![python](https://img.shields.io/badge/python-3.12-blue)

**Status: work in progress.** The project lands in six pull requests; `docs/PLAN.md` is the
contract and the table below says what exists today.

| Piece | State |
| --- | --- |
| TPC-DS generation and the deterministic OLTP seed | done |
| OLTP and control databases, apply functions, workload generator | next |
| ClickHouse, dbt pricing procedure, dbt runner | planned |
| NiFi image, flows as code, extract and load | planned |
| Transform, reverse load, reconciliation, `make demo` | planned |
| Monitoring, heavy SF10 run, results | planned |

## What this demonstrates

- Reproducible data at scale: TPC-DS generated at a chosen scale factor, mapped to a normalized
  retail OLTP model with deterministic synthetic tables (supplier costs, competitor prices, margin
  targets, KVI flags, price zones, pack-size and brand-tier ladders), with manifests of row counts
  and content hashes.
- Incremental extraction by watermark from an OLTP under load, landing Parquet in an
  S3-compatible raw zone, loaded into ClickHouse (planned).
- A multi-step pricing procedure in dbt on ClickHouse with guardrail tests and unit tests on the
  rule logic (planned).
- Reverse ETL of changed rows only, applied with optimistic concurrency (version checks),
  idempotent per cycle, with conflicts recorded and reconciliation by checksums (planned).
- Job registry and run audit in Postgres, and Prometheus plus Grafana over the whole loop
  (planned).

## Data preparation

![TPC-DS is generated with DuckDB's tpcds extension into Parquet with a manifest, then DuckDB SQL maps it and the deterministic synthetic tables to the OLTP seed, also with a manifest.](docs/diagrams/data-prep.svg)

Source: [data-prep.d2](docs/diagrams/data-prep.d2).

## Tech stack

Python 3.12 · DuckDB · PostgreSQL · Apache NiFi · ClickHouse · dbt · RustFS (S3) · Prometheus ·
Grafana · Docker Compose · GitHub Actions · uv · ruff · mypy · pytest · Hypothesis · D2

## Quickstart (today)

```bash
git clone https://github.com/emanuel-luis/retail-pricing-reverse-etl && cd retail-pricing-reverse-etl
make install
make data SF=0.01   # TPC-DS SF0.01 plus the OLTP seed under data/ (seconds)
make test           # unit tests plus the SF0.01 generation checks
```

On the owner's laptop `make data SF=1` takes about 36 s and SF10 about 8.5 minutes, with DuckDB
capped at 4 GB (not TPC-DS benchmark results).

## Project structure

```
src/retail_pricing/   CLI, settings, JSON logging, data generation and build
src/retail_pricing/sql/duckdb/   TPC-DS to OLTP mapping, one file per table
tests/                unit tests and SF0.01 data checks
docs/                 PLAN (the contract), DECISIONS, D2 diagrams
```

## Data and licence notice

The data comes from the TPC-DS data generator (dsdgen), run locally through DuckDB's `tpcds`
extension. This repository does not redistribute dsdgen and never commits generated data. Any
timing shown here is not a TPC-DS benchmark result and is not comparable to TPC Benchmark
results. TPC, TPC-DS and TPC Benchmark are trademarks of the Transaction Processing Performance
Council.

## License

MIT, see [LICENSE](LICENSE).
