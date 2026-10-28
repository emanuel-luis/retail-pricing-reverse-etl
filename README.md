# retail-pricing-reverse-etl

ELT plus reverse ETL for retail pricing, on a laptop: a busy OLTP Postgres, Apache NiFi extracting
it incrementally to Parquet on S3-compatible storage, ClickHouse and dbt computing new product
prices, and NiFi writing only the changed rows back to the OLTP, with every job, run, step and
apply batch audited in Postgres and the whole loop monitored with Prometheus and Grafana.

Work in progress: the project is built in small pull requests. Data is open or synthetic and is
never committed.

## License

MIT, see `LICENSE` (added with the first pull request).
