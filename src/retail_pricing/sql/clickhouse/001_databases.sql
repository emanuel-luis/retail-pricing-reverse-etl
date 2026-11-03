-- Databases of the loop. raw is filled by NiFi (or ch load-raw); the others are built by dbt.
-- Creating them here keeps the grants and the Grafana datasource valid before the first build.
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS int;
CREATE DATABASE IF NOT EXISTS pricing;
CREATE DATABASE IF NOT EXISTS reverse;
CREATE DATABASE IF NOT EXISTS marts;
