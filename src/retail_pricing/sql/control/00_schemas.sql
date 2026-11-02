-- Control database: schemas and the job registry. Idempotent; a reset is DROP SCHEMA ... CASCADE.
-- Every timestamp is timestamptz (stored as UTC); the database default timezone is set to UTC.

DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'UTC');
END $$;

CREATE SCHEMA IF NOT EXISTS registry;
CREATE SCHEMA IF NOT EXISTS audit;

-- One row per NiFi stage (process group). A disabled job has an empty stage plan and its run
-- ends skipped. nifi_process_group_id is filled by the NiFi deploy.
CREATE TABLE IF NOT EXISTS registry.job (
    job_id                text PRIMARY KEY,
    process_group_name    text NOT NULL,
    nifi_process_group_id text,
    source                text,
    target                text,
    schedule_interval     interval,
    owner                 text NOT NULL,
    sla                   interval NOT NULL,
    enabled               boolean NOT NULL DEFAULT true,
    stage_order           int NOT NULL,
    batch_rows            int
);

-- The extract list is data, not flow: adding a table needs no flow change.
-- pk_column is a dense numeric column for value partitioning of full loads.
CREATE TABLE IF NOT EXISTS registry.extract_table (
    table_name  text PRIMARY KEY,
    enabled     boolean NOT NULL DEFAULT true,
    pk_column   text NOT NULL,
    select_list text NOT NULL,
    page_rows   int NOT NULL DEFAULT 250000 CHECK (page_rows > 0),
    load_order  int NOT NULL
);

-- Highest OLTP clock value whose rows are loaded. Only commit_watermarks writes it, and it only
-- moves forward.
CREATE TABLE IF NOT EXISTS registry.watermark (
    table_name text PRIMARY KEY,
    high_value timestamptz NOT NULL,
    cycle_id   bigint,
    updated_at timestamptz NOT NULL DEFAULT now()
);
