-- Audit tables. step_event is append-only; the trigger in 20_audit_functions.sql keeps audit.step.

CREATE TABLE IF NOT EXISTS audit.cycle (
    cycle_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mode          text NOT NULL CHECK (mode IN ('full', 'incremental')),
    requested_by  text NOT NULL,
    business_date date,
    status        text NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'succeeded', 'failed')),
    failed_stage  text,
    error         text,
    attempt       int NOT NULL DEFAULT 1,
    started_at    timestamptz NOT NULL DEFAULT now(),
    ended_at      timestamptz
);
-- At most one running cycle, enforced by the database and not only by start_cycle.
CREATE UNIQUE INDEX IF NOT EXISTS cycle_one_running ON audit.cycle ((true)) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS cycle_started_at ON audit.cycle (started_at DESC);

CREATE TABLE IF NOT EXISTS audit.job_run (
    run_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    cycle_id   bigint NOT NULL REFERENCES audit.cycle,
    job_id     text NOT NULL REFERENCES registry.job,
    status     text NOT NULL DEFAULT 'running'
               CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
    started_at timestamptz NOT NULL DEFAULT now(),
    ended_at   timestamptz,
    rows_in    bigint,
    rows_out   bigint,
    bytes      bigint,
    error      text
);
CREATE INDEX IF NOT EXISTS job_run_cycle ON audit.job_run (cycle_id, job_id, started_at DESC);
CREATE INDEX IF NOT EXISTS job_run_job_time ON audit.job_run (job_id, started_at DESC);

CREATE TABLE IF NOT EXISTS audit.extract_plan (
    cycle_id   bigint NOT NULL REFERENCES audit.cycle,
    table_name text NOT NULL,
    mode       text NOT NULL CHECK (mode IN ('full', 'incremental')),
    wm_low     timestamptz,
    wm_high    timestamptz NOT NULL,
    page_rows  int NOT NULL,
    PRIMARY KEY (cycle_id, table_name)
);

CREATE TABLE IF NOT EXISTS audit.step_event (
    event_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id    uuid NOT NULL REFERENCES audit.job_run,
    step_id   uuid NOT NULL,
    step_name text NOT NULL,
    item      text,
    event     text NOT NULL CHECK (event IN ('started', 'succeeded', 'failed', 'retried')),
    rows      bigint,
    bytes     bigint,
    detail    jsonb,
    at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS step_event_run ON audit.step_event (run_id, at);
CREATE INDEX IF NOT EXISTS step_event_step ON audit.step_event (step_id);
CREATE INDEX IF NOT EXISTS step_event_at ON audit.step_event (at DESC);

-- Current state of each step, maintained by the trigger on step_event. Not written by NiFi.
CREATE TABLE IF NOT EXISTS audit.step (
    step_id    uuid PRIMARY KEY,
    run_id     uuid NOT NULL REFERENCES audit.job_run,
    step_name  text NOT NULL,
    item       text,
    status     text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at timestamptz NOT NULL,
    ended_at   timestamptz,
    rows       bigint,
    bytes      bigint,
    attempts   int NOT NULL DEFAULT 1,
    error      text
);
CREATE INDEX IF NOT EXISTS step_run_status ON audit.step (run_id, status);

CREATE TABLE IF NOT EXISTS audit.dbt_node_result (
    run_id           uuid NOT NULL REFERENCES audit.job_run,
    unique_id        text NOT NULL,
    cycle_id         bigint NOT NULL REFERENCES audit.cycle,
    resource_type    text,
    status           text NOT NULL,
    failures         int,
    execution_time_s double precision,
    rows_affected    bigint,
    message          text,
    PRIMARY KEY (run_id, unique_id)
);
CREATE INDEX IF NOT EXISTS dbt_node_result_cycle ON audit.dbt_node_result (cycle_id);

CREATE TABLE IF NOT EXISTS audit.reverse_batch (
    cycle_id  bigint NOT NULL REFERENCES audit.cycle,
    target    text NOT NULL,
    batch_no  int NOT NULL,
    run_id    uuid REFERENCES audit.job_run,
    sent      bigint NOT NULL DEFAULT 0,
    applied   bigint NOT NULL DEFAULT 0,
    unchanged bigint NOT NULL DEFAULT 0,
    conflicts bigint NOT NULL DEFAULT 0,
    rejects   bigint NOT NULL DEFAULT 0,
    replayed  bigint NOT NULL DEFAULT 0,
    at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cycle_id, target, batch_no)
);
CREATE INDEX IF NOT EXISTS reverse_batch_at ON audit.reverse_batch (at DESC);

-- Expected (ClickHouse) and actual (OLTP) checksums. zone_id and category_id are 0 when a check
-- is not split by them, so they can sit in the primary key.
CREATE TABLE IF NOT EXISTS audit.reconcile_expected (
    cycle_id    bigint NOT NULL REFERENCES audit.cycle,
    target      text NOT NULL,
    zone_id     smallint NOT NULL DEFAULT 0,
    category_id int NOT NULL DEFAULT 0,
    check_name  text NOT NULL,
    rows        bigint NOT NULL,
    sum_cents   bigint NOT NULL,
    wsum        numeric NOT NULL,
    PRIMARY KEY (cycle_id, target, zone_id, category_id, check_name)
);
CREATE TABLE IF NOT EXISTS audit.reconcile_actual (
    cycle_id    bigint NOT NULL REFERENCES audit.cycle,
    target      text NOT NULL,
    zone_id     smallint NOT NULL DEFAULT 0,
    category_id int NOT NULL DEFAULT 0,
    check_name  text NOT NULL,
    rows        bigint NOT NULL,
    sum_cents   bigint NOT NULL,
    wsum        numeric NOT NULL,
    PRIMARY KEY (cycle_id, target, zone_id, category_id, check_name)
);
CREATE TABLE IF NOT EXISTS audit.reconcile_result (
    cycle_id    bigint NOT NULL REFERENCES audit.cycle,
    target      text NOT NULL,
    zone_id     smallint NOT NULL DEFAULT 0,
    category_id int NOT NULL DEFAULT 0,
    check_name  text NOT NULL,
    passed      boolean NOT NULL,
    detail      jsonb,
    PRIMARY KEY (cycle_id, target, zone_id, category_id, check_name)
);

CREATE TABLE IF NOT EXISTS audit.alert (
    alert_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cycle_id    bigint REFERENCES audit.cycle,
    run_id      uuid REFERENCES audit.job_run,
    severity    text NOT NULL DEFAULT 'error' CHECK (severity IN ('info', 'warning', 'error')),
    kind        text NOT NULL,
    message     text NOT NULL,
    raised_at   timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz
);
CREATE INDEX IF NOT EXISTS alert_raised_at ON audit.alert (raised_at DESC);
CREATE INDEX IF NOT EXISTS alert_cycle ON audit.alert (cycle_id, kind);
