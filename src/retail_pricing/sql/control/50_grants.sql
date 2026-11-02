-- Grants. The roles nifi, grafana_ro and exporter are created by `control init` before this file.
-- Re-running is harmless. Functions run as the owner, so nifi needs EXECUTE and no table rights
-- except what PutSQL and PutDatabaseRecord write directly.

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA registry, audit FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA registry, audit FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA registry, audit FROM PUBLIC;

-- nifi: call the functions, append step events, upsert the tables PutDatabaseRecord writes.
GRANT USAGE ON SCHEMA registry, audit TO nifi;
GRANT EXECUTE ON FUNCTION
    registry.start_cycle(text, text, bigint),
    registry.start_cycle_if_due(),
    registry.extract_plan(bigint, timestamptz, date, text),
    registry.commit_watermarks(bigint),
    audit.start_run(bigint, text),
    audit.load_plan(bigint, uuid),
    audit.stage_plan(bigint, text),
    audit.end_run(uuid),
    audit.reconcile(bigint),
    audit.finish_cycle(bigint, text, text, text)
    TO nifi;
GRANT INSERT ON audit.step_event TO nifi;
GRANT INSERT, SELECT, UPDATE ON
    audit.dbt_node_result, audit.reverse_batch,
    audit.reconcile_expected, audit.reconcile_actual
    TO nifi;

-- grafana_ro: read the tables and views, nothing else.
GRANT USAGE ON SCHEMA registry, audit TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA registry, audit TO grafana_ro;

-- exporter: postgres-exporter.
GRANT pg_monitor TO exporter;

-- audit_writer: NOLOGIN owner of every SECURITY DEFINER function, so they never run as the
-- (super)user that built the database. It gets only what those functions touch: no DDL, no
-- TRUNCATE, and DELETE only on reconcile_result. Identity columns need no sequence grants.
GRANT USAGE ON SCHEMA registry, audit TO audit_writer;
GRANT SELECT ON registry.job, registry.extract_table,
    audit.reconcile_expected, audit.reconcile_actual TO audit_writer;
GRANT SELECT, INSERT, UPDATE ON registry.watermark, audit.cycle, audit.job_run, audit.step,
    audit.alert TO audit_writer;
GRANT SELECT, INSERT ON audit.extract_plan TO audit_writer;
GRANT SELECT, INSERT, DELETE ON audit.reconcile_result TO audit_writer;

-- ALTER FUNCTION ... OWNER TO needs CREATE on the schema for the new owner: held only here.
GRANT CREATE ON SCHEMA registry, audit TO audit_writer;
DO $$
DECLARE
    f regprocedure;
BEGIN
    FOR f IN
        SELECT p.oid::regprocedure
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname IN ('registry', 'audit') AND p.prosecdef
          AND pg_get_userbyid(p.proowner) <> 'audit_writer'
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO audit_writer', f);
    END LOOP;
END $$;
REVOKE CREATE ON SCHEMA registry, audit FROM audit_writer;
