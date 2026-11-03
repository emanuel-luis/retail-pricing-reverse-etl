-- Audit functions and triggers. All run as the owner (SECURITY DEFINER, fixed search_path), so the
-- nifi role needs EXECUTE only. They return rows that ExecuteSQLRecord can read. Timestamps are
-- now() (timestamptz, UTC). `#variable_conflict use_column` lets RETURNS TABLE names match columns.

-- Append-only guard for audit.step_event.
CREATE OR REPLACE FUNCTION audit.forbid_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = 'restrict_violation';
END $$;

DROP TRIGGER IF EXISTS step_event_append_only ON audit.step_event;
CREATE TRIGGER step_event_append_only BEFORE UPDATE OR DELETE ON audit.step_event
    FOR EACH ROW EXECUTE FUNCTION audit.forbid_change();

-- Keeps audit.step current from step_event rows.
--   started   inserts a running step (a second started for the same step_id is ignored)
--   retried   attempts + 1 and the last error text
--   succeeded / failed  close the step; rows and bytes keep the last non-null value
-- The error text is detail->>'error', else detail->>'message'.
CREATE OR REPLACE FUNCTION audit.apply_step_event() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    v_error text := coalesce(NEW.detail ->> 'error', NEW.detail ->> 'message');
BEGIN
    IF NEW.event = 'started' THEN
        INSERT INTO audit.step (step_id, run_id, step_name, item, status, started_at)
        VALUES (NEW.step_id, NEW.run_id, NEW.step_name, NEW.item, 'running', NEW.at)
        ON CONFLICT (step_id) DO NOTHING;
    ELSIF NEW.event = 'retried' THEN
        INSERT INTO audit.step AS s (step_id, run_id, step_name, item, status, started_at,
                                     attempts, error)
        VALUES (NEW.step_id, NEW.run_id, NEW.step_name, NEW.item, 'running', NEW.at, 2, v_error)
        ON CONFLICT (step_id) DO UPDATE
            SET attempts = s.attempts + 1, error = coalesce(excluded.error, s.error);
    ELSE
        INSERT INTO audit.step AS s (step_id, run_id, step_name, item, status, started_at,
                                     ended_at, rows, bytes, error)
        VALUES (NEW.step_id, NEW.run_id, NEW.step_name, NEW.item, NEW.event, NEW.at, NEW.at,
                NEW.rows, NEW.bytes, CASE WHEN NEW.event = 'failed' THEN v_error END)
        ON CONFLICT (step_id) DO UPDATE
            SET status = excluded.status,
                ended_at = excluded.ended_at,
                rows = coalesce(excluded.rows, s.rows),
                bytes = coalesce(excluded.bytes, s.bytes),
                error = excluded.error;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS step_event_apply ON audit.step_event;
CREATE TRIGGER step_event_apply AFTER INSERT ON audit.step_event
    FOR EACH ROW EXECUTE FUNCTION audit.apply_step_event();

-- audit.start_run(p_cycle_id, p_job_id) -> (run_id, enabled)
-- Opens a run of the job in the cycle. A disabled job gets a run that is already skipped and
-- enabled = false, so the stage does nothing. Unknown job or cycle raises. Each call is a new run.
CREATE OR REPLACE FUNCTION audit.start_run(p_cycle_id bigint, p_job_id text)
RETURNS TABLE(run_id uuid, enabled boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_enabled boolean;
    v_run uuid;
BEGIN
    SELECT j.enabled INTO v_enabled FROM registry.job j WHERE j.job_id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown job %', p_job_id;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM audit.cycle c WHERE c.cycle_id = p_cycle_id) THEN
        RAISE EXCEPTION 'unknown cycle %', p_cycle_id;
    END IF;
    INSERT INTO audit.job_run AS r (cycle_id, job_id, status, ended_at)
    VALUES (p_cycle_id, p_job_id,
            CASE WHEN v_enabled THEN 'running' ELSE 'skipped' END,
            CASE WHEN v_enabled THEN NULL ELSE now() END)
    RETURNING r.run_id INTO v_run;
    RETURN QUERY SELECT v_run, v_enabled;
END $$;

-- audit.load_plan(p_cycle_id, p_extract_run_id) -> (table_name, prefix, expected_rows, file_count,
-- load_order)
-- One row per table that the extract run wrote rows for: the S3 prefix of the run, the sum of the
-- rows of its succeeded extract steps and the number of non-empty steps (files). Extract items are
-- named '<table>#<page>-<fragment>'. Ordered by load_order. Read-only.
CREATE OR REPLACE FUNCTION audit.load_plan(p_cycle_id bigint, p_extract_run_id uuid)
RETURNS TABLE(table_name text, prefix text, expected_rows bigint, file_count bigint,
              load_order int)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
BEGIN
    RETURN QUERY
    SELECT x.tn,
           'raw/' || x.tn || '/cycle=' || p_cycle_id || '/run=' || p_extract_run_id || '/',
           x.total, x.files, coalesce(t.load_order, 2147483647)
    FROM (SELECT split_part(s.item, '#', 1) AS tn,
                 sum(s.rows)::bigint AS total,
                 count(*) FILTER (WHERE s.rows > 0) AS files
          FROM audit.step s
          WHERE s.run_id = p_extract_run_id AND s.status = 'succeeded'
          GROUP BY 1
          HAVING sum(s.rows) > 0) x
    LEFT JOIN registry.extract_table t ON t.table_name = x.tn
    ORDER BY 5, 1;
END $$;

-- audit.stage_plan(p_cycle_id, p_job_id) -> (item, target)
-- Items of a stage that has no plan query of its own. reconcile: one item per enabled reverse.*
-- job (target = the job name after 'reverse.'), in stage order. Any other job: one item named
-- after the job. A disabled job returns no rows. Unknown job raises.
CREATE OR REPLACE FUNCTION audit.stage_plan(p_cycle_id bigint, p_job_id text)
RETURNS TABLE(item text, target text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_enabled boolean;
BEGIN
    SELECT j.enabled INTO v_enabled FROM registry.job j WHERE j.job_id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown job %', p_job_id;
    END IF;
    IF NOT v_enabled THEN
        RETURN;
    END IF;
    IF p_job_id = 'reconcile' THEN
        RETURN QUERY
        SELECT substr(j.job_id, 9), substr(j.job_id, 9)
        FROM registry.job j
        WHERE j.job_id LIKE 'reverse.%' AND j.enabled
        ORDER BY j.stage_order;
    ELSE
        RETURN QUERY SELECT p_job_id, NULL::text;
    END IF;
END $$;

-- audit.end_run(p_run_id) -> (status, failed_steps, open_steps)
-- Closes a running run: failed when any step failed or is still open, else succeeded. A planned
-- item with no step at all counts as open, so an item NiFi dropped before its first event fails
-- the run: for job extract every audit.extract_plan table of the cycle needs an 'extract.table'
-- step (item = table), for job load every table of audit.load_plan(cycle, latest succeeded
-- extract run) a 'load.table' step. Fills rows_in = rows_out (sum of rows of succeeded steps) and
-- bytes, and on failure stores the error and raises an alert of kind stage_failed. Safe to call
-- twice: an ended run is returned as is.
CREATE OR REPLACE FUNCTION audit.end_run(p_run_id uuid)
RETURNS TABLE(status text, failed_steps int, open_steps int)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_run audit.job_run%ROWTYPE;
    v_failed int;
    v_open int;
    v_rows bigint;
    v_bytes bigint;
    v_errors text;
    v_status text;
    v_error text;
    v_planned text[] := '{}';
    v_missing text[];
    v_extract uuid;
BEGIN
    SELECT * INTO v_run FROM audit.job_run r WHERE r.run_id = p_run_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown run %', p_run_id;
    END IF;
    SELECT count(*) FILTER (WHERE s.status = 'failed'),
           count(*) FILTER (WHERE s.status = 'running'),
           sum(s.rows) FILTER (WHERE s.status = 'succeeded'),
           sum(s.bytes) FILTER (WHERE s.status = 'succeeded')
    INTO v_failed, v_open, v_rows, v_bytes
    FROM audit.step s WHERE s.run_id = p_run_id;
    IF v_run.job_id = 'extract' THEN
        SELECT coalesce(array_agg(p.table_name), '{}') INTO v_planned
        FROM audit.extract_plan p WHERE p.cycle_id = v_run.cycle_id;
    ELSIF v_run.job_id = 'load' THEN
        SELECT r.run_id INTO v_extract FROM audit.job_run r
        WHERE r.cycle_id = v_run.cycle_id AND r.job_id = 'extract' AND r.status = 'succeeded'
        ORDER BY r.started_at DESC LIMIT 1;
        IF FOUND THEN
            SELECT coalesce(array_agg(lp.table_name), '{}') INTO v_planned
            FROM audit.load_plan(v_run.cycle_id, v_extract) lp;
        END IF;
    END IF;
    -- A failed or running step is already counted above; only items with no step are added.
    SELECT array_agg(t ORDER BY t) INTO v_missing
    FROM unnest(v_planned) t
    WHERE NOT EXISTS (SELECT 1 FROM audit.step s WHERE s.run_id = p_run_id
                      AND s.step_name = v_run.job_id || '.table' AND s.item = t);
    v_open := v_open + coalesce(cardinality(v_missing), 0);
    IF v_run.status <> 'running' THEN
        RETURN QUERY SELECT v_run.status, v_failed, v_open;
        RETURN;
    END IF;
    v_status := CASE WHEN v_failed > 0 OR v_open > 0 THEN 'failed' ELSE 'succeeded' END;
    IF v_status = 'failed' THEN
        SELECT string_agg(f.msg, '; ') INTO v_errors
        FROM (SELECT coalesce(s.item, s.step_name) || ': ' || coalesce(s.error, 'failed') AS msg
              FROM audit.step s
              WHERE s.run_id = p_run_id AND s.status = 'failed'
              ORDER BY s.started_at LIMIT 3) f;
        v_error := concat_ws('; ',
            CASE WHEN v_failed > 0 THEN v_failed || ' failed step(s): ' || v_errors END,
            CASE WHEN v_open > 0 THEN v_open || ' step(s) still open' END,
            CASE WHEN v_missing IS NOT NULL THEN cardinality(v_missing)
                || ' planned item(s) never started: ' || array_to_string(v_missing, ', ') END);
    END IF;
    UPDATE audit.job_run r
       SET status = v_status, ended_at = now(), rows_in = v_rows, rows_out = v_rows,
           bytes = v_bytes, error = v_error
     WHERE r.run_id = p_run_id;
    IF v_status = 'failed' THEN
        INSERT INTO audit.alert (cycle_id, run_id, severity, kind, message)
        VALUES (v_run.cycle_id, p_run_id, 'error', 'stage_failed',
                v_run.job_id || ' failed: ' || v_error);
    END IF;
    RETURN QUERY SELECT v_status, v_failed, v_open;
END $$;

-- audit.reconcile(p_cycle_id) -> (passed, failed_checks)
-- Compares reconcile_expected with reconcile_actual per (target, zone_id, category_id, check_name)
-- on rows, sum_cents and wsum. A check missing on either side fails. Rewrites reconcile_result of
-- the cycle. On failure raises one alert of kind reconcile_failed (not repeated while unresolved);
-- a passing call resolves it. With no checks at all it passes. Safe to call twice.
CREATE OR REPLACE FUNCTION audit.reconcile(p_cycle_id bigint)
RETURNS TABLE(passed boolean, failed_checks int)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_failed int;
    v_run uuid;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM audit.cycle c WHERE c.cycle_id = p_cycle_id) THEN
        RAISE EXCEPTION 'unknown cycle %', p_cycle_id;
    END IF;
    DELETE FROM audit.reconcile_result r WHERE r.cycle_id = p_cycle_id;
    INSERT INTO audit.reconcile_result
        (cycle_id, target, zone_id, category_id, check_name, passed, detail)
    SELECT p_cycle_id,
           coalesce(e.target, a.target), coalesce(e.zone_id, a.zone_id),
           coalesce(e.category_id, a.category_id), coalesce(e.check_name, a.check_name),
           (e.cycle_id IS NOT NULL AND a.cycle_id IS NOT NULL AND e.rows = a.rows
            AND e.sum_cents = a.sum_cents AND e.wsum = a.wsum),
           jsonb_build_object(
               'expected', CASE WHEN e.cycle_id IS NULL THEN NULL ELSE
                   jsonb_build_object('rows', e.rows, 'sum_cents', e.sum_cents, 'wsum', e.wsum) END,
               'actual', CASE WHEN a.cycle_id IS NULL THEN NULL ELSE
                   jsonb_build_object('rows', a.rows, 'sum_cents', a.sum_cents, 'wsum', a.wsum) END)
    FROM (SELECT * FROM audit.reconcile_expected x WHERE x.cycle_id = p_cycle_id) e
    FULL JOIN (SELECT * FROM audit.reconcile_actual y WHERE y.cycle_id = p_cycle_id) a
      ON e.target = a.target AND e.zone_id = a.zone_id AND e.category_id = a.category_id
     AND e.check_name = a.check_name;
    SELECT count(*) INTO v_failed
    FROM audit.reconcile_result r WHERE r.cycle_id = p_cycle_id AND NOT r.passed;
    IF v_failed > 0 THEN
        SELECT r.run_id INTO v_run FROM audit.job_run r
        WHERE r.cycle_id = p_cycle_id AND r.job_id = 'reconcile'
        ORDER BY r.started_at DESC LIMIT 1;
        INSERT INTO audit.alert (cycle_id, run_id, severity, kind, message)
        SELECT p_cycle_id, v_run, 'error', 'reconcile_failed',
               v_failed || ' reconcile check(s) differ between ClickHouse and the OLTP'
        WHERE NOT EXISTS (SELECT 1 FROM audit.alert al WHERE al.cycle_id = p_cycle_id
                          AND al.kind = 'reconcile_failed' AND al.resolved_at IS NULL);
    ELSE
        UPDATE audit.alert al SET resolved_at = now()
        WHERE al.cycle_id = p_cycle_id AND al.kind = 'reconcile_failed' AND al.resolved_at IS NULL;
    END IF;
    RETURN QUERY SELECT v_failed = 0, v_failed;
END $$;

-- audit.finish_cycle(p_cycle_id, p_status, p_failed_stage, p_error) -> (cycle_id, status)
-- Ends a running cycle as succeeded or failed (failed_stage and error stored). On failure any run
-- still running is marked failed. An already ended cycle is returned unchanged, so a second call
-- is a no-op.
CREATE OR REPLACE FUNCTION audit.finish_cycle(
    p_cycle_id bigint, p_status text, p_failed_stage text DEFAULT NULL, p_error text DEFAULT NULL)
RETURNS TABLE(cycle_id bigint, status text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_status text;
BEGIN
    IF p_status NOT IN ('succeeded', 'failed') THEN
        RAISE EXCEPTION 'status must be succeeded or failed, got %', p_status;
    END IF;
    SELECT c.status INTO v_status FROM audit.cycle c WHERE c.cycle_id = p_cycle_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown cycle %', p_cycle_id;
    END IF;
    IF v_status = 'running' THEN
        UPDATE audit.cycle c
           SET status = p_status, ended_at = now(),
               failed_stage = CASE WHEN p_status = 'failed' THEN p_failed_stage END,
               error = CASE WHEN p_status = 'failed' THEN p_error END
         WHERE c.cycle_id = p_cycle_id;
        IF p_status = 'failed' THEN
            UPDATE audit.job_run r
               SET status = 'failed', ended_at = now(), error = coalesce(r.error, 'cycle failed')
             WHERE r.cycle_id = p_cycle_id AND r.status = 'running';
        END IF;
        v_status := p_status;
    END IF;
    RETURN QUERY SELECT p_cycle_id, v_status;
END $$;
