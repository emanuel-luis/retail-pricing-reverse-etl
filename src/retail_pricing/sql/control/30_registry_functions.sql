-- Registry functions. Same conventions as 20_audit_functions.sql.

-- registry.start_cycle(p_mode, p_requested_by, p_rerun_cycle_id) -> (cycle_id, status)
-- Serialized by an advisory transaction lock. While a cycle is running it returns that cycle's id
-- with status 'busy' and changes nothing. Otherwise:
--   p_rerun_cycle_id null      a new running cycle, status 'started'
--   p_rerun_cycle_id of a failed cycle   resets it to running with the same id (attempt + 1,
--                              error and failed_stage cleared, started_at = now()), 'rerun'
-- Rerunning an unknown or non-failed cycle, or a mode other than full|incremental, raises.
-- On a rerun p_mode is ignored: the cycle keeps its mode.
CREATE OR REPLACE FUNCTION registry.start_cycle(
    p_mode text, p_requested_by text, p_rerun_cycle_id bigint DEFAULT NULL)
RETURNS TABLE(cycle_id bigint, status text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_id bigint;
    v_status text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('retail-pricing.cycle'));
    IF p_mode NOT IN ('full', 'incremental') THEN
        RAISE EXCEPTION 'mode must be full or incremental, got %', p_mode;
    END IF;
    SELECT c.cycle_id INTO v_id FROM audit.cycle c WHERE c.status = 'running';
    IF FOUND THEN
        RETURN QUERY SELECT v_id, 'busy'::text;
        RETURN;
    END IF;
    IF p_rerun_cycle_id IS NOT NULL THEN
        SELECT c.status INTO v_status FROM audit.cycle c WHERE c.cycle_id = p_rerun_cycle_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'unknown cycle %', p_rerun_cycle_id;
        ELSIF v_status <> 'failed' THEN
            RAISE EXCEPTION 'cycle % is % and only a failed cycle can be rerun',
                p_rerun_cycle_id, v_status;
        END IF;
        UPDATE audit.cycle c
           SET status = 'running', failed_stage = NULL, error = NULL, ended_at = NULL,
               started_at = now(), attempt = c.attempt + 1
         WHERE c.cycle_id = p_rerun_cycle_id;
        RETURN QUERY SELECT p_rerun_cycle_id, 'rerun'::text;
        RETURN;
    END IF;
    INSERT INTO audit.cycle AS c (mode, requested_by) VALUES (p_mode, p_requested_by)
    RETURNING c.cycle_id INTO v_id;
    RETURN QUERY SELECT v_id, 'started'::text;
END $$;

-- registry.start_cycle_if_due() -> SETOF (cycle_id)
-- Starts an incremental cycle (requested_by 'schedule') and returns its id, only when job 'cycle'
-- is enabled with a schedule_interval, no cycle is running and the last cycle started more than
-- the interval ago (or none exists). Otherwise returns no rows.
CREATE OR REPLACE FUNCTION registry.start_cycle_if_due()
RETURNS TABLE(cycle_id bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_interval interval;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('retail-pricing.cycle'));
    SELECT j.schedule_interval INTO v_interval
    FROM registry.job j WHERE j.job_id = 'cycle' AND j.enabled;
    IF NOT FOUND OR v_interval IS NULL THEN
        RETURN;
    END IF;
    IF EXISTS (SELECT 1 FROM audit.cycle c WHERE c.status = 'running'
               OR c.started_at > now() - v_interval) THEN
        RETURN;
    END IF;
    RETURN QUERY
    SELECT s.cycle_id FROM registry.start_cycle('incremental', 'schedule', NULL) s
    WHERE s.status = 'started';
END $$;

-- registry.extract_plan(p_cycle_id, p_wm_high, p_business_date, p_mode)
--   -> (table_name, mode, wm_low, wm_high, page_rows, pk_column, select_list, load_order)
-- The extract window of each enabled extract_table, ordered by load_order. Idempotent per cycle:
-- the first call writes audit.extract_plan, later calls return the stored plan unchanged (a rerun
-- extracts the same window). A table is 'full' (wm_low null) when p_mode is full (null means the
-- cycle's mode) or it has no watermark; otherwise 'incremental' with
-- wm_low = watermark - 2 minutes (overlap for commit-time skew). Always sets
-- audit.cycle.business_date when p_business_date is given.
CREATE OR REPLACE FUNCTION registry.extract_plan(
    p_cycle_id bigint, p_wm_high timestamptz, p_business_date date, p_mode text DEFAULT NULL)
RETURNS TABLE(table_name text, mode text, wm_low timestamptz, wm_high timestamptz,
              page_rows int, pk_column text, select_list text, load_order int)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_overlap CONSTANT interval := interval '2 minutes';
    v_cycle_mode text;
    v_mode text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('retail-pricing.plan.' || p_cycle_id));
    SELECT c.mode INTO v_cycle_mode FROM audit.cycle c WHERE c.cycle_id = p_cycle_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown cycle %', p_cycle_id;
    END IF;
    v_mode := coalesce(p_mode, v_cycle_mode);
    IF v_mode NOT IN ('full', 'incremental') THEN
        RAISE EXCEPTION 'mode must be full or incremental, got %', v_mode;
    END IF;
    UPDATE audit.cycle c SET business_date = coalesce(p_business_date, c.business_date)
    WHERE c.cycle_id = p_cycle_id;
    IF NOT EXISTS (SELECT 1 FROM audit.extract_plan p WHERE p.cycle_id = p_cycle_id) THEN
        IF p_wm_high IS NULL THEN
            RAISE EXCEPTION 'p_wm_high is required for the first plan of a cycle';
        END IF;
        INSERT INTO audit.extract_plan (cycle_id, table_name, mode, wm_low, wm_high, page_rows)
        SELECT p_cycle_id, t.table_name,
               CASE WHEN v_mode = 'full' OR w.high_value IS NULL THEN 'full'
                    ELSE 'incremental' END,
               CASE WHEN v_mode = 'full' OR w.high_value IS NULL THEN NULL
                    ELSE w.high_value - v_overlap END,
               p_wm_high, t.page_rows
        FROM registry.extract_table t
        LEFT JOIN registry.watermark w ON w.table_name = t.table_name
        WHERE t.enabled;
    END IF;
    RETURN QUERY
    SELECT p.table_name, p.mode, p.wm_low, p.wm_high, p.page_rows, t.pk_column, t.select_list,
           t.load_order
    FROM audit.extract_plan p
    JOIN registry.extract_table t ON t.table_name = p.table_name
    WHERE p.cycle_id = p_cycle_id
    ORDER BY t.load_order;
END $$;

-- registry.commit_watermarks(p_cycle_id) -> (table_name, high_value)
-- Moves each planned table's watermark to the plan's wm_high, only forward (an older or equal
-- value changes nothing). Raises unless the cycle has a succeeded 'load' run. Returns the
-- watermarks of the planned tables after the commit. Safe to call twice.
CREATE OR REPLACE FUNCTION registry.commit_watermarks(p_cycle_id bigint)
RETURNS TABLE(table_name text, high_value timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
BEGIN
    IF NOT EXISTS (SELECT 1 FROM audit.job_run r WHERE r.cycle_id = p_cycle_id
                   AND r.job_id = 'load' AND r.status = 'succeeded') THEN
        RAISE EXCEPTION 'cycle % has no succeeded load run; watermarks not committed', p_cycle_id;
    END IF;
    INSERT INTO registry.watermark AS w (table_name, high_value, cycle_id, updated_at)
    SELECT p.table_name, p.wm_high, p.cycle_id, now()
    FROM audit.extract_plan p WHERE p.cycle_id = p_cycle_id
    ON CONFLICT ON CONSTRAINT watermark_pkey DO UPDATE
        SET high_value = excluded.high_value, cycle_id = excluded.cycle_id,
            updated_at = excluded.updated_at
        WHERE excluded.high_value > w.high_value;
    RETURN QUERY
    SELECT w.table_name, w.high_value
    FROM registry.watermark w
    JOIN audit.extract_plan p ON p.table_name = w.table_name AND p.cycle_id = p_cycle_id
    ORDER BY w.table_name;
END $$;
