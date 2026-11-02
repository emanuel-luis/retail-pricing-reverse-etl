-- Grafana views. Each reads the latest run of a job per cycle, so a rerun does not double count.

CREATE OR REPLACE VIEW audit.v_latest_run AS
SELECT DISTINCT ON (r.cycle_id, r.job_id)
       r.run_id, r.cycle_id, r.job_id, r.status, r.started_at, r.ended_at,
       r.rows_in, r.rows_out, r.bytes, r.error
FROM audit.job_run r
ORDER BY r.cycle_id, r.job_id, r.started_at DESC;

CREATE OR REPLACE VIEW audit.v_cycle_stage_duration AS
SELECT r.cycle_id, c.started_at AS cycle_started_at, c.mode, r.job_id, j.stage_order,
       r.status, r.started_at, r.ended_at,
       extract(epoch FROM coalesce(r.ended_at, now()) - r.started_at)::double precision AS duration_s
FROM audit.v_latest_run r
JOIN audit.cycle c USING (cycle_id)
JOIN registry.job j USING (job_id);

CREATE OR REPLACE VIEW audit.v_rows_per_stage AS
SELECT r.cycle_id, c.started_at AS cycle_started_at, r.job_id, j.stage_order, r.status,
       r.rows_in, r.rows_out, r.bytes
FROM audit.v_latest_run r
JOIN audit.cycle c USING (cycle_id)
JOIN registry.job j USING (job_id)
WHERE r.rows_out IS NOT NULL OR r.bytes IS NOT NULL;

CREATE OR REPLACE VIEW audit.v_reverse_totals AS
SELECT b.cycle_id, c.started_at AS cycle_started_at, b.target,
       count(*) AS batches, sum(b.sent) AS sent, sum(b.applied) AS applied,
       sum(b.unchanged) AS unchanged, sum(b.conflicts) AS conflicts, sum(b.rejects) AS rejects,
       sum(b.replayed) AS replayed
FROM audit.reverse_batch b
JOIN audit.cycle c USING (cycle_id)
GROUP BY b.cycle_id, c.started_at, b.target;

CREATE OR REPLACE VIEW audit.v_sla AS
SELECT r.cycle_id, c.started_at AS cycle_started_at, r.job_id, r.status, j.sla,
       extract(epoch FROM coalesce(r.ended_at, now()) - r.started_at)::double precision AS duration_s,
       extract(epoch FROM j.sla)::double precision AS sla_s,
       (coalesce(r.ended_at, now()) - r.started_at) > j.sla AS breached
FROM audit.v_latest_run r
JOIN audit.cycle c USING (cycle_id)
JOIN registry.job j USING (job_id)
WHERE r.status <> 'skipped';
