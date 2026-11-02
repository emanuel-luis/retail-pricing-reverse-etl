-- Staging, apply and reconciliation of engine prices (PLAN 4.1). Idempotent.
-- NiFi inserts stage rows (status 'staged'), then calls apply_batch once per batch.

CREATE TABLE IF NOT EXISTS pricing_apply.zone_price_stage (
    cycle_id bigint NOT NULL,
    product_id bigint NOT NULL,
    zone_id smallint NOT NULL,
    batch_no int NOT NULL,
    new_price numeric(10,2),
    old_price numeric(10,2),
    expected_version bigint NOT NULL,
    reason_code text,
    row_hash text,
    status text NOT NULL DEFAULT 'staged',
    status_detail text,
    processed_at timestamptz,
    PRIMARY KEY (cycle_id, product_id, zone_id)
);

-- expected_version 0 means "create a new open override".
CREATE TABLE IF NOT EXISTS pricing_apply.store_override_stage (
    cycle_id bigint NOT NULL,
    store_id int NOT NULL,
    product_id bigint NOT NULL,
    batch_no int NOT NULL,
    new_price numeric(10,2),
    old_price numeric(10,2),
    expected_version bigint NOT NULL,
    reason_code text,
    row_hash text,
    status text NOT NULL DEFAULT 'staged',
    status_detail text,
    processed_at timestamptz,
    PRIMARY KEY (cycle_id, store_id, product_id)
);

CREATE TABLE IF NOT EXISTS pricing_apply.promo_price_stage (
    cycle_id bigint NOT NULL,
    promo_id int NOT NULL,
    product_id bigint NOT NULL,
    zone_id smallint NOT NULL,
    batch_no int NOT NULL,
    new_price numeric(10,2),
    old_price numeric(10,2),
    expected_version bigint NOT NULL,
    reason_code text,
    row_hash text,
    status text NOT NULL DEFAULT 'staged',
    status_detail text,
    processed_at timestamptz,
    PRIMARY KEY (cycle_id, promo_id, product_id, zone_id)
);

-- source_target: 'zone_price' or 'store_override', the stage row the label depends on.
CREATE TABLE IF NOT EXISTS pricing_apply.shelf_label_stage (
    cycle_id bigint NOT NULL,
    store_id int NOT NULL,
    product_id bigint NOT NULL,
    batch_no int NOT NULL,
    price numeric(10,2),
    promo_price numeric(10,2),
    due_date date NOT NULL,
    source_target text NOT NULL,
    zone_id smallint NOT NULL,
    row_hash text,
    status text NOT NULL DEFAULT 'staged',
    status_detail text,
    processed_at timestamptz,
    PRIMARY KEY (cycle_id, store_id, product_id)
);

CREATE TABLE IF NOT EXISTS pricing_apply.batch (
    cycle_id bigint NOT NULL,
    target text NOT NULL,
    batch_no int NOT NULL,
    received int,
    applied int,
    unchanged int,
    conflicts int,
    rejects int,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cycle_id, target, batch_no)
);

CREATE TABLE IF NOT EXISTS pricing_apply.conflict (
    cycle_id bigint NOT NULL,
    target text NOT NULL,
    key_text text NOT NULL,
    expected_version bigint,
    actual_version bigint,
    proposed_price numeric(10,2),
    actual_price numeric(10,2),
    actual_updated_by text,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cycle_id, target, key_text)
);

CREATE INDEX IF NOT EXISTS zone_price_stage_batch_idx ON pricing_apply.zone_price_stage (cycle_id, batch_no);
CREATE INDEX IF NOT EXISTS store_override_stage_batch_idx ON pricing_apply.store_override_stage (cycle_id, batch_no);
CREATE INDEX IF NOT EXISTS promo_price_stage_batch_idx ON pricing_apply.promo_price_stage (cycle_id, batch_no);
CREATE INDEX IF NOT EXISTS shelf_label_stage_batch_idx ON pricing_apply.shelf_label_stage (cycle_id, batch_no);

-- Each _apply_<target> runs inside apply_batch (one transaction, actor pricing-engine) and
-- moves every stage row of the batch from 'staged' to rejected / unchanged / applied / conflict.

CREATE OR REPLACE FUNCTION pricing_apply._apply_zone_price(p_cycle_id bigint, p_batch_no int)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    -- Fixed lock order: no deadlock with single-row edits.
    PERFORM 1
       FROM pricing.zone_price t
       JOIN pricing_apply.zone_price_stage s USING (product_id, zone_id)
      WHERE s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no
      ORDER BY t.product_id, t.zone_id
        FOR UPDATE OF t;

    UPDATE pricing_apply.zone_price_stage s
       SET status = 'rejected', processed_at = now(),
           status_detail = CASE WHEN s.new_price IS NULL OR s.new_price <= 0
                                THEN 'price_not_positive' ELSE 'unknown_key' END
     WHERE s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no
       AND (s.new_price IS NULL OR s.new_price <= 0
            OR NOT EXISTS (SELECT 1 FROM pricing.zone_price t
                            WHERE t.product_id = s.product_id AND t.zone_id = s.zone_id));

    UPDATE pricing_apply.zone_price_stage s
       SET status = 'unchanged', processed_at = now()
      FROM pricing.zone_price t
     WHERE t.product_id = s.product_id AND t.zone_id = s.zone_id
       AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
       AND (t.last_cycle_id = p_cycle_id OR t.price = s.new_price);

    WITH upd AS (
        UPDATE pricing.zone_price t
           SET price = s.new_price, last_cycle_id = p_cycle_id
          FROM pricing_apply.zone_price_stage s
         WHERE t.product_id = s.product_id AND t.zone_id = s.zone_id
           AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
           AND t.version = s.expected_version AND t.price IS DISTINCT FROM s.new_price
        RETURNING t.product_id, t.zone_id, old.price AS old_price, new.price AS new_price,
                  s.reason_code
    ), chg AS (
        INSERT INTO pricing.price_change
               (cycle_id, target, product_id, zone_id, old_price, new_price, reason_code, applied_at)
        SELECT p_cycle_id, 'zone_price', product_id, zone_id, old_price, new_price, reason_code, now()
          FROM upd
        RETURNING product_id, zone_id
    )
    UPDATE pricing_apply.zone_price_stage s
       SET status = 'applied', processed_at = now()
      FROM chg
     WHERE s.cycle_id = p_cycle_id AND s.product_id = chg.product_id AND s.zone_id = chg.zone_id;

    WITH c AS (
        UPDATE pricing_apply.zone_price_stage s
           SET status = 'conflict', status_detail = 'version_changed', processed_at = now()
          FROM pricing.zone_price t
         WHERE t.product_id = s.product_id AND t.zone_id = s.zone_id
           AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
        RETURNING s.product_id, s.zone_id, s.expected_version, t.version, s.new_price, t.price,
                  t.updated_by
    )
    INSERT INTO pricing_apply.conflict (cycle_id, target, key_text, expected_version,
                                        actual_version, proposed_price, actual_price,
                                        actual_updated_by)
    SELECT p_cycle_id, 'zone_price', product_id || ':' || zone_id, expected_version, version,
           new_price, price, updated_by
      FROM c
    ON CONFLICT DO NOTHING;
END $$;

CREATE OR REPLACE FUNCTION pricing_apply._apply_store_override(p_cycle_id bigint, p_batch_no int)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    PERFORM 1
       FROM pricing.store_price_override t
       JOIN pricing_apply.store_override_stage s USING (store_id, product_id)
      WHERE s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND t.valid_to IS NULL
      ORDER BY t.store_id, t.product_id
        FOR UPDATE OF t;

    UPDATE pricing_apply.store_override_stage s
       SET status = 'rejected', processed_at = now(),
           status_detail = CASE WHEN s.new_price IS NULL OR s.new_price <= 0
                                THEN 'price_not_positive' ELSE 'unknown_key' END
     WHERE s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no
       AND (s.new_price IS NULL OR s.new_price <= 0
            OR NOT EXISTS (SELECT 1 FROM master.store m WHERE m.store_id = s.store_id)
            OR NOT EXISTS (SELECT 1 FROM master.product m WHERE m.product_id = s.product_id)
            OR (s.expected_version > 0
                AND NOT EXISTS (SELECT 1 FROM pricing.store_price_override t
                                 WHERE t.store_id = s.store_id AND t.product_id = s.product_id
                                   AND t.valid_to IS NULL)));

    UPDATE pricing_apply.store_override_stage s
       SET status = 'unchanged', processed_at = now()
      FROM pricing.store_price_override t
     WHERE t.store_id = s.store_id AND t.product_id = s.product_id AND t.valid_to IS NULL
       AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
       AND (t.last_cycle_id = p_cycle_id OR t.price = s.new_price);

    WITH upd AS (
        UPDATE pricing.store_price_override t
           SET price = s.new_price, last_cycle_id = p_cycle_id
          FROM pricing_apply.store_override_stage s
         WHERE t.store_id = s.store_id AND t.product_id = s.product_id AND t.valid_to IS NULL
           AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
           AND t.version = s.expected_version AND t.price IS DISTINCT FROM s.new_price
        RETURNING t.store_id, t.product_id, old.price AS old_price, new.price AS new_price,
                  s.reason_code
    ), ins AS (
        -- A lost race with another open override inserts nothing and ends as a conflict.
        INSERT INTO pricing.store_price_override
               (store_id, product_id, price, valid_from, last_cycle_id, updated_by)
        SELECT s.store_id, s.product_id, s.new_price,
               (SELECT business_date FROM ops.business_clock WHERE id = 1), p_cycle_id,
               'pricing-engine'
          FROM pricing_apply.store_override_stage s
         WHERE s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
           AND s.expected_version = 0
         ORDER BY s.store_id, s.product_id
        ON CONFLICT (store_id, product_id) WHERE valid_to IS NULL DO NOTHING
        RETURNING store_id, product_id, NULL::numeric AS old_price, price AS new_price
    ), done AS (
        SELECT store_id, product_id, old_price, new_price, reason_code FROM upd
        UNION ALL
        SELECT i.store_id, i.product_id, i.old_price, i.new_price, s.reason_code
          FROM ins i
          JOIN pricing_apply.store_override_stage s
            ON s.cycle_id = p_cycle_id AND s.store_id = i.store_id AND s.product_id = i.product_id
    ), chg AS (
        INSERT INTO pricing.price_change
               (cycle_id, target, product_id, store_id, old_price, new_price, reason_code, applied_at)
        SELECT p_cycle_id, 'store_override', product_id, store_id, old_price, new_price,
               reason_code, now()
          FROM done
        RETURNING store_id, product_id
    )
    UPDATE pricing_apply.store_override_stage s
       SET status = 'applied', processed_at = now()
      FROM chg
     WHERE s.cycle_id = p_cycle_id AND s.store_id = chg.store_id AND s.product_id = chg.product_id;

    WITH c AS (
        UPDATE pricing_apply.store_override_stage s
           SET status = 'conflict', processed_at = now(),
               status_detail = CASE WHEN s.expected_version = 0 THEN 'override_exists'
                                    ELSE 'version_changed' END
          FROM pricing.store_price_override t
         WHERE t.store_id = s.store_id AND t.product_id = s.product_id AND t.valid_to IS NULL
           AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
        RETURNING s.store_id, s.product_id, s.expected_version, t.version, s.new_price, t.price,
                  t.updated_by
    )
    INSERT INTO pricing_apply.conflict (cycle_id, target, key_text, expected_version,
                                        actual_version, proposed_price, actual_price,
                                        actual_updated_by)
    SELECT p_cycle_id, 'store_override', store_id || ':' || product_id, expected_version, version,
           new_price, price, updated_by
      FROM c
    ON CONFLICT DO NOTHING;
END $$;

CREATE OR REPLACE FUNCTION pricing_apply._apply_promo_price(p_cycle_id bigint, p_batch_no int)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    PERFORM 1
       FROM pricing.promo_price t
       JOIN pricing_apply.promo_price_stage s USING (promo_id, product_id, zone_id)
      WHERE s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no
      ORDER BY t.promo_id, t.product_id, t.zone_id
        FOR UPDATE OF t;

    UPDATE pricing_apply.promo_price_stage s
       SET status = 'rejected', processed_at = now(), status_detail = r.detail
      FROM (SELECT s2.promo_id, s2.product_id, s2.zone_id,
                   CASE WHEN s2.new_price IS NULL OR s2.new_price <= 0 THEN 'price_not_positive'
                        WHEN t.promo_id IS NULL THEN 'unknown_key'
                        WHEN s2.new_price >= z.price THEN 'promo_not_below_regular'
                   END AS detail
              FROM pricing_apply.promo_price_stage s2
              LEFT JOIN pricing.promo_price t USING (promo_id, product_id, zone_id)
              LEFT JOIN pricing.zone_price z USING (product_id, zone_id)
             WHERE s2.cycle_id = p_cycle_id AND s2.batch_no = p_batch_no) r
     WHERE s.cycle_id = p_cycle_id AND s.promo_id = r.promo_id AND s.product_id = r.product_id
       AND s.zone_id = r.zone_id AND r.detail IS NOT NULL;

    UPDATE pricing_apply.promo_price_stage s
       SET status = 'unchanged', processed_at = now()
      FROM pricing.promo_price t
     WHERE (t.promo_id, t.product_id, t.zone_id) = (s.promo_id, s.product_id, s.zone_id)
       AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
       AND (t.last_cycle_id = p_cycle_id OR t.promo_price = s.new_price);

    WITH upd AS (
        UPDATE pricing.promo_price t
           SET promo_price = s.new_price, last_cycle_id = p_cycle_id
          FROM pricing_apply.promo_price_stage s
         WHERE (t.promo_id, t.product_id, t.zone_id) = (s.promo_id, s.product_id, s.zone_id)
           AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
           AND t.version = s.expected_version AND t.promo_price IS DISTINCT FROM s.new_price
        RETURNING t.promo_id, t.product_id, t.zone_id, old.promo_price AS old_price,
                  new.promo_price AS new_price, s.reason_code
    ), chg AS (
        INSERT INTO pricing.price_change (cycle_id, target, product_id, zone_id, promo_id,
                                          old_price, new_price, reason_code, applied_at)
        SELECT p_cycle_id, 'promo_price', product_id, zone_id, promo_id, old_price, new_price,
               reason_code, now()
          FROM upd
        RETURNING promo_id, product_id, zone_id
    )
    UPDATE pricing_apply.promo_price_stage s
       SET status = 'applied', processed_at = now()
      FROM chg
     WHERE s.cycle_id = p_cycle_id
       AND (s.promo_id, s.product_id, s.zone_id) = (chg.promo_id, chg.product_id, chg.zone_id);

    WITH c AS (
        UPDATE pricing_apply.promo_price_stage s
           SET status = 'conflict', status_detail = 'version_changed', processed_at = now()
          FROM pricing.promo_price t
         WHERE (t.promo_id, t.product_id, t.zone_id) = (s.promo_id, s.product_id, s.zone_id)
           AND s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
        RETURNING s.promo_id, s.product_id, s.zone_id, s.expected_version, t.version,
                  s.new_price, t.promo_price, t.updated_by
    )
    INSERT INTO pricing_apply.conflict (cycle_id, target, key_text, expected_version,
                                        actual_version, proposed_price, actual_price,
                                        actual_updated_by)
    SELECT p_cycle_id, 'promo_price', promo_id || ':' || product_id || ':' || zone_id,
           expected_version, version, new_price, promo_price, updated_by
      FROM c
    ON CONFLICT DO NOTHING;
END $$;

-- A label is queued only when its price row was applied (or found already applied) this cycle.
CREATE OR REPLACE FUNCTION pricing_apply._apply_shelf_label(p_cycle_id bigint, p_batch_no int)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    UPDATE pricing_apply.shelf_label_stage s
       SET status = 'rejected', processed_at = now(), status_detail = r.detail
      FROM (SELECT s2.store_id, s2.product_id,
                   CASE WHEN s2.price IS NULL OR s2.price <= 0 THEN 'price_not_positive'
                        WHEN NOT EXISTS (SELECT 1 FROM master.store m WHERE m.store_id = s2.store_id)
                          OR NOT EXISTS (SELECT 1 FROM master.product m
                                          WHERE m.product_id = s2.product_id)
                        THEN 'unknown_key'
                        WHEN s2.source_target = 'zone_price' AND EXISTS (
                             SELECT 1 FROM pricing_apply.zone_price_stage z
                               JOIN pricing.zone_price t USING (product_id, zone_id)
                              WHERE z.cycle_id = s2.cycle_id AND z.product_id = s2.product_id
                                AND z.zone_id = s2.zone_id AND z.status IN ('applied', 'unchanged')
                                AND t.last_cycle_id = s2.cycle_id) THEN NULL
                        WHEN s2.source_target = 'store_override' AND EXISTS (
                             SELECT 1 FROM pricing_apply.store_override_stage o
                               JOIN pricing.store_price_override t
                                 ON t.store_id = o.store_id AND t.product_id = o.product_id
                                AND t.valid_to IS NULL
                              WHERE o.cycle_id = s2.cycle_id AND o.store_id = s2.store_id
                                AND o.product_id = s2.product_id
                                AND o.status IN ('applied', 'unchanged')
                                AND t.last_cycle_id = s2.cycle_id) THEN NULL
                        ELSE 'price_not_applied'
                   END AS detail
              FROM pricing_apply.shelf_label_stage s2
             WHERE s2.cycle_id = p_cycle_id AND s2.batch_no = p_batch_no) r
     WHERE s.cycle_id = p_cycle_id AND s.store_id = r.store_id AND s.product_id = r.product_id
       AND r.detail IS NOT NULL;

    WITH ins AS (
        INSERT INTO pricing.shelf_label_queue
               (cycle_id, store_id, product_id, price, promo_price, due_date, status)
        SELECT s.cycle_id, s.store_id, s.product_id, s.price, s.promo_price, s.due_date, 'queued'
          FROM pricing_apply.shelf_label_stage s
         WHERE s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged'
         ORDER BY s.store_id, s.product_id
        ON CONFLICT (cycle_id, store_id, product_id) DO NOTHING
        RETURNING store_id, product_id
    )
    UPDATE pricing_apply.shelf_label_stage s
       SET status = 'applied', processed_at = now()
      FROM ins
     WHERE s.cycle_id = p_cycle_id AND s.store_id = ins.store_id AND s.product_id = ins.product_id;

    -- Already queued by an earlier attempt.
    UPDATE pricing_apply.shelf_label_stage s
       SET status = 'unchanged', processed_at = now()
     WHERE s.cycle_id = p_cycle_id AND s.batch_no = p_batch_no AND s.status = 'staged';
END $$;

CREATE OR REPLACE FUNCTION pricing_apply.apply_batch(p_cycle_id bigint, p_target text, p_batch_no int)
RETURNS TABLE (target text, batch_no int, received int, applied int, unchanged int,
               conflicts int, rejects int, replayed boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
#variable_conflict use_column
DECLARE
    inserted int;
BEGIN
    IF p_target NOT IN ('zone_price', 'store_override', 'promo_price', 'shelf_label') THEN
        RAISE EXCEPTION 'unknown apply target %', p_target USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- The idempotency record: a second call for the same batch returns the stored counts.
    INSERT INTO pricing_apply.batch (cycle_id, target, batch_no)
    VALUES (p_cycle_id, p_target, p_batch_no)
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS inserted = ROW_COUNT;
    IF inserted = 0 THEN
        RETURN QUERY
            SELECT b.target, b.batch_no, b.received, b.applied, b.unchanged, b.conflicts,
                   b.rejects, true
              FROM pricing_apply.batch b
             WHERE b.cycle_id = p_cycle_id AND b.target = p_target AND b.batch_no = p_batch_no;
        RETURN;
    END IF;

    PERFORM set_config('app.actor', 'pricing-engine', true);
    EXECUTE format('SELECT pricing_apply.%I($1, $2)', '_apply_' || p_target)
      USING p_cycle_id, p_batch_no;

    RETURN QUERY EXECUTE format(
        $q$
        UPDATE pricing_apply.batch b
           SET received = c.received, applied = c.applied, unchanged = c.unchanged,
               conflicts = c.conflicts, rejects = c.rejects, processed_at = now()
          FROM (SELECT count(*)::int AS received,
                       (count(*) FILTER (WHERE status = 'applied'))::int AS applied,
                       (count(*) FILTER (WHERE status = 'unchanged'))::int AS unchanged,
                       (count(*) FILTER (WHERE status = 'conflict'))::int AS conflicts,
                       (count(*) FILTER (WHERE status = 'rejected'))::int AS rejects
                  FROM pricing_apply.%I
                 WHERE cycle_id = $1 AND batch_no = $3) c
         WHERE b.cycle_id = $1 AND b.target = $2 AND b.batch_no = $3
        RETURNING b.target, b.batch_no, b.received, b.applied, b.unchanged, b.conflicts,
                  b.rejects, false
        $q$, p_target || '_stage')
      USING p_cycle_id, p_target, p_batch_no;
END $$;

-- Checksums per (zone, category), two checks: 'transit' = every staged row of the cycle with
-- its proposed price; 'applied' = target rows with last_cycle_id = cycle whose stage row was
-- applied, with their current price. wsum = sum(((key1 * 1000003 + key2) % 1000000007) * cents).
-- Keys: zone_price (product_id, zone_id); store_override (store_id, product_id);
-- promo_price (promo_id, product_id * 100 + zone_id); shelf_label (store_id, product_id).
CREATE OR REPLACE FUNCTION pricing_apply.reconcile_actual(p_cycle_id bigint, p_target text)
RETURNS TABLE (target text, zone_id smallint, category_id int, check_name text, rows bigint,
               sum_cents numeric, wsum numeric)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    WITH checks (check_name, zone_id, product_id, k1, k2, price) AS (
        SELECT 'transit', s.zone_id, s.product_id, s.product_id, s.zone_id::bigint, s.new_price
          FROM pricing_apply.zone_price_stage s
         WHERE p_target = 'zone_price' AND s.cycle_id = p_cycle_id
        UNION ALL
        SELECT 'transit', m.zone_id, s.product_id, s.store_id, s.product_id, s.new_price
          FROM pricing_apply.store_override_stage s JOIN master.store m USING (store_id)
         WHERE p_target = 'store_override' AND s.cycle_id = p_cycle_id
        UNION ALL
        SELECT 'transit', s.zone_id, s.product_id, s.promo_id, s.product_id * 100 + s.zone_id,
               s.new_price
          FROM pricing_apply.promo_price_stage s
         WHERE p_target = 'promo_price' AND s.cycle_id = p_cycle_id
        UNION ALL
        SELECT 'transit', s.zone_id, s.product_id, s.store_id, s.product_id, s.price
          FROM pricing_apply.shelf_label_stage s
         WHERE p_target = 'shelf_label' AND s.cycle_id = p_cycle_id
        UNION ALL
        SELECT 'applied', t.zone_id, t.product_id, t.product_id, t.zone_id::bigint, t.price
          FROM pricing.zone_price t
          JOIN pricing_apply.zone_price_stage s
            ON s.cycle_id = p_cycle_id AND s.product_id = t.product_id AND s.zone_id = t.zone_id
           AND s.status = 'applied'
         WHERE p_target = 'zone_price' AND t.last_cycle_id = p_cycle_id
        UNION ALL
        SELECT 'applied', m.zone_id, t.product_id, t.store_id, t.product_id, t.price
          FROM pricing.store_price_override t
          JOIN master.store m USING (store_id)
          JOIN pricing_apply.store_override_stage s
            ON s.cycle_id = p_cycle_id AND s.store_id = t.store_id AND s.product_id = t.product_id
           AND s.status = 'applied'
         WHERE p_target = 'store_override' AND t.last_cycle_id = p_cycle_id AND t.valid_to IS NULL
        UNION ALL
        SELECT 'applied', t.zone_id, t.product_id, t.promo_id, t.product_id * 100 + t.zone_id,
               t.promo_price
          FROM pricing.promo_price t
          JOIN pricing_apply.promo_price_stage s
            ON s.cycle_id = p_cycle_id AND s.promo_id = t.promo_id
           AND s.product_id = t.product_id AND s.zone_id = t.zone_id AND s.status = 'applied'
         WHERE p_target = 'promo_price' AND t.last_cycle_id = p_cycle_id
        UNION ALL
        SELECT 'applied', s.zone_id, q.product_id, q.store_id, q.product_id, q.price
          FROM pricing.shelf_label_queue q
          JOIN pricing_apply.shelf_label_stage s
            ON s.cycle_id = q.cycle_id AND s.store_id = q.store_id AND s.product_id = q.product_id
           AND s.status = 'applied'
         WHERE p_target = 'shelf_label' AND q.cycle_id = p_cycle_id
    ), agg AS (
        SELECT c.check_name, c.zone_id, pc.category_id, count(*) AS n,
               sum(round(c.price * 100)) AS cents,
               sum(((c.k1::numeric * 1000003 + c.k2) % 1000000007) * round(c.price * 100)) AS w
          FROM checks c
          JOIN master.product p ON p.product_id = c.product_id
          JOIN master.product_class pc ON pc.class_id = p.class_id
         GROUP BY 1, 2, 3
    )
    SELECT p_target, g.zone_id, g.category_id, k.check_name, coalesce(a.n, 0),
           coalesce(a.cents, 0), coalesce(a.w, 0)
      FROM (SELECT DISTINCT zone_id, category_id FROM agg) g
     CROSS JOIN (VALUES ('transit'), ('applied')) k (check_name)
      LEFT JOIN agg a USING (zone_id, category_id, check_name)
     ORDER BY 2, 3, 4 DESC;
$$;
