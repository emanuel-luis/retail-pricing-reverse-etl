-- Row versioning on every table, pricing ownership and the zone price history (PLAN 4).
-- Idempotent: CREATE OR REPLACE everywhere.

CREATE OR REPLACE FUNCTION ops.touch_row() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.version := OLD.version + 1;
    NEW.updated_at := now();
    RETURN NEW;
END $$;

-- Every table with an updated_at column, outside the staging schema.
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT c.table_schema, c.table_name
        FROM information_schema.columns c
        JOIN information_schema.tables t USING (table_schema, table_name)
        WHERE c.column_name = 'updated_at' AND t.table_type = 'BASE TABLE'
          AND c.table_schema IN ('master', 'inventory', 'pricing', 'market', 'rules', 'sales', 'ops')
    LOOP
        EXECUTE format(
            'CREATE OR REPLACE TRIGGER touch_row BEFORE UPDATE ON %I.%I '
            'FOR EACH ROW EXECUTE FUNCTION ops.touch_row()', r.table_schema, r.table_name);
    END LOOP;
END $$;

-- Who changed an owned price; only the engine keeps last_cycle_id. The engine is recognized by
-- the role, not by app.actor: inside pricing_apply.apply_batch (SECURITY DEFINER, owned by the
-- NOLOGIN role pricing_engine) current_user is pricing_engine, and no login role can become it.
-- app.actor is a free setting, so a session that claims 'pricing-engine' without the role is
-- recorded as 'unknown' and loses last_cycle_id like any other edit. Fires on INSERT too, so a
-- new row cannot carry a forged cycle id either.
CREATE OR REPLACE FUNCTION pricing.track_price() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE actor text := coalesce(nullif(current_setting('app.actor', true), ''), 'unknown');
BEGIN
    IF current_user = 'pricing_engine' THEN
        NEW.updated_by := 'pricing-engine';
    ELSE
        NEW.updated_by := CASE WHEN actor = 'pricing-engine' THEN 'unknown' ELSE actor END;
        NEW.last_cycle_id := NULL;
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE TRIGGER track_price BEFORE INSERT OR UPDATE ON pricing.zone_price
    FOR EACH ROW EXECUTE FUNCTION pricing.track_price();
CREATE OR REPLACE TRIGGER track_price BEFORE INSERT OR UPDATE ON pricing.store_price_override
    FOR EACH ROW EXECUTE FUNCTION pricing.track_price();
CREATE OR REPLACE TRIGGER track_price BEFORE INSERT OR UPDATE ON pricing.promo_price
    FOR EACH ROW EXECUTE FUNCTION pricing.track_price();

-- SECURITY DEFINER: the workload role edits zone prices but has no rights on price_history.
CREATE OR REPLACE FUNCTION pricing.zone_price_history() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    UPDATE pricing.price_history
       SET valid_to = now()
     WHERE product_id = NEW.product_id AND zone_id = NEW.zone_id AND valid_to IS NULL;
    INSERT INTO pricing.price_history (product_id, zone_id, price, valid_from, source, cycle_id)
    VALUES (NEW.product_id, NEW.zone_id, NEW.price, now(),
            CASE WHEN NEW.updated_by = 'pricing-engine' THEN 'engine' ELSE 'manual' END,
            NEW.last_cycle_id);
    RETURN NULL;
END $$;

CREATE OR REPLACE TRIGGER zone_price_history AFTER UPDATE ON pricing.zone_price
    FOR EACH ROW WHEN (OLD.price IS DISTINCT FROM NEW.price)
    EXECUTE FUNCTION pricing.zone_price_history();
