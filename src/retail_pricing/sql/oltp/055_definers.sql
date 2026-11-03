-- Least-privilege owners for the SECURITY DEFINER functions. oltp_owner is a superuser, so a
-- definer function it owns would run as superuser. Idempotent. NOLOGIN: nobody connects as these.
-- Identity columns need no sequence grant: INSERT on the table covers the identity default.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pricing_engine') THEN
        CREATE ROLE pricing_engine NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'price_history_writer') THEN
        CREATE ROLE price_history_writer NOLOGIN;
    END IF;
END $$;

-- pricing_engine: apply_batch (and the _apply_* helpers it calls), reconcile_actual and
-- reconcile_expected_applied (and _reconcile, which both call).
GRANT USAGE ON SCHEMA pricing, pricing_apply, master, ops TO pricing_engine;
GRANT SELECT ON master.store, master.product, master.product_class, ops.business_clock
    TO pricing_engine;
-- FOR UPDATE and the version check need SELECT plus UPDATE on the written columns.
GRANT SELECT, UPDATE (price, last_cycle_id) ON pricing.zone_price TO pricing_engine;
GRANT SELECT, INSERT, UPDATE (price, last_cycle_id) ON pricing.store_price_override
    TO pricing_engine;
GRANT SELECT, UPDATE (promo_price, last_cycle_id) ON pricing.promo_price TO pricing_engine;
-- SELECT because the inserts use RETURNING.
GRANT SELECT, INSERT ON pricing.price_change, pricing.shelf_label_queue TO pricing_engine;
GRANT SELECT, INSERT, UPDATE ON pricing_apply.zone_price_stage, pricing_apply.store_override_stage,
    pricing_apply.promo_price_stage, pricing_apply.shelf_label_stage, pricing_apply.batch,
    pricing_apply.conflict TO pricing_engine;
GRANT EXECUTE ON FUNCTION pricing_apply._apply_zone_price(bigint, int),
    pricing_apply._apply_store_override(bigint, int), pricing_apply._apply_promo_price(bigint, int),
    pricing_apply._apply_shelf_label(bigint, int),
    pricing_apply._reconcile(bigint, text, boolean) TO pricing_engine;
ALTER FUNCTION pricing_apply.apply_batch(bigint, text, int) OWNER TO pricing_engine;
ALTER FUNCTION pricing_apply.reconcile_actual(bigint, text) OWNER TO pricing_engine;
ALTER FUNCTION pricing_apply.reconcile_expected_applied(bigint, text) OWNER TO pricing_engine;

-- price_history_writer: the AFTER UPDATE trigger on zone_price.
GRANT USAGE ON SCHEMA pricing TO price_history_writer;
GRANT SELECT (product_id, zone_id, valid_to), INSERT, UPDATE (valid_to) ON pricing.price_history
    TO price_history_writer;
ALTER FUNCTION pricing.zone_price_history() OWNER TO price_history_writer;
