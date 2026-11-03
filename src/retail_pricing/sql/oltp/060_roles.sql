-- Grants for the roles `oltp load` creates (PLAN 4.1). Placeholders are psycopg identifiers.
-- No role gets DELETE: there are no hard deletes.

GRANT CONNECT ON DATABASE {db} TO {workload}, {nifi}, {exporter};

GRANT USAGE ON SCHEMA master, inventory, pricing, market, rules, sales, ops TO {workload}, {nifi};
GRANT SELECT ON ALL TABLES IN SCHEMA master, inventory, pricing, market, rules, sales, ops
    TO {workload}, {nifi};

-- workload: sales, stock, manual pricing edits, cost rows, label printing, the business clock.
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA sales, inventory TO {workload};
GRANT UPDATE (price, effective_from) ON pricing.zone_price TO {workload};
-- No INSERT: the workload only edits open overrides; new ones come from the engine.
REVOKE INSERT ON pricing.store_price_override FROM {workload};
GRANT UPDATE (price, valid_to) ON pricing.store_price_override TO {workload};
GRANT INSERT ON pricing.supplier_cost TO {workload};
GRANT UPDATE (status) ON pricing.shelf_label_queue TO {workload};
GRANT UPDATE (business_date) ON ops.business_clock TO {workload};

-- nifi: extracts everything, writes only stage rows and goes through the three functions.
GRANT USAGE ON SCHEMA pricing_apply TO {nifi};
GRANT SELECT, INSERT ON pricing_apply.zone_price_stage, pricing_apply.store_override_stage,
    pricing_apply.promo_price_stage, pricing_apply.shelf_label_stage TO {nifi};
GRANT SELECT ON pricing_apply.batch, pricing_apply.conflict TO {nifi};
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pricing_apply FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pricing_apply.apply_batch(bigint, text, int),
    pricing_apply.reconcile_actual(bigint, text),
    pricing_apply.reconcile_expected_applied(bigint, text) TO {nifi};

GRANT pg_monitor TO {exporter};
