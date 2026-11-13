-- Run as the PostgreSQL administrator in each source database.
BEGIN;
CREATE SCHEMA IF NOT EXISTS graphlake_tracking;
REVOKE CREATE ON SCHEMA graphlake_tracking FROM PUBLIC;

CREATE TABLE IF NOT EXISTS graphlake_tracking.relation_versions (
    relation oid PRIMARY KEY,
    generation bigint NOT NULL DEFAULT 0
);
REVOKE ALL ON graphlake_tracking.relation_versions FROM PUBLIC;

-- Each writing transaction owns a distinct row. The read-side count is MVCC
-- consistent even when transactions commit out of order.
CREATE TABLE IF NOT EXISTS graphlake_tracking.relation_changes (
    relation oid NOT NULL,
    transaction_id xid8 NOT NULL,
    PRIMARY KEY (relation, transaction_id)
);
REVOKE ALL ON graphlake_tracking.relation_changes FROM PUBLIC;

-- Preserve the function OID when upgrading: existing triggers switch to the
-- journal without acquiring a DDL lock on every watched source table.
DO $$
BEGIN
    IF to_regprocedure('graphlake_tracking.record_relation_change()') IS NULL
       AND to_regprocedure('graphlake_tracking.bump_relation_version()') IS NOT NULL THEN
        ALTER FUNCTION graphlake_tracking.bump_relation_version() RENAME TO record_relation_change;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION graphlake_tracking.record_relation_change()
RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
AS $$
BEGIN
    INSERT INTO graphlake_tracking.relation_changes(relation, transaction_id)
        VALUES (TG_RELID, pg_current_xact_id())
        ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION graphlake_tracking.record_relation_change() FROM PUBLIC;

CREATE OR REPLACE FUNCTION graphlake_tracking.watch(target regclass)
RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog
AS $$
BEGIN
    EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE', target);
    IF NOT EXISTS (
        SELECT FROM pg_class c JOIN pg_am am ON am.oid = c.relam
        WHERE c.oid = target AND c.relkind = 'r' AND am.amname = 'heap'
        AND c.relpersistence = 'p' AND NOT c.relrowsecurity
        AND NOT EXISTS (SELECT FROM pg_inherits WHERE target IN (inhrelid, inhparent))
    ) THEN
        RAISE EXCEPTION 'GraphLake tracking requires a permanent standalone table without row security';
    END IF;
    INSERT INTO graphlake_tracking.relation_versions(relation) VALUES (target)
    ON CONFLICT (relation) DO UPDATE SET generation = relation_versions.generation + 1;
    EXECUTE format('DROP TRIGGER IF EXISTS graphlake_relation_version ON %s', target);
    EXECUTE format(
        'CREATE TRIGGER graphlake_relation_version AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s
         FOR EACH STATEMENT EXECUTE FUNCTION graphlake_tracking.record_relation_change()', target);
    EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER graphlake_relation_version', target);
END;
$$;
REVOKE ALL ON FUNCTION graphlake_tracking.watch(regclass) FROM PUBLIC;
-- Fold visible journal entries into the base count without blocking source
-- writes. The deletion and base update are visible in the same snapshot.
CREATE OR REPLACE FUNCTION graphlake_tracking.compact(target regclass)
RETURNS bigint
LANGUAGE sql SET search_path = pg_catalog
AS $$
    WITH removed AS (
        DELETE FROM graphlake_tracking.relation_changes
        WHERE relation = target AND EXISTS (
            SELECT FROM graphlake_tracking.relation_versions WHERE relation = target
        )
        RETURNING 1
    )
    UPDATE graphlake_tracking.relation_versions
    SET generation = generation + (SELECT count(*) FROM removed)
    WHERE relation = target
    RETURNING generation
$$;
REVOKE ALL ON FUNCTION graphlake_tracking.compact(regclass) FROM PUBLIC;
COMMIT;

-- For each mapped standalone table:
-- SELECT graphlake_tracking.watch('public.accounts');
-- GRANT USAGE ON SCHEMA graphlake_tracking TO graphlake_reader;
-- GRANT SELECT ON graphlake_tracking.relation_versions, graphlake_tracking.relation_changes TO graphlake_reader;
-- Run periodically for busy tables, in a separate transaction from source writes:
-- SELECT graphlake_tracking.compact('public.accounts');
