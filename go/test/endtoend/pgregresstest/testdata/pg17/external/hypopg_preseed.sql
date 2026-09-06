-- HypoPG test-helper function, seeded directly on the primary (bypassing
-- multigateway) before the hypopg pg_regress suite runs.
--
-- do_explain wraps EXPLAIN in a dynamic EXECUTE, so multigateway's Tier-1
-- PL/pgSQL body analysis rejects the test's own CREATE OR REPLACE (the single
-- honest divergence recorded in hypopg.patch). It is defined once, at the top of
-- test/sql/hypopg.sql, and the other five test files (hypo_brin, hypo_index_part,
-- hypo_include, hypo_hash, hypo_hide_index) call it without redefining it -- they
-- rely on it persisting in the database, since pg_regress runs the files
-- sequentially against the same database. Seeding it here on the primary (it
-- replicates to standbys via WAL, so every pooled backend sees it) lets all six
-- files' do_explain(...) calls resolve; only hypopg.sql's rejected CREATE remains.
--
-- Extracted verbatim from hypopg 1.4.2 (test/sql/hypopg.sql).
CREATE OR REPLACE FUNCTION do_explain(stmt text) RETURNS table(a text) AS
$_$
DECLARE
    ret text;
BEGIN
    FOR ret IN EXECUTE format('EXPLAIN (FORMAT text) %s', stmt) LOOP
        a := ret;
        RETURN next ;
    END LOOP;
END;
$_$
LANGUAGE plpgsql;
