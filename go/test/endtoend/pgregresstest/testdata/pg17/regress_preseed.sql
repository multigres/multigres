-- Regression-suite helper functions pre-seeded directly on the primary (bypassing
-- multigateway). Their runtime CREATE is rejected by the gateway's PL/pgSQL body
-- analysis (dynamic EXECUTE); seeding these benign scaffolding helpers lets the
-- substantive tests run instead of cascading to "function does not exist".
-- Extracted from the upstream regression sources; see regress_preseed.go.

-- alter_table.sql
CREATE OR REPLACE FUNCTION check_ddl_rewrite(p_tablename regclass, p_ddl text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    v_relfilenode oid;
BEGIN
    v_relfilenode := relfilenode FROM pg_class WHERE oid = p_tablename;

    EXECUTE p_ddl;

    RETURN v_relfilenode <> (SELECT relfilenode FROM pg_class WHERE oid = p_tablename);
END;
$$;

-- join_hash.sql
-- find_hash has no dynamic EXECUTE, so the gateway allows the test's own CREATE;
-- it is seeded first only so hash_join_batches' body resolves at preseed time.
-- hash_join_batches runs a dynamic EXPLAIN (rejected by the gateway), so it is
-- seeded here — its CREATE sits inside the test's explicit transaction, and now
-- that a gateway rejection no longer aborts that transaction, the seeded function
-- lets the batch's EXPLAIN checks run instead of cascading.
create or replace function find_hash(node json)
returns json language plpgsql
as
$$
declare
  x json;
  child json;
begin
  if node->>'Node Type' = 'Hash' then
    return node;
  else
    for child in select json_array_elements(node->'Plans')
    loop
      x := find_hash(child);
      if x is not null then
        return x;
      end if;
    end loop;
    return null;
  end if;
end;
$$;
create or replace function hash_join_batches(query text)
returns table (original int, final int) language plpgsql
as
$$
declare
  whole_plan json;
  hash_node json;
begin
  for whole_plan in
    execute 'explain (analyze, format ''json'') ' || query
  loop
    hash_node := find_hash(json_extract_path(whole_plan, '0', 'Plan'));
    original := hash_node->>'Original Hash Batches';
    final := hash_node->>'Hash Batches';
    return next;
  end loop;
end;
$$;

-- incremental_sort.sql
create or replace function explain_analyze_without_memory(query text)
returns table (out_line text) language plpgsql
as
$$
declare
  line text;
begin
  for line in
    execute 'explain (analyze, costs off, summary off, timing off) ' || query
  loop
    out_line := regexp_replace(line, '\d+kB', 'NNkB', 'g');
    return next;
  end loop;
end;
$$;
create or replace function explain_analyze_inc_sort_nodes(query text)
returns jsonb language plpgsql
as
$$
declare
  elements jsonb;
  element jsonb;
  matching_nodes jsonb := '[]'::jsonb;
begin
  execute 'explain (analyze, costs off, summary off, timing off, format ''json'') ' || query into strict elements;
  while jsonb_array_length(elements) > 0 loop
    element := elements->0;
    elements := elements - 0;
    case jsonb_typeof(element)
    when 'array' then
      if jsonb_array_length(element) > 0 then
        elements := elements || element;
      end if;
    when 'object' then
      if element ? 'Plan' then
        elements := elements || jsonb_build_array(element->'Plan');
        element := element - 'Plan';
      else
        if element ? 'Plans' then
          elements := elements || jsonb_build_array(element->'Plans');
          element := element - 'Plans';
        end if;
        if (element->>'Node Type')::text = 'Incremental Sort' then
          matching_nodes := matching_nodes || element;
        end if;
      end if;
    end case;
  end loop;
  return matching_nodes;
end;
$$;

-- interval.sql
CREATE OR REPLACE FUNCTION eval(expr text)
RETURNS text AS
$$
DECLARE
  result text;
BEGIN
  EXECUTE 'select '||expr INTO result;
  RETURN result;
EXCEPTION WHEN OTHERS THEN
  RETURN SQLERRM;
END
$$
LANGUAGE plpgsql;

-- memoize.sql
CREATE OR REPLACE FUNCTION explain_memoize(query text, hide_hitmiss bool) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in
        execute format('explain (analyze, costs off, summary off, timing off) %s',
            query)
    loop
        if hide_hitmiss = true then
                ln := regexp_replace(ln, 'Hits: 0', 'Hits: Zero');
                ln := regexp_replace(ln, 'Hits: \d+', 'Hits: N');
                ln := regexp_replace(ln, 'Misses: 0', 'Misses: Zero');
                ln := regexp_replace(ln, 'Misses: \d+', 'Misses: N');
        end if;
        ln := regexp_replace(ln, 'Evictions: 0', 'Evictions: Zero');
        ln := regexp_replace(ln, 'Evictions: \d+', 'Evictions: N');
        ln := regexp_replace(ln, 'Memory Usage: \d+', 'Memory Usage: N');
	ln := regexp_replace(ln, 'Heap Fetches: \d+', 'Heap Fetches: N');
	ln := regexp_replace(ln, 'loops=\d+', 'loops=N');
        return next ln;
    end loop;
end;
$$;

-- merge.sql
CREATE OR REPLACE FUNCTION explain_merge(query text) RETURNS SETOF text
LANGUAGE plpgsql AS
$$
DECLARE ln text;
BEGIN
    FOR ln IN
        EXECUTE 'explain (analyze, timing off, summary off, costs off) ' ||
		  query
    LOOP
        ln := regexp_replace(ln, '(Memory( Usage)?|Buckets|Batches): \S*',  '\1: xxx', 'g');
        RETURN NEXT ln;
    END LOOP;
END;
$$;

-- stats_ext.sql
CREATE OR REPLACE FUNCTION check_estimated_rows(text) returns table (estimated int, actual int)
language plpgsql as
$$
declare
    ln text;
    tmp text[];
    first_row bool := true;
begin
    for ln in
        execute format('explain analyze %s', $1)
    loop
        if first_row then
            first_row := false;
            tmp := regexp_match(ln, 'rows=(\d*) .* rows=(\d*)');
            return query select tmp[1]::int, tmp[2]::int;
        end if;
    end loop;
end;
$$;

-- triggers.sql
-- depth_b_tf runs a dynamic EXECUTE (rejected by the gateway); its body only
-- inserts into depth_c — no session-state change — so it is safe to seed.
-- Seeding lets "create trigger depth_b_tr" succeed and the trigger-depth
-- recursion test run. triggers.sql uses no SET SESSION AUTHORIZATION, so the
-- test's final DROP FUNCTION runs as the superuser and drops the seeded
-- function cleanly.
create or replace function depth_b_tf() returns trigger
  language plpgsql as $$
begin
  raise notice '%: depth = %', tg_name, pg_trigger_depth();
  begin
    execute 'insert into depth_c values (' || new.id::text || ')';
  exception
    when sqlstate 'U9999' then
      raise notice 'SQLSTATE = U9999: depth = %', pg_trigger_depth();
  end;
  raise notice '%: depth = %', tg_name, pg_trigger_depth();
  if new.id = 1 then
    execute 'insert into depth_c values (' || new.id::text || ')';
  end if;
  return new;
end;
$$;

-- explain.sql
-- explain_filter / explain_filter_to_json run a dynamic EXECUTE of the EXPLAIN
-- text passed in and filter it (regexp only) — no session-state change — so both
-- are safe to seed. They are used throughout explain.sql, so seeding avoids a
-- large "function does not exist" cascade.
create or replace function explain_filter(text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in execute $1
    loop
        -- Replace any numeric word with just 'N'
        ln := regexp_replace(ln, '-?\m\d+\M', 'N', 'g');
        -- In sort output, the above won't match units-suffixed numbers
        ln := regexp_replace(ln, '\m\d+kB', 'NkB', 'g');
        -- Ignore text-mode buffers output because it varies depending
        -- on the system state
        CONTINUE WHEN (ln ~ ' +Buffers: .*');
        -- Ignore text-mode "Planning:" line because whether it's output
        -- varies depending on the system state
        CONTINUE WHEN (ln = 'Planning:');
        return next ln;
    end loop;
end;
$$;
create or replace function explain_filter_to_json(text) returns jsonb
language plpgsql as
$$
declare
    data text := '';
    ln text;
begin
    for ln in execute $1
    loop
        -- Replace any numeric word with just '0'
        ln := regexp_replace(ln, '\m\d+\M', '0', 'g');
        data := data || ln;
    end loop;
    return data::jsonb;
end;
$$;

-- partition_prune.sql
-- explain_parallel_append runs a dynamic EXECUTE of an EXPLAIN and filters it
-- (regexp only) — no session-state change — so it is safe to seed. Used by many
-- parallel-append plan checks, so seeding avoids a "function does not exist"
-- cascade.
create or replace function explain_parallel_append(text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in
        execute format('explain (analyze, costs off, summary off, timing off) %s',
            $1)
    loop
        ln := regexp_replace(ln, 'Workers Launched: \d+', 'Workers Launched: N');
        ln := regexp_replace(ln, 'actual rows=\d+ loops=\d+', 'actual rows=N loops=N');
        ln := regexp_replace(ln, 'Rows Removed by Filter: \d+', 'Rows Removed by Filter: N');
        return next ln;
    end loop;
end;
$$;
