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
