-- PostGIS *core* test-helper functions, re-seeded directly on the primary
-- (bypassing multigateway) BEFORE EACH test by the patched regress/run_test.pl.
-- PostGIS test files each CREATE OR REPLACE their own helpers (qnodes, etc.) --
-- rejected by the gateway's PL/pgSQL body analysis -- and DROP them at the end,
-- so a one-time pre-seed is dropped by the first test that runs. Re-seeding
-- before every test keeps them present. Extracted verbatim from postgis 3.6.3.
-- Schema-qualified helpers (tm, testc) pre-create their schema IF NOT EXISTS;
-- the test's own plain CREATE SCHEMA then reports a one-line "already exists"
-- divergence instead of a multi-thousand-line cascade.
-- qnodes
CREATE OR REPLACE FUNCTION qnodes(q text) RETURNS text
LANGUAGE 'plpgsql' AS
$$
DECLARE
  exp TEXT;
  mat TEXT[];
  ret TEXT[];
BEGIN
  FOR exp IN EXECUTE 'EXPLAIN ' || q
  LOOP
    --RAISE NOTICE 'EXP: %', exp;
    mat := regexp_matches(exp, ' *(?:-> *)?(.*Scan)');
    --RAISE NOTICE 'MAT: %', mat;
    IF mat IS NOT NULL THEN
      ret := array_append(ret, mat[1]);
    END IF;
    --RAISE NOTICE 'RET: %', ret;
  END LOOP;
  RETURN array_to_string(ret,',');
END;
$$;
-- catcherror
CREATE OR REPLACE FUNCTION catcherror(sql text)
RETURNS text
AS $$
BEGIN
  EXECUTE sql;
  RETURN 'NO ERROR';
EXCEPTION WHEN OTHERS THEN
  RETURN SQLERRM;
END;
$$
LANGUAGE 'plpgsql';
-- estimate_error
CREATE OR REPLACE FUNCTION estimate_error(qry text, tol int)
RETURNS text
LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  anl TEXT; -- analysis
  err INT; -- absolute difference between planned and actual rows
  est INT; -- estimated count
  act INT; -- actual count
  mat TEXT[];
BEGIN

  -- TODO: rewrite using json output ?
  EXECUTE 'EXPLAIN ANALYZE ' || qry INTO anl;

  SELECT regexp_matches(anl, E' rows=([0-9]*) .* rows=([0-9\.]*) ')
  INTO mat;

  est := mat[1];
  act := mat[2]::numeric::integer;

  err = abs(est-act);

  RETURN act || '+-' || tol || ':' || coalesce(
    nullif((err < tol)::text,'false'),
    'false:'||err::text
    );

END;
$$;

CREATE SCHEMA IF NOT EXISTS tm;
-- tm.insert_all
CREATE OR REPLACE FUNCTION tm.insert_all(tmpfile_prefix text)
RETURNS TABLE(out_where varchar, out_srid int, out_type varchar, out_flags varchar, out_status text)
AS
$$
DECLARE
	sql text;
	rec RECORD;
	rec2 RECORD;
	tmpfile text;
	cnt INT;
	hasgeog BOOL;
BEGIN

	tmpfile := tmpfile_prefix;

	FOR rec2 IN SELECT * from tm.types ORDER BY id
	LOOP
		tmpfile := tmpfile_prefix || rec2.id;
		sql := 'COPY ( SELECT g FROM tm.types WHERE id = ' || rec2.id || ') TO '
			|| quote_literal(tmpfile)
			|| ' WITH BINARY ';
		EXECUTE sql;
	END LOOP;

	FOR rec IN SELECT * FROM geometry_columns
		WHERE f_table_name != 'types'
		AND f_table_schema != 'tiger'
		ORDER BY 3
	LOOP
		out_where := rec.f_table_name;

		hasgeog := rec.type NOT LIKE '%CURVE%'
  			AND rec.type NOT LIKE '%CIRCULAR%'
  			AND rec.type NOT LIKE '%SURFACE%'
  			AND rec.type NOT LIKE 'TRIANGLE%'
  			AND rec.type NOT LIKE 'TIN%';

		FOR rec2 IN SELECT * from tm.types ORDER BY id
		LOOP
			out_srid := ST_Srid(rec2.g);
			out_type := substr(ST_GeometryType(rec2.g), 4);
			IF NOT ST_IsEmpty(rec2.g) THEN
				out_type := out_type || 'NE';
			END IF;
			out_flags := ST_zmflag(rec2.g);
			BEGIN
				sql := 'INSERT INTO '
					|| quote_ident(rec.f_table_schema)
					|| '.' || quote_ident(rec.f_table_name)
					|| '(g) VALUES ('
					|| quote_literal(rec2.g::text)
					|| ');';
				EXECUTE sql;
				out_status := 'OK';
			EXCEPTION
			WHEN OTHERS THEN
				out_status := 'KO'; -- || SQLERRM;
			END;

			-- binary insertion {
			tmpfile := tmpfile_prefix || rec2.id;
			sql := 'COPY '
				|| quote_ident(rec.f_table_schema)
				|| '.' || quote_ident(rec.f_table_name)
				|| '(g) FROM '
				|| quote_literal(tmpfile) || ' WITH BINARY ';
			BEGIN
				EXECUTE sql;
				out_status := out_status || '-BOK';
			EXCEPTION
			WHEN OTHERS THEN
				out_status := out_status || '-BKO';
			END;
			-- }

			IF NOT hasgeog THEN
				RETURN NEXT;
				CONTINUE;
			END IF;

			BEGIN
				sql := 'INSERT INTO '
					|| quote_ident(rec.f_table_schema)
					|| '.' || quote_ident(rec.f_table_name)
					|| '(gg) VALUES ('
					|| quote_literal(rec2.g::text)
					|| ');';
				EXECUTE sql;
				out_status := out_status || '-GOK';
			EXCEPTION
			WHEN OTHERS THEN
				out_status := out_status || '-GKO';
			END;

			-- binary insertion (geography) {
			sql := 'COPY '
				|| quote_ident(rec.f_table_schema)
				|| '.' || quote_ident(rec.f_table_name)
				|| '(gg) FROM '
				|| quote_literal(tmpfile) || ' WITH BINARY ';
			BEGIN
				EXECUTE sql;
				out_status := out_status || '-BGOK';
			EXCEPTION
			WHEN OTHERS THEN
				out_status := out_status || '-BGKO'; -- || SQLERRM;
			END;
			-- }

			RETURN NEXT;
		END LOOP;

		-- Count number of geometries in the table
		sql := 'SELECT count(g) FROM '
			|| quote_ident(rec.f_table_schema)
			|| '.' || quote_ident(rec.f_table_name);
		EXECUTE sql INTO STRICT cnt;

		out_srid := NULL;
		out_type := 'COUNT';
		out_flags := cnt;
		out_status := NULL;
		RETURN NEXT;

		IF hasgeog THEN
			-- Count number of geographies in the table
			sql := 'SELECT count(gg) FROM '
				|| quote_ident(rec.f_table_schema)
				|| '.' || quote_ident(rec.f_table_name);
			EXECUTE sql INTO STRICT cnt;

			out_srid := NULL;
			out_type := 'GCOUNT';
			out_flags := cnt;
			out_status := NULL;
			RETURN NEXT;
		END IF;

	END LOOP;
END;
$$ LANGUAGE 'plpgsql';

CREATE SCHEMA IF NOT EXISTS testc;
-- testc.compute_exection_time
CREATE OR REPLACE FUNCTION testc.compute_exection_time(param_sql text) RETURNS interval
AS $$
DECLARE var_start_time timestamptz; var_end_time timestamptz;
BEGIN
var_start_time = clock_timestamp();
EXECUTE param_sql;
var_end_time = clock_timestamp();
RETURN var_end_time - var_start_time;
END;
$$ language plpgsql;
