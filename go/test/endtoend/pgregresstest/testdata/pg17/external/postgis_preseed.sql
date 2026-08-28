-- PostGIS test-helper functions, re-seeded directly on the primary (bypassing
-- multigateway) BEFORE EACH test by the patched regress/run_test.pl. PostGIS test
-- files each CREATE OR REPLACE their own helpers -- rejected by the gateway's
-- PL/pgSQL body analysis -- and DROP them at the end, so re-seeding before every
-- test keeps them present. Extracted verbatim from postgis 3.6.3.
--
-- Schema-qualified helpers (tm, testc) pre-create their schema IF NOT EXISTS.
-- Only single-variant helpers are seeded. Helpers redefined across files with
-- conflicting bodies (runTest) or with overloaded / default-argument signatures
-- that would make a bare call ambiguous (check_changes, make_test_raster) cannot
-- be seeded under their original name -- seeding every variant yields "function is
-- not unique". Where only one file's copy is actually rejected, that file's calls
-- are renamed per-file (by patchPostGISConflictingHelpers) to a unique name and
-- the renamed variant is seeded below; the rest stay as accepted cascade patches.

-- === core ===
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

-- === topology ===
-- check_nodes
CREATE OR REPLACE FUNCTION check_nodes(lbl text)
RETURNS TABLE (l text, o text, node_id int8,
    containing_face int8)
AS $$
DECLARE
  sql1 text;
  sql2 text;
  q text;
BEGIN
  sql1 := 'node_id,
      containing_face
  		FROM city_data.node';
  sql2 := 'node_id, containing_face
  		FROM orig_node_summary';

  q := format(
    $SQL$
      (
          SELECT %1$L, '+' as op, %2$s
            EXCEPT
          SELECT %1$L, '+', %3$s
      ) UNION ALL (
          SELECT %1$L, '-', %3$s
            EXCEPT
          SELECT %1$L, '-', %2$s
      )
      ORDER BY node_id, op
    $SQL$,
    lbl,
    sql1,
    sql2
  );

  RAISE DEBUG '%', q;

  RETURN QUERY EXECUTE q;

END
$$ LANGUAGE 'plpgsql';
-- check_faces
CREATE OR REPLACE FUNCTION check_faces(lbl text)
RETURNS TABLE (l text, o text, face_id int8, mbr text)
AS $$
DECLARE
  sql1 text;
  sql2 text;
  q text;
BEGIN
  sql1 := 'face_id, ST_AsEWKT(mbr) FROM city_data.face';
  sql2 := 'face_id, ST_AsEWKT(mbr) FROM orig_face_summary';

  q := format(
    $SQL$
      (
          SELECT %1$L, '+' as op, %2$s
            EXCEPT
          SELECT %1$L, '+', %3$s
      ) UNION ALL (
          SELECT %1$L, '-', %3$s
            EXCEPT
          SELECT %1$L, '-', %2$s
      )
      ORDER BY face_id, op
    $SQL$,
    lbl,
    sql1,
    sql2
  );

  RAISE DEBUG '%', q;

  RETURN QUERY EXECUTE q;

END
$$ language 'plpgsql';
-- check_edges
CREATE OR REPLACE FUNCTION check_edges(lbl text)
RETURNS TABLE (l text, o text, edge_id int8,
    next_left_edge int8, next_right_edge int8,
    left_face int8, right_face int8)
AS $$
DECLARE
  rec RECORD;
  sql1 text;
  sql2 text;
  q text;
BEGIN
  sql1 := 'edge_id,
      next_left_edge, next_right_edge, left_face, right_face
  		FROM city_data.edge_data';
  sql2 := 'edge_id,
  		next_left_edge, next_right_edge, left_face, right_face
  		FROM orig_edge_summary';

  q := format(
    $SQL$
      (
          SELECT %1$L, '+' as op, %2$s
            EXCEPT
          SELECT %1$L, '+', %3$s
      ) UNION ALL (
          SELECT %1$L, '-', %3$s
            EXCEPT
          SELECT %1$L, '-', %2$s
      )
      ORDER BY edge_id, op
    $SQL$,
    lbl,
    sql1,
    sql2
  );

  RAISE DEBUG '%', q;

  RETURN QUERY EXECUTE q;

END
$$ LANGUAGE 'plpgsql';
-- print_elements_count
CREATE OR REPLACE FUNCTION print_elements_count(lbl text)
 RETURNS table(olbl text, nodes text, edges text, faces text)
AS $$
DECLARE
 sql text;
BEGIN
  sql := 'select ' || quote_literal(lbl) || '::text,
       ( select count(node_id) || '' nodes'' from t.node ) as nodes,
       ( select count(edge_id) || '' edges'' from t.edge ) as edges,
       ( select count(face_id) || '' faces'' from t.face
                                    where face_id <> 0 ) as faces';
  RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE 'plpgsql';
-- print_isolated_nodes
CREATE OR REPLACE FUNCTION print_isolated_nodes(lbl text)
 RETURNS table(olbl text, msg text)
AS $$
DECLARE
 sql text;
BEGIN
  sql := 'SELECT ' || quote_literal(lbl) || '::text, count(node_id)
    || '' isolated nodes in face '' || containing_face
    FROM t.node WHERE containing_face IS NOT NULL GROUP by containing_face
    ORDER BY count(node_id), containing_face';
  RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE 'plpgsql';
-- catch_error
CREATE OR REPLACE FUNCTION catch_error(query text)
RETURNS bool
AS $$
BEGIN
    EXECUTE query;
    RETURN FALSE;
EXCEPTION
    WHEN OTHERS THEN
        RETURN TRUE;
END
$$ LANGUAGE 'plpgsql';

-- === raster (requires postgis_raster) ===
-- make_value_array
CREATE OR REPLACE FUNCTION make_value_array(
	rows integer DEFAULT 3,
	columns integer DEFAULT 3,
	start_val double precision DEFAULT 1,
	step double precision DEFAULT 1,
	skip_expr text DEFAULT NULL
)
	RETURNS double precision[][][]
	AS $$
	DECLARE
		x int;
		y int;
		value double precision;
		values double precision[][][];
		result boolean;
		expr text;
	BEGIN
		value := start_val;

		values := array_fill(NULL::double precision, ARRAY[1, columns, rows]);

		FOR y IN 1..columns LOOP
			FOR x IN 1..rows LOOP
				IF skip_expr IS NULL OR length(skip_expr) < 1 THEN
					result := TRUE;
				ELSE
					expr := replace(skip_expr, '[v]'::text, value::text);
					EXECUTE 'SELECT (' || expr || ')::boolean' INTO result;
				END IF;

				IF result IS TRUE THEN
					values[1][y][x] := value;
				END IF;

				value := value + step;
			END LOOP;
		END LOOP;

		RETURN values;
	END;
	$$ LANGUAGE 'plpgsql';
-- make_raster
CREATE OR REPLACE FUNCTION make_raster(
	rast raster DEFAULT NULL,
	pixtype text DEFAULT '8BUI',
	rows integer DEFAULT 3,
	columns integer DEFAULT 3,
	nodataval double precision DEFAULT 0,
	start_val double precision DEFAULT 1,
	step double precision DEFAULT 1,
	skip_expr text DEFAULT NULL
)
	RETURNS raster
	AS $$
	DECLARE
		x int;
		y int;
		value double precision;
		values double precision[][][];
		result boolean;
		expr text;
		_rast raster;
		nband int;
	BEGIN
		IF rast IS NULL THEN
			nband := 1;
			_rast := ST_AddBand(ST_MakeEmptyRaster(columns, rows, 0, 0, 1, -1, 0, 0, 0), nband, pixtype, 0, nodataval);
		ELSE
			nband := ST_NumBands(rast) + 1;
			_rast := ST_AddBand(rast, nband, pixtype, 0, nodataval);
		END IF;

		value := start_val;
		values := array_fill(NULL::double precision, ARRAY[columns, rows]);

		FOR y IN 1..columns LOOP
			FOR x IN 1..rows LOOP
				IF skip_expr IS NULL OR length(skip_expr) < 1 THEN
					result := TRUE;
				ELSE
					expr := replace(skip_expr, '[v]'::text, value::text);
					EXECUTE 'SELECT (' || expr || ')::boolean' INTO result;
				END IF;

				IF result IS TRUE THEN
					values[y][x] := value;
				END IF;

				value := value + step;
			END LOOP;
		END LOOP;

		_rast := ST_SetValues(_rast, nband, 1, 1, values);
		RETURN _rast;
	END;
	$$ LANGUAGE 'plpgsql';

-- === topology: conflicting-overload helpers, renamed per file to avoid
-- "function is not unique" ambiguity (the same rename is applied to the test
-- .sql by patchPostGISConflictingHelpers before run_test.pl reads it) ===
CREATE OR REPLACE FUNCTION check_changes()
RETURNS TABLE (o text)
AS $$
DECLARE
  rec RECORD;
  sql text;
BEGIN
  -- Check effect on nodes
  sql := 'SELECT n.node_id, ''N|'' || n.node_id || ''|'' ||
        COALESCE(n.containing_face::text,'''') || ''|'' ||
        ST_AsText(ST_SnapToGrid(n.geom, 0.2))::text as xx
  	FROM city_data.node n WHERE n.node_id > (
    		SELECT max FROM city_data.limits WHERE what = ''node''::text )
  		ORDER BY n.node_id';

  FOR rec IN EXECUTE sql LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  -- Check effect on edges (there should be one split)
  sql := '
  WITH node_limits AS ( SELECT max FROM city_data.limits WHERE what = ''node''::text ),
       edge_limits AS ( SELECT max FROM city_data.limits WHERE what = ''edge''::text )
  SELECT ''E|'' || e.edge_id || ''|sn'' || e.start_node || ''|en'' || e.end_node
         || ''|nl'' || e.next_left_edge
         || ''|nr'' || e.next_right_edge
         || ''|lf'' || e.left_face
         || ''|rf'' || e.right_face
         :: text as xx
   FROM city_data.edge e, node_limits nl, edge_limits el
   WHERE e.start_node > nl.max
      OR e.end_node > nl.max
      OR e.edge_id > el.max
  ORDER BY e.edge_id;
  ';

  FOR rec IN EXECUTE sql LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  UPDATE city_data.limits SET max = (SELECT max(n.node_id) FROM city_data.node n) WHERE what = 'node';
  UPDATE city_data.limits SET max = (SELECT max(e.edge_id) FROM city_data.edge e) WHERE what = 'edge';

END;
$$ LANGUAGE 'plpgsql';
CREATE OR REPLACE FUNCTION check_changes(lbl text)
RETURNS TABLE (o text)
AS $$
DECLARE
  rec RECORD;
  sql text;
BEGIN
  -- Check effect on nodes
  sql :=  'SELECT $1 || ''|N|'' ||
        COALESCE(n.containing_face::text,'''') || ''|'' ||
        ST_AsText(n.geom, 2)::text as xx
  	FROM city_data.node n WHERE n.node_id > (
    		SELECT max FROM city_data.limits WHERE what = ''node''::text )
  		ORDER BY n.geom';

  FOR rec IN EXECUTE sql USING ( lbl )
  LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  -- Check effect on edges
  sql := 'WITH node_limits AS ( SELECT max FROM city_data.limits WHERE what = ''node''::text ),
       edge_limits AS ( SELECT max FROM city_data.limits WHERE what = ''edge''::text )
  SELECT $1 || ''|E|'' || ST_AsText(e.geom, 2)::text AS xx ' ||
   ' FROM city_data.edge e, node_limits nl, edge_limits el
   WHERE e.start_node > nl.max
      OR e.end_node > nl.max
      OR e.edge_id > el.max
  ORDER BY e.geom;';

  FOR rec IN EXECUTE sql USING ( lbl )
  LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  UPDATE city_data.limits SET max = (SELECT max(n.node_id) FROM city_data.node n) WHERE what = 'node';
  UPDATE city_data.limits SET max = (SELECT max(e.edge_id) FROM city_data.edge e) WHERE what = 'edge';

END;
$$ LANGUAGE 'plpgsql';
CREATE OR REPLACE FUNCTION check_changes_ap(lbl text, add_id boolean default true)
RETURNS TABLE (o text)
AS $$
DECLARE
  rec RECORD;
  sql text;
BEGIN
  -- Check effect on nodes
  sql :=  'SELECT $1 || ''|N|'' ' || CASE WHEN add_id THEN ' || n.node_id || ''|'' ' ELSE '' END || ' ||
        COALESCE(n.containing_face::text,'''') || ''|'' ||
        ST_AsText(ST_SnapToGrid(n.geom, 0.2))::text as xx
  	FROM city_data.node n WHERE n.node_id > (
    		SELECT max FROM city_data.limits WHERE what = ''node''::text )
  		ORDER BY n.node_id';

  FOR rec IN EXECUTE sql USING ( lbl )
  LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  -- Check effect on edges (there should be one split)
  sql := 'WITH node_limits AS ( SELECT max FROM city_data.limits WHERE what = ''node''::text ),
       edge_limits AS ( SELECT max FROM city_data.limits WHERE what = ''edge''::text )
  SELECT $1 || ''|E|'' ' || CASE WHEN add_id THEN ' || e.edge_id || ''|sn'' || e.start_node || ''|en'' || e.end_node::text ' ELSE '' END || ' AS xx ' ||
   ' FROM city_data.edge e, node_limits nl, edge_limits el
   WHERE e.start_node > nl.max
      OR e.end_node > nl.max
      OR e.edge_id > el.max
  ORDER BY e.edge_id;';

  FOR rec IN EXECUTE sql USING ( lbl )
  LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  -- Check effect on faces
  sql := 'WITH face_limits AS ( SELECT max FROM city_data.limits WHERE what = ''face''::text )
  SELECT $1 || ''|F|'' ' || CASE WHEN add_id THEN ' || f.face_id ::text ' ELSE '' END || ' as xx
   FROM city_data.face f, face_limits fl
   WHERE f.face_id > fl.max
  ORDER BY f.face_id;';

  FOR rec IN EXECUTE sql USING ( lbl ) LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  UPDATE city_data.limits SET max = (SELECT max(n.node_id) FROM city_data.node n) WHERE what = 'node';
  UPDATE city_data.limits SET max = (SELECT max(e.edge_id) FROM city_data.edge e) WHERE what = 'edge';
  UPDATE city_data.limits SET max = (SELECT max(f.face_id) FROM city_data.face f) WHERE what = 'face';

END;
$$ LANGUAGE 'plpgsql';
CREATE OR REPLACE FUNCTION runtest_alr( lbl text, lines geometry[], newline geometry, prec float8, debug bool default false )
RETURNS SETOF text AS
$BODY$
DECLARE
  g geometry;
  n int := 0;
  rec record;
BEGIN
  IF EXISTS ( SELECT * FROM topology.topology WHERE name = 'topo' )
  THEN
    PERFORM topology.DropTopology ('topo');
  END IF;

  PERFORM topology.CreateTopology ('topo');
  CREATE TABLE topo.fl(lbl text, g geometry);
  PERFORM topology.AddTopoGeometryColumn('topo','topo','fl','tg','LINESTRING');
  CREATE TABLE topo.fa(lbl text, g geometry);
  PERFORM topology.AddTopoGeometryColumn('topo','topo','fa','tg','POLYGON');

  -- Add a polygon containing all lines
  PERFORM topology.TopoGeo_addPolygon('topo', ST_Expand(ST_Extent(geom), 100))
  FROM unnest(lines) geom;

  -- Add all lines
  FOR g IN SELECT unnest(lines)
  LOOP
    INSERT INTO topo.fl(lbl, tg) VALUES
      ( 'l'||n, topology.toTopoGeom(g, 'topo', 1) );
    n = n+1;
  END LOOP;

  FOR n IN SELECT face_id FROM topo.face WHERE face_id > 0
  LOOP
    INSERT INTO topo.fa(lbl, tg) VALUES
      ( 'a'||n, topology.CreateTopoGeom('topo', 3, 2, ARRAY[ARRAY[n,3]]) );
    n = n+1;
  END LOOP;

  UPDATE topo.fl SET g = tg::geometry;
  UPDATE topo.fa SET g = tg::geometry;

  RETURN QUERY SELECT  array_to_string(ARRAY[
    lbl,
    '-checking-'
  ], '|');

  IF debug THEN
    RETURN QUERY SELECT array_to_string(ARRAY[
      lbl, -- 'topo',
      'bfr',
      'E' || edge_id,
      'next_left:'||next_left_edge,
      'next_right:'||next_right_edge,
      'face_left:'||left_face,
      'face_right:'||right_face
    ], '|')
    FROM topo.edge
    ORDER BY edge_id;
  END IF;

  IF debug THEN
    set client_min_messages to DEBUG;
  END IF;

  BEGIN
    PERFORM topology.TopoGeo_addLinestring('topo', newline, prec);
  EXCEPTION WHEN OTHERS THEN
    RETURN QUERY SELECT format('%s|addline exception|%s (%s)', lbl, SQLERRM, SQLSTATE);
  END;

  IF debug THEN
    set client_min_messages to WARNING;
  END IF;

  IF debug THEN
    RETURN QUERY SELECT array_to_string(ARRAY[
      lbl, --'topo',
      'aft',
      'E' || edge_id,
      'next_left:'||next_left_edge,
      'next_right:'||next_right_edge,
      'face_left:'||left_face,
      'face_right:'||right_face
    ], '|')
    FROM topo.edge
    ORDER BY edge_id;
  END IF;

  RETURN QUERY
    WITH j AS (
      SELECT
        row_number() over () as rn,
        to_json(s) as o
      FROM ValidateTopology('topo') s
    )
    SELECT
      array_to_string(
        ARRAY[lbl,'unexpected','validity issue'] ||
        array_agg(x.value order by x.ordinality),
        '|'
      )
    FROM j, json_each_text(j.o)
    WITH ordinality AS x
    GROUP by j.rn;


  RETURN QUERY SELECT array_to_string(ARRAY[
    lbl,
    'unexpected',
    'lineal drift',
    l,
    dist::text
  ], '|')
  FROM (
    SELECT t.lbl l, ST_HausdorffDistance(t.g, tg::geometry) dist
    FROM topo.fl t
  ) foo WHERE dist >= COALESCE(
      NULLIF(prec,0),
      topology._st_mintolerance(newline)
  )
  ORDER BY foo.l;

  SELECT sum(ST_Area(t.g)) as before, sum(ST_Area(tg::geometry)) as after
  FROM topo.fa t
  INTO rec;

  IF rec.before != rec.after THEN
    RETURN QUERY SELECT array_to_string(ARRAY[
      lbl,
      'unexpected total area change',
      rec.before::text,
      rec.after::text
    ], '|')
    ;

    RETURN QUERY SELECT array_to_string(ARRAY[
      lbl,
      'area change',
      l,
      bfr::text,
      aft::text
    ], '|')
    FROM (
      SELECT t.lbl l, ST_Area(t.g) bfr, ST_Area(tg::geometry) aft
      FROM topo.fa t
    ) foo WHERE bfr != aft
    ORDER BY foo.l;

  END IF;

  IF NOT debug THEN
    PERFORM topology.DropTopology ('topo');
  END IF;

END;
$BODY$
LANGUAGE 'plpgsql';
CREATE OR REPLACE FUNCTION runtest_apme( lbl text, lines geometry[], point geometry, prec float8, debug bool default false )
RETURNS SETOF text AS
$BODY$
DECLARE
  g geometry;
  n int := 0;
  rec record;
BEGIN
  IF EXISTS ( SELECT * FROM topology.topology WHERE name = 'topo' )
  THEN
    PERFORM topology.DropTopology ('topo');
  END IF;

  PERFORM topology.CreateTopology ('topo');
  CREATE TABLE topo.fl(lbl text, g geometry);
  PERFORM topology.AddTopoGeometryColumn('topo','topo','fl','tg','LINESTRING');
  CREATE TABLE topo.fa(lbl text, g geometry);
  PERFORM topology.AddTopoGeometryColumn('topo','topo','fa','tg','POLYGON');

  -- Add a polygon containing all lines
  PERFORM topology.TopoGeo_addPolygon('topo', ST_Expand(ST_Extent(geom), 100))
  FROM unnest(lines) geom;

  -- Add all lines
  FOR g IN SELECT unnest(lines)
  LOOP
    INSERT INTO topo.fl(lbl, tg) VALUES
      ( 'l'||n, topology.toTopoGeom(g, 'topo', 1) );
    n = n+1;
  END LOOP;

  FOR n IN SELECT face_id FROM topo.face WHERE face_id > 0
  LOOP
    INSERT INTO topo.fa(lbl, tg) VALUES
      ( 'a'||n, topology.CreateTopoGeom('topo', 3, 2, ARRAY[ARRAY[n,3]]) );
    n = n+1;
  END LOOP;

  UPDATE topo.fl SET g = tg::geometry;
  UPDATE topo.fa SET g = tg::geometry;

  RETURN QUERY SELECT  array_to_string(ARRAY[
    lbl,
    '-checking-'
  ], '|');

  IF debug THEN
    RETURN QUERY SELECT array_to_string(ARRAY[
      lbl, -- 'topo',
      'bfr',
      'E' || edge_id,
      'next_left:'||next_left_edge,
      'next_right:'||next_right_edge,
      'face_left:'||left_face,
      'face_right:'||right_face
    ], '|')
    FROM topo.edge
    ORDER BY edge_id;
  END IF;

  IF debug THEN
    set client_min_messages to DEBUG;
  END IF;
  PERFORM topology.TopoGeo_addPoint('topo', point, prec);
  IF debug THEN
    set client_min_messages to WARNING;
  END IF;

  IF debug THEN
    RETURN QUERY SELECT array_to_string(ARRAY[
      lbl, --'topo',
      'aft',
      'E' || edge_id,
      'next_left:'||next_left_edge,
      'next_right:'||next_right_edge,
      'face_left:'||left_face,
      'face_right:'||right_face
    ], '|')
    FROM topo.edge
    ORDER BY edge_id;
  END IF;

  RETURN QUERY
    WITH j AS (
      SELECT
        row_number() over () as rn,
        to_json(s) as o
      FROM ValidateTopology('topo') s
    )
    SELECT
      array_to_string(
        ARRAY[lbl,'unexpected','validity issue'] ||
        array_agg(x.value order by x.ordinality),
        '|'
      )
    FROM j, json_each_text(j.o)
    WITH ordinality AS x
    GROUP by j.rn;


  RETURN QUERY SELECT array_to_string(ARRAY[
    lbl,
    'unexpected',
    'lineal drift',
    l,
    dist::text
  ], '|')
  FROM (
    SELECT t.lbl l, ST_HausdorffDistance(t.g, tg::geometry) dist
    FROM topo.fl t
  ) foo WHERE dist >= COALESCE(
      NULLIF(prec,0),
      topology._st_mintolerance(point)
  )
  ORDER BY foo.l;

  SELECT sum(ST_Area(t.g)) as before, sum(ST_Area(tg::geometry)) as after
  FROM topo.fa t
  INTO rec;

  IF rec.before != rec.after THEN
    RETURN QUERY SELECT array_to_string(ARRAY[
      lbl,
      'unexpected total area change',
      rec.before::text,
      rec.after::text
    ], '|')
    ;

    RETURN QUERY SELECT array_to_string(ARRAY[
      lbl,
      'area change',
      l,
      bfr::text,
      aft::text
    ], '|')
    FROM (
      SELECT t.lbl l, ST_Area(t.g) bfr, ST_Area(tg::geometry) aft
      FROM topo.fa t
    ) foo WHERE bfr != aft
    ORDER BY foo.l;

  END IF;

  IF NOT debug THEN
    PERFORM topology.DropTopology ('topo');
  END IF;

END;
$BODY$
LANGUAGE 'plpgsql';

-- === raster: conflicting-overload helper, renamed per file to avoid ambiguity ===
-- make_test_raster is defined in ~23 raster files; only tickets.sql's copy runs a
-- dynamic EXECUTE (so only it is rejected). patchPostGISConflictingHelpers renames
-- just tickets.sql's calls to make_test_raster_tickets, so this seed does not
-- collide with the 22 self-created make_test_raster copies in the other files.
CREATE OR REPLACE FUNCTION make_test_raster_tickets(
  table_suffix text,
	rid integer,
  scale_x double precision,
  scale_y double precision DEFAULT 1.0
)
RETURNS void
AS $$
DECLARE
	rast raster;
 	width integer := 2;
 	height integer := 2;
 	ul_x double precision := 0;
 	ul_y double precision := 0;
 	skew_x double precision := 0;
 	skew_y double precision := 0;
 	initvalue double precision := 1;
 	nodataval double precision := 0;
BEGIN
	rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, scale_x, scale_y, skew_x, skew_y, 0);
	rast := ST_AddBand(rast, 1, '8BUI', initvalue, nodataval);

	EXECUTE format('INSERT INTO test_raster_scale_%s VALUES (%L, %L)', table_suffix, rid, rast);
	RETURN;
END;
$$ LANGUAGE 'plpgsql';
