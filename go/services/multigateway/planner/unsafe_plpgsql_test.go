// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAnalyzeProceduralBody_Reject covers Tier 1 statements whose PL/pgSQL or SQL
// body reaches an unsafe construct — a session-state change or a blocklisted
// call — and must be rejected with feature_not_supported.
func TestAnalyzeProceduralBody_Reject(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		{
			name:    "DO PERFORM set_config",
			sql:     "DO $$ BEGIN PERFORM set_config('work_mem','10GB',false); END $$",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
		{
			name:    "DO literal SET",
			sql:     "DO $$ BEGIN SET work_mem = '10GB'; END $$",
			wantMsg: "SET/RESET inside a PL/pgSQL body is not supported",
		},
		{
			name:    "DO literal RESET",
			sql:     "DO $$ BEGIN RESET work_mem; END $$",
			wantMsg: "SET/RESET inside a PL/pgSQL body is not supported",
		},
		{
			name:    "DO blocklisted dblink in PERFORM",
			sql:     "DO $$ BEGIN PERFORM dblink('host=x','SELECT 1'); END $$",
			wantMsg: "dblink is not supported",
		},
		{
			name:    "DO blocklisted pg_read_file in assignment",
			sql:     "DO $$ DECLARE x text; BEGIN x := pg_read_file('/etc/passwd'); END $$",
			wantMsg: "pg_read_file is not supported",
		},
		{
			name:    "DO set_config in DECLARE default",
			sql:     "DO $$ DECLARE x text := set_config('work_mem','10GB',false); BEGIN NULL; END $$",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
		{
			name:    "DO set_config inside IF (conditional)",
			sql:     "DO $$ BEGIN IF true THEN PERFORM set_config('work_mem','10GB',false); END IF; END $$",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
		{
			name:    "DO set_config inside exception handler",
			sql:     "DO $$ BEGIN NULL; EXCEPTION WHEN others THEN PERFORM set_config('work_mem','10GB',false); END $$",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
		{
			name:    "DO dynamic EXECUTE literal SET",
			sql:     "DO $$ BEGIN EXECUTE 'SET work_mem = ''10GB'''; END $$",
			wantMsg: "SET/RESET inside a PL/pgSQL body is not supported",
		},
		{
			name:    "DO dynamic EXECUTE non-literal",
			sql:     "DO $$ DECLARE v text := '10GB'; BEGIN EXECUTE 'SET work_mem = ' || v; END $$",
			wantMsg: "EXECUTE of a runtime-built statement",
		},
		{
			// The restricted-GUC guard runs ahead of the generic body-SET
			// rejection and gives its more specific message; either way it is
			// rejected.
			name:    "DO restricted GUC via SET",
			sql:     "DO $$ BEGIN SET synchronous_commit = off; END $$",
			wantMsg: "setting synchronous_commit is not supported",
		},
		{
			name:    "DO Tier 2 in body",
			sql:     "DO $$ BEGIN CREATE DATABASE evil; END $$",
			wantMsg: "CREATE DATABASE is not supported",
		},
		{
			name:    "CREATE FUNCTION plpgsql PERFORM set_config",
			sql:     "CREATE FUNCTION f() RETURNS void AS $$ BEGIN PERFORM set_config('work_mem','10GB',false); END $$ LANGUAGE plpgsql",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
		{
			name:    "CREATE PROCEDURE plpgsql SET",
			sql:     "CREATE PROCEDURE p() AS $$ BEGIN SET work_mem = '10GB'; END $$ LANGUAGE plpgsql",
			wantMsg: "SET/RESET inside a PL/pgSQL body is not supported",
		},
		{
			name:    "CREATE FUNCTION sql body set_config",
			sql:     "CREATE FUNCTION f() RETURNS text AS $$ SELECT set_config('work_mem','10GB',false) $$ LANGUAGE sql",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
		{
			name:    "CREATE FUNCTION sql body dblink",
			sql:     "CREATE FUNCTION f() RETURNS text AS $$ SELECT dblink('host=x','SELECT 1') $$ LANGUAGE sql",
			wantMsg: "dblink is not supported",
		},
		{
			name:    "CREATE FUNCTION opaque language plpython",
			sql:     "CREATE FUNCTION f() RETURNS void AS $$ pass $$ LANGUAGE plpython3u",
			wantMsg: "cannot be inspected by the connection pooler",
		},
		{
			name:    "CREATE FUNCTION opaque language plperl",
			sql:     "CREATE FUNCTION f() RETURNS void AS $$ 1; $$ LANGUAGE plperl",
			wantMsg: "cannot be inspected by the connection pooler",
		},
		{
			// A dangerous call in a SELECT … INTO body must still be caught now
			// that the parser separates the INTO clause from the query.
			name:    "DO SELECT INTO with blocklisted call",
			sql:     "DO $$ DECLARE x text; BEGIN SELECT dblink('host=x','SELECT 1') INTO x; END $$",
			wantMsg: "dblink is not supported",
		},
		{
			name:    "DO SELECT INTO with set_config",
			sql:     "DO $$ DECLARE x text; BEGIN SELECT set_config('work_mem','1GB',false) INTO x; END $$",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
		{
			// Seeding a parameter makes `i := <rhs>` a real assignment; the RHS is
			// still analyzed, so a blocklisted call in it must still be caught.
			name:    "seeded param assignment with blocklisted RHS",
			sql:     "CREATE FUNCTION f(i text) RETURNS void AS $$ BEGIN i := dblink('h','q'); END $$ LANGUAGE plpgsql",
			wantMsg: "dblink is not supported",
		},
		{
			// Same for a seeded trigger variable target.
			name:    "seeded trigger NEW.field with set_config RHS",
			sql:     "CREATE FUNCTION tf() RETURNS trigger AS $$ BEGIN NEW.a := set_config('work_mem','1GB',false); RETURN NEW; END $$ LANGUAGE plpgsql",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
		{
			name:    "nested DO inside DO body",
			sql:     "DO $$ BEGIN EXECUTE 'DO $x$ BEGIN PERFORM set_config(''work_mem'',''10GB'',false); END $x$'; END $$",
			wantMsg: "set_config inside a PL/pgSQL body is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, tt.sql), false)
			require.ErrorContains(t, err, tt.wantMsg)
		})
	}
}

// TestAnalyzeProceduralBody_ChildCoverage guards the statements the walker
// intercepts with `return false`: it must re-descend into their other children
// (USING params, a dynamic FOR loop body, OPEN's static/dynamic query), or an
// unsafe construct there would silently pass (fail open).
func TestAnalyzeProceduralBody_ChildCoverage(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		{
			name:    "EXECUTE USING param",
			sql:     "DO $$ BEGIN EXECUTE 'SELECT $1' USING pg_read_file('/etc/passwd'); END $$",
			wantMsg: "pg_read_file is not supported",
		},
		{
			name:    "dynamic FOR loop body",
			sql:     "DO $$ BEGIN FOR r IN EXECUTE 'SELECT 1' LOOP PERFORM dblink('host=x','SELECT 1'); END LOOP; END $$",
			wantMsg: "dblink is not supported",
		},
		{
			name:    "dynamic FOR USING param",
			sql:     "DO $$ BEGIN FOR r IN EXECUTE 'SELECT $1' USING lo_import('/etc/passwd') LOOP NULL; END LOOP; END $$",
			wantMsg: "lo_import is not supported",
		},
		{
			name:    "RETURN QUERY EXECUTE literal SET",
			sql:     "CREATE FUNCTION f() RETURNS SETOF int AS $$ BEGIN RETURN QUERY EXECUTE 'SET work_mem = ''1GB'''; END $$ LANGUAGE plpgsql",
			wantMsg: "SET/RESET inside a PL/pgSQL body is not supported",
		},
		{
			name:    "RETURN QUERY static blocklisted",
			sql:     "CREATE FUNCTION f() RETURNS SETOF text AS $$ BEGIN RETURN QUERY SELECT pg_read_file('/etc/passwd'); END $$ LANGUAGE plpgsql",
			wantMsg: "pg_read_file is not supported",
		},
		{
			name:    "OPEN FOR static query blocklisted",
			sql:     "DO $$ DECLARE c refcursor; BEGIN OPEN c FOR SELECT dblink('host=x','SELECT 1'); END $$",
			wantMsg: "dblink is not supported",
		},
		{
			name:    "OPEN FOR EXECUTE non-literal",
			sql:     "DO $$ DECLARE c refcursor; v text := 'x'; BEGIN OPEN c FOR EXECUTE 'SELECT ' || v; END $$",
			wantMsg: "EXECUTE of a runtime-built statement",
		},
		{
			name:    "OPEN bound-cursor positional arg blocklisted",
			sql:     "DO $$ DECLARE c refcursor; BEGIN OPEN c(dblink('host=x','SELECT 1')); END $$",
			wantMsg: "dblink is not supported",
		},
		{
			name:    "OPEN bound-cursor named arg blocklisted",
			sql:     "DO $$ DECLARE c refcursor; BEGIN OPEN c(p := lo_import('/etc/passwd')); END $$",
			wantMsg: "lo_import is not supported",
		},
		{
			name:    "cursor FOR loop body blocklisted",
			sql:     "DO $$ DECLARE c CURSOR FOR SELECT 1; BEGIN FOR r IN c LOOP PERFORM dblink('host=x','SELECT 1'); END LOOP; END $$",
			wantMsg: "dblink is not supported",
		},
		{
			name:    "cursor FOR loop arg value blocklisted",
			sql:     "DO $$ DECLARE c CURSOR (p int) FOR SELECT 1; BEGIN FOR r IN c(lo_import('/etc/passwd')) LOOP NULL; END LOOP; END $$",
			wantMsg: "lo_import is not supported",
		},
		{
			name:    "query FOR loop blocklisted (regression: statement path still analyzed)",
			sql:     "DO $$ BEGIN FOR r IN SELECT dblink('host=x','SELECT 1') LOOP NULL; END LOOP; END $$",
			wantMsg: "dblink is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, tt.sql), false)
			require.ErrorContains(t, err, tt.wantMsg)
		})
	}
}

// TestAnalyzeProceduralBody_Allow covers benign Tier 1 bodies that reach no unsafe
// construct and must be allowed through.
func TestAnalyzeProceduralBody_Allow(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"DO benign insert", "DO $$ BEGIN INSERT INTO audit(msg) VALUES ('hi'); END $$"},
		{"DO benign assignment", "DO $$ DECLARE x int; BEGIN x := 1 + 1; END $$"},
		{"DO benign perform", "DO $$ BEGIN PERFORM count(*) FROM users; END $$"},
		{"DO benign dynamic literal", "DO $$ BEGIN EXECUTE 'INSERT INTO t VALUES (1)'; END $$"},
		{"DO benign EXECUTE USING param", "DO $$ BEGIN EXECUTE 'INSERT INTO t VALUES ($1)' USING abs(-1); END $$"},
		{"DO benign dynamic FOR body", "DO $$ BEGIN FOR r IN EXECUTE 'SELECT 1' LOOP PERFORM pg_sleep(0); END LOOP; END $$"},
		{"OPEN bound-cursor named args (PL/pgSQL ':=' must not reach SQL parser)", "DO $$ DECLARE c refcursor; BEGIN OPEN c(p2 := 21, p1 := 20); END $$"},
		{"OPEN bound-cursor benign arg value", "DO $$ DECLARE c refcursor; BEGIN OPEN c(1, abs(-2)); END $$"},
		{"DO loop and if", "DO $$ BEGIN FOR i IN 1..10 LOOP IF i > 5 THEN PERFORM pg_sleep(0); END IF; END LOOP; END $$"},
		{"CREATE FUNCTION plpgsql benign", "CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 42; END $$ LANGUAGE plpgsql"},
		{"CREATE FUNCTION sql body benign", "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql"},
		{"CREATE FUNCTION sql standard body", "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; END"},
		// LANGUAGE c / internal reference a compiled symbol, not SQL — nothing to
		// inspect, no session-state vector, so they are allowed.
		{"CREATE FUNCTION language c", "CREATE FUNCTION f() RETURNS int AS 'MODULE_PATHNAME', 'f_sym' LANGUAGE c"},
		{"CREATE FUNCTION language internal", "CREATE FUNCTION xin(cstring) RETURNS int IMMUTABLE STRICT LANGUAGE internal AS 'int4in'"},
		// SELECT/INSERT … INTO bodies: the parser now separates the INTO clause,
		// so the query is analyzable SQL and a benign body is allowed.
		{"DO SELECT INTO benign", "DO $$ DECLARE x int; BEGIN SELECT id INTO x FROM users WHERE login = 'a'; END $$"},
		{"DO SELECT INTO STRICT multi-target", "DO $$ DECLARE x int; y int; BEGIN SELECT 1, 2 INTO STRICT x, y; END $$"},
		{"CREATE FUNCTION INSERT RETURNING INTO", "CREATE FUNCTION f() RETURNS void AS $$ DECLARE x int; BEGIN INSERT INTO t VALUES (1) RETURNING id INTO x; END $$ LANGUAGE plpgsql"},
		// Param-seeding: assignments to parameters, OUT params, positional $N, and
		// trigger variables resolve to assignments (not unparseable execsql
		// fragments), so a benign body is allowed.
		{"param assignment", "CREATE FUNCTION add(i int, j int) RETURNS int AS $$ BEGIN j := i + 1; RETURN j; END $$ LANGUAGE plpgsql"},
		{"OUT param assignment", "CREATE FUNCTION f(OUT result int) AS $$ BEGIN result := 42; END $$ LANGUAGE plpgsql"},
		{"positional param assignment", "CREATE FUNCTION f(int) RETURNS void AS $$ BEGIN $1 := $1 + 1; END $$ LANGUAGE plpgsql"},
		{"trigger NEW.field assignment", "CREATE FUNCTION tf() RETURNS trigger AS $$ BEGIN NEW.a := NEW.a * 10; RETURN NEW; END $$ LANGUAGE plpgsql"},
		// Cursor FOR loop over a bound cursor (with and without args). The cursor
		// reference (`c(5,7)` / `c2`) is the fors "query"; it is not a standalone
		// SQL statement, so it must be analyzed as an expression, not rejected.
		{"cursor FOR loop with args (forc01)", "CREATE FUNCTION forc01() RETURNS void AS $$ DECLARE c CURSOR (r1 int, r2 int) FOR SELECT * FROM generate_series(r1, r2) i; BEGIN FOR r IN c(5, 7) LOOP RAISE NOTICE '%', r.i; END LOOP; END $$ LANGUAGE plpgsql"},
		{"cursor FOR loop no args", "CREATE FUNCTION forc02() RETURNS void AS $$ DECLARE c2 CURSOR FOR SELECT * FROM generate_series(41, 43) i; BEGIN FOR r IN c2 LOOP RAISE NOTICE '%', r.i; END LOOP; END $$ LANGUAGE plpgsql"},
		// Assignment to a field of a named-composite variable (avg_transfn). Without
		// a catalog the composite type is treated as scalar, but `rec.field := expr`
		// is still an assignment, not an execsql fragment.
		{"composite field assignment (avg_transfn)", "CREATE FUNCTION avg_transfn(state avg_state, n int) RETURNS avg_state AS $$ DECLARE new_state avg_state; BEGIN new_state.total := n; state.total := state.total + n; RETURN state; END $$ LANGUAGE plpgsql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, tt.sql), false)
			require.NoError(t, err)
		})
	}
}

// TestAnalyzeProceduralBody_UnsafePoolerMode confirms the operator opt-out disables
// the body analysis: a body that would otherwise be rejected is allowed.
func TestAnalyzeProceduralBody_UnsafePoolerMode(t *testing.T) {
	unsafe := []string{
		"DO $$ BEGIN PERFORM set_config('work_mem','10GB',false); END $$",
		"DO $$ BEGIN SET work_mem = '10GB'; END $$",
		"DO $$ BEGIN PERFORM dblink('host=x','SELECT 1'); END $$",
		"CREATE FUNCTION f() RETURNS void AS $$ pass $$ LANGUAGE plpython3u",
	}
	for _, sql := range unsafe {
		t.Run(sql, func(t *testing.T) {
			// Enforced: rejected.
			_, err := analyzeStatement(parseOne(t, sql), false)
			require.Error(t, err)
			// unsafe-pooler-mode: allowed.
			_, err = analyzeStatement(parseOne(t, sql), true)
			require.NoError(t, err)
		})
	}
}

// TestAnalyzeDynamicExecute_SafeSkeleton covers the injection-safe dynamic-EXECUTE
// vetting: an EXECUTE whose structure is fixed by format() with %I/%L or by a `||`
// chain of string literals and quote_ident/quote_literal is analyzed as a static
// statement (skeleton + interpolated values), while any raw substitution,
// dangerous statement type, or blocklisted interpolated value is still rejected.
func TestAnalyzeDynamicExecute_SafeSkeleton(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		allow := map[string]string{
			"format %I create table":     "DO $$ DECLARE t text; BEGIN EXECUTE format('CREATE TABLE %I AS SELECT 1', t); END $$",
			"|| quote_ident select":      "DO $$ DECLARE t text; BEGIN EXECUTE 'select count(*) from ' || quote_ident(t); END $$",
			"|| quote_literal create":    "DO $$ DECLARE v text; BEGIN EXECUTE 'CREATE COLLATION c (locale = ' || quote_literal(v) || ')'; END $$",
			"format %L temp table check": "DO $$ DECLARE v text; BEGIN EXECUTE format('CREATE TEMP TABLE t (c text CHECK (c < %L)) ON COMMIT DROP', v); END $$",
			"nested format under %I":     "DO $$ DECLARE a text; b text; BEGIN EXECUTE format('DROP TABLE IF EXISTS s.%I', format('%s_%s', a, b)); END $$",
		}
		for name, sql := range allow {
			t.Run(name, func(t *testing.T) {
				_, err := analyzeStatement(parseOne(t, sql), false)
				require.NoError(t, err)
			})
		}
	})
	t.Run("reject", func(t *testing.T) {
		reject := map[string]string{
			// Dangerous statement type reached through safe quoting — the skeleton
			// still carries the SET / set_config, so it is caught.
			"SET via ||":              "DO $$ DECLARE v text; BEGIN EXECUTE 'SET work_mem = ' || quote_literal(v); END $$",
			"SET via format %L":       "DO $$ DECLARE v text; BEGIN EXECUTE format('SET work_mem = %L', v); END $$",
			"set_config via ||":       "DO $$ DECLARE a text; BEGIN EXECUTE 'SELECT set_config(' || quote_literal(a) || ',''1'',false)'; END $$",
			"blocklisted in skeleton": "DO $$ DECLARE x text; BEGIN EXECUTE format('SELECT dblink(%L, %L)', x, x); END $$",
			// Blocklisted call in an interpolated value (runs when the arg is built).
			"blocklisted in value": "DO $$ BEGIN EXECUTE format('CREATE TABLE %I AS SELECT 1', quote_literal(lo_import('/etc/passwd'))); END $$",
			// Raw substitution — arbitrary text reaches the statement.
			"raw concat variable": "DO $$ DECLARE q text; BEGIN EXECUTE 'explain analyze ' || q; END $$",
			"format %s raw":       "DO $$ DECLARE q text; BEGIN EXECUTE format('explain %s', q); END $$",
			"bare param":          "CREATE FUNCTION f(q text) RETURNS void AS $$ BEGIN EXECUTE q; END $$ LANGUAGE plpgsql",
			// A schema-qualified same-named function is not the trusted builtin.
			"schema-qualified format": "DO $$ DECLARE t text; BEGIN EXECUTE myschema.format('CREATE TABLE %I AS SELECT 1', t); END $$",
		}
		for name, sql := range reject {
			t.Run(name, func(t *testing.T) {
				_, err := analyzeStatement(parseOne(t, sql), false)
				require.Error(t, err)
			})
		}
	})
}

// TestAnalyzeDynamicExecute_VarDataflow covers the intra-body dataflow that lets
// a bare-variable EXECUTE payload (`EXECUTE v` / `RETURN QUERY EXECUTE v`) be
// checked against the expressions the variable can hold. Acceptance is sound:
// every assignment to the variable must reduce to a safe skeleton, and a variable
// written by any form we cannot reduce (SELECT/EXECUTE … INTO, a loop variable, a
// self-referential build, or with no assignment at all) is rejected.
func TestAnalyzeDynamicExecute_VarDataflow(t *testing.T) {
	accept := map[string]string{
		"const concat into var":    `DO $$ DECLARE v text; BEGIN v := 'SELECT 1 FROM t WHERE x = ' || '5'; EXECUTE v; END $$`,
		"format %I into var":       `DO $$ DECLARE v text; c text; BEGIN v := format('CREATE TABLE %I AS SELECT 1', c); EXECUTE v; END $$`,
		"RETURN QUERY EXECUTE var": `CREATE FUNCTION f() RETURNS SETOF int AS $$ DECLARE v text; c text; BEGIN v := format('SELECT * FROM %I', c); RETURN QUERY EXECUTE v; END $$ LANGUAGE plpgsql`,
		"transitive w := v":        `DO $$ DECLARE v text; w text; BEGIN v := 'SELECT 1'; w := v; EXECUTE w; END $$`,
		"FOR .. IN EXECUTE var":    `DO $$ DECLARE r record; v text; BEGIN v := 'SELECT 1 FROM t WHERE bucket_id = $1' ; FOR r IN EXECUTE v USING 1 LOOP NULL; END LOOP; END $$`,
		"safe assign, no CALL":     `DO $$ DECLARE v text; BEGIN v := 'SELECT 1'; EXECUTE v; END $$`,
		"CALL not feeding EXECUTE": `DO $$ DECLARE v text; BEGIN v := 'SELECT 1'; CALL p(v); END $$`,
	}
	for name, sql := range accept {
		t.Run("accept/"+name, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql), false)
			require.NoError(t, err)
		})
	}

	reject := map[string]string{
		"format %s into var":            `DO $$ DECLARE v text; x text; BEGIN v := format('SELECT * FROM t WHERE %s', x); EXECUTE v; END $$`,
		"unsafe concat into var":        `DO $$ DECLARE v text; x text; BEGIN v := 'DROP TABLE ' || x; EXECUTE v; END $$`,
		"var from SELECT INTO":          `DO $$ DECLARE v text; BEGIN SELECT relname INTO v FROM pg_class LIMIT 1; EXECUTE v; END $$`,
		"var also assigned via INTO":    `DO $$ DECLARE v text; BEGIN v := 'SELECT 1'; SELECT relname INTO v FROM pg_class LIMIT 1; EXECUTE v; END $$`,
		"var is loop variable":          `DO $$ DECLARE v text; BEGIN FOR v IN SELECT relname FROM pg_class LOOP EXECUTE v; END LOOP; END $$`,
		"self-referential build":        `DO $$ DECLARE v text := ''; c text; BEGIN v := v || ' ' || c; EXECUTE v; END $$`,
		"bare param, no assignment":     `CREATE FUNCTION f(q text) RETURNS void AS $$ BEGIN EXECUTE q; END $$ LANGUAGE plpgsql`,
		"param assigned conditionally":  `CREATE FUNCTION f(q text) RETURNS void AS $$ BEGIN IF false THEN q := 'SELECT 1'; END IF; EXECUTE q; END $$ LANGUAGE plpgsql`,
		"param reassigned then EXECUTE": `CREATE FUNCTION f(q text) RETURNS void AS $$ BEGIN q := 'SELECT 1'; EXECUTE q; END $$ LANGUAGE plpgsql`,
		"alias for param then EXECUTE":  `CREATE FUNCTION f(text) RETURNS void AS $$ DECLARE q ALIAS FOR $1; BEGIN q := 'SELECT 1'; EXECUTE q; END $$ LANGUAGE plpgsql`,
		"var mutated by CALL":           `DO $$ DECLARE v text; BEGIN v := 'SELECT 1'; CALL p(v); EXECUTE v; END $$`,
		"var mutated by named CALL":     `DO $$ DECLARE v text; BEGIN v := 'SELECT 1'; CALL p(arg => v); EXECUTE v; END $$`,
	}
	for name, sql := range reject {
		t.Run("reject/"+name, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql), false)
			require.Error(t, err)
		})
	}
}

// TestAnalyzeDynamicExecute_ConstrainedFormatS covers the constrained-%s handling:
// a top-level format() whose %s conversions are fed by a bounded set of constant
// string values (a literal, a CASE whose branches are all literals, or a variable
// proven to hold only such values) is expanded into every concrete statement it
// can produce, each analyzed as a static statement. It stays sound because a
// hostile constant is re-analyzed after substitution; and %s fed by anything whose
// value set cannot be bounded (a bare parameter, a lower()/guard-narrowed variable,
// a CASE without ELSE) is rejected.
func TestAnalyzeDynamicExecute_ConstrainedFormatS(t *testing.T) {
	accept := map[string]string{
		"literal %s":                      `DO $$ BEGIN EXECUTE format('SELECT * FROM t ORDER BY name %s', 'asc'); END $$`,
		"CASE-of-const var %s":            `DO $$ DECLARE d text; c text; BEGIN d := CASE WHEN true THEN 'asc' ELSE 'desc' END; EXECUTE format('SELECT * FROM t ORDER BY %I %s', c, d); END $$`,
		"two %s same const var":           `DO $$ DECLARE d text; BEGIN d := CASE WHEN true THEN 'asc' ELSE 'desc' END; EXECUTE format('SELECT a %s, b %s FROM t', d, d); END $$`,
		"const %s built then EXECUTE var": `CREATE FUNCTION f() RETURNS SETOF int AS $$ DECLARE v text; d text; c text; BEGIN d := CASE WHEN true THEN 'asc' ELSE 'desc' END; v := format('SELECT * FROM t ORDER BY %I %s', c, d); RETURN QUERY EXECUTE v; END $$ LANGUAGE plpgsql`,
	}
	for name, sql := range accept {
		t.Run("accept/"+name, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql), false)
			require.NoError(t, err)
		})
	}

	reject := map[string]string{
		"%s fed by lower(param)":      `CREATE FUNCTION f(so text) RETURNS void AS $$ DECLARE d text; BEGIN d := lower(so); EXECUTE format('SELECT 1 ORDER BY x %s', d); END $$ LANGUAGE plpgsql`,
		"%s fed by bare param":        `CREATE FUNCTION f(d text) RETURNS void AS $$ BEGIN EXECUTE format('SELECT 1 ORDER BY x %s', d); END $$ LANGUAGE plpgsql`,
		"%s fed by CASE without ELSE": `DO $$ DECLARE d text; BEGIN d := CASE WHEN true THEN 'asc' END; EXECUTE format('SELECT 1 ORDER BY x %s', d); END $$`,
		"injected set_config via %s":  `DO $$ DECLARE d text; BEGIN d := 'x); SELECT set_config(''work_mem'',''1GB'',false'; EXECUTE format('SELECT count(*) FROM t WHERE a IN (%s)', d); END $$`,
		"%s var tainted by INTO":      `DO $$ DECLARE d text; BEGIN d := 'asc'; SELECT relname INTO d FROM pg_class LIMIT 1; EXECUTE format('SELECT 1 ORDER BY x %s', d); END $$`,
	}
	for name, sql := range reject {
		t.Run("reject/"+name, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql), false)
			require.Error(t, err)
		})
	}
}

// TestAnalyzeProceduralBody_TransactionScopedSet covers the transaction-scoped
// SET allowance: SET LOCAL and SET TRANSACTION inside a body revert at
// transaction end and are allowed, while the session-persisting forms — plain
// SET, RESET, SET SESSION CHARACTERISTICS — and a SET LOCAL of a cluster-managed
// GUC stay rejected.
func TestAnalyzeProceduralBody_TransactionScopedSet(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		allow := map[string]string{
			"SET LOCAL param":       "DO $$ BEGIN SET LOCAL work_mem = '2MB'; END $$",
			"SET LOCAL search_path": "DO $$ BEGIN SET LOCAL search_path = 'x'; END $$",
			"SET TRANSACTION iso":   "DO $$ BEGIN COMMIT; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; END $$",
			"SET TRANSACTION ro":    "DO $$ BEGIN SET TRANSACTION READ ONLY; END $$",
		}
		for name, sql := range allow {
			t.Run(name, func(t *testing.T) {
				_, err := analyzeStatement(parseOne(t, sql), false)
				require.NoError(t, err)
			})
		}
	})
	t.Run("reject", func(t *testing.T) {
		reject := map[string]string{
			"plain SET":                   "DO $$ BEGIN SET work_mem = '2MB'; END $$",
			"RESET":                       "DO $$ BEGIN RESET work_mem; END $$",
			"SET SESSION CHARACTERISTICS": "DO $$ BEGIN SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE; END $$",
			"SET LOCAL cluster GUC":       "DO $$ BEGIN SET LOCAL synchronous_commit = off; END $$",
		}
		for name, sql := range reject {
			t.Run(name, func(t *testing.T) {
				_, err := analyzeStatement(parseOne(t, sql), false)
				require.Error(t, err)
			})
		}
	})
}

// TestAnalyzeProceduralBody_MalformedBodyFailsClosed confirms a body that does not
// parse as PL/pgSQL is rejected rather than passed through.
func TestAnalyzeProceduralBody_MalformedBodyFailsClosed(t *testing.T) {
	// A body that is not a valid PL/pgSQL block (missing BEGIN) fails to parse.
	_, err := analyzeStatement(parseOne(t, "DO $$ this is not plpgsql $$"), false)
	require.ErrorContains(t, err, "could not be parsed for safety analysis")
}
