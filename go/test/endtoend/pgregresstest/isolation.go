// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgregresstest

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/tools/executil"
)

// patchIsolationtester rewrites two pieces of
// src/test/isolation/isolationtester.c so the harness works against
// multigateway:
//
//  1. Per-session application_name setup. Upstream sends
//     PQexecParams("SELECT set_config('application_name',
//     current_setting('application_name') || '/' || $1, false)", ...),
//     which the multigateway planner rejects (the value must be a literal
//     constant for is_local=false set_config so the pooler can track it).
//     The replacement reads PGAPPNAME (set by isolation_main.c per test),
//     concatenates the session name client-side, escapes via
//     PQescapeLiteral, and sends a simple-protocol PQexec.
//
//  2. Lock-wait probe function and arguments. Upstream prepares
//     `pg_catalog.pg_isolation_test_session_is_blocked(...)` with backend
//     PIDs. Through multigateway those PIDs are virtual, while PostgreSQL lock
//     tables expose real backend PIDs. We point the harness at our own
//     function `public.multigres_test_session_is_blocked` (installed by
//     installPIDMappingFunction) and pass the per-session application_name
//     values instead; the shim maps those names to real backend PIDs via
//     pg_stat_activity.
//
// Idempotent: source is reset via `git checkout` before patching, so
// repeat invocations against the cached checkout produce the same
// result.
func (pb *PostgresBuilder) patchIsolationtester(t *testing.T, ctx context.Context) error {
	t.Helper()
	rel := filepath.Join("src", "test", "isolation", "isolationtester.c")
	abs := filepath.Join(pb.SourceDir, rel)

	reset := executil.Command(ctx, "git", "-C", pb.SourceDir, "checkout", "--", rel)
	if out, err := reset.CombinedOutput(); err != nil {
		return fmt.Errorf("reset %s: %w\n%s", rel, err, out)
	}

	src, err := os.ReadFile(abs)
	if err != nil {
		return fmt.Errorf("read %s: %w", abs, err)
	}

	appNameOrig := "\t\tres = PQexecParams(conns[i].conn,\n" +
		"\t\t\t\t\t\t   \"SELECT set_config('application_name',\\n\"\n" +
		"\t\t\t\t\t\t   \"  current_setting('application_name') || '/' || $1,\\n\"\n" +
		"\t\t\t\t\t\t   \"  false)\",\n" +
		"\t\t\t\t\t\t   1, NULL,\n" +
		"\t\t\t\t\t\t   &sessionname,\n" +
		"\t\t\t\t\t\t   NULL, NULL, 0);"

	appNameReplacement := "\t\t/*\n" +
		"\t\t * multigres patch: build the application_name literal client-side\n" +
		"\t\t * and send it via the simple protocol. The multigateway planner\n" +
		"\t\t * rejects set_config() when the value is a non-literal expression\n" +
		"\t\t * or bound parameter (the pooler tracks the literal value), so we\n" +
		"\t\t * cannot use PQexecParams + current_setting() here. Keep the\n" +
		"\t\t * combined value for the multigres wait-detection shim too.\n" +
		"\t\t */\n" +
		"\t\t{\n" +
		"\t\t\tconst char *appname_prefix = getenv(\"PGAPPNAME\");\n" +
		"\t\t\tchar\t   *combined;\n" +
		"\t\t\tchar\t   *escaped;\n" +
		"\t\t\tchar\t   *appname_query;\n" +
		"\n" +
		"\t\t\tif (appname_prefix == NULL)\n" +
		"\t\t\t\tappname_prefix = \"\";\n" +
		"\t\t\tcombined = psprintf(\"%s/%s\", appname_prefix, sessionname);\n" +
		"\t\t\tconns[i].application_name = combined;\n" +
		"\t\t\tescaped = PQescapeLiteral(conns[i].conn, combined, strlen(combined));\n" +
		"\t\t\tif (escaped == NULL)\n" +
		"\t\t\t{\n" +
		"\t\t\t\tfprintf(stderr, \"PQescapeLiteral failed: %s\",\n" +
		"\t\t\t\t\t\tPQerrorMessage(conns[i].conn));\n" +
		"\t\t\t\texit(1);\n" +
		"\t\t\t}\n" +
		"\t\t\tappname_query = psprintf(\n" +
		"\t\t\t\t\"SELECT set_config('application_name', %s, false)\", escaped);\n" +
		"\t\t\tPQfreemem(escaped);\n" +
		"\t\t\tres = PQexec(conns[i].conn, appname_query);\n" +
		"\t\t\tfree(appname_query);\n" +
		"\t\t}"

	if !bytes.Contains(src, []byte(appNameOrig)) {
		return fmt.Errorf("%s: original set_config block not found (PG version drift?)", rel)
	}
	patched := bytes.Replace(src, []byte(appNameOrig), []byte(appNameReplacement), 1)

	structOrig := "\t/* Name of the associated session. */\n" +
		"\tconst char *sessionname;\n" +
		"\t/* Active step on this connection, or NULL if idle. */\n"
	structReplacement := "\t/* Name of the associated session. */\n" +
		"\tconst char *sessionname;\n" +
		"\t/* Full application_name assigned to this connection. */\n" +
		"\tconst char *application_name;\n" +
		"\t/* Active step on this connection, or NULL if idle. */\n"
	if !bytes.Contains(patched, []byte(structOrig)) {
		return fmt.Errorf("%s: IsoConnInfo sessionname block not found (PG version drift?)", rel)
	}
	patched = bytes.Replace(patched, []byte(structOrig), []byte(structReplacement), 1)

	waitQueryOrig := "\tinitPQExpBuffer(&wait_query);\n" +
		"\tappendPQExpBufferStr(&wait_query,\n" +
		"\t\t\t\t\t\t \"SELECT pg_catalog.pg_isolation_test_session_is_blocked($1, '{\");\n" +
		"\t/* The spec syntax requires at least one session; assume that here. */\n" +
		"\tappendPQExpBufferStr(&wait_query, conns[1].backend_pid_str);\n" +
		"\tfor (i = 2; i < nconns; i++)\n" +
		"\t\tappendPQExpBuffer(&wait_query, \",%s\", conns[i].backend_pid_str);\n" +
		"\tappendPQExpBufferStr(&wait_query, \"}')\");"
	waitQueryReplacement := "\tinitPQExpBuffer(&wait_query);\n" +
		"\tappendPQExpBufferStr(&wait_query,\n" +
		"\t\t\t\t\t\t \"SELECT public.multigres_test_session_is_blocked($1::text, ARRAY[\");\n" +
		"\t/* The spec syntax requires at least one session; assume that here. */\n" +
		"\t{\n" +
		"\t\tchar\t   *escaped;\n" +
		"\n" +
		"\t\tescaped = PQescapeLiteral(conns[0].conn, conns[1].application_name,\n" +
		"\t\t\t\t\t\t\t\t  strlen(conns[1].application_name));\n" +
		"\t\tif (escaped == NULL)\n" +
		"\t\t{\n" +
		"\t\t\tfprintf(stderr, \"PQescapeLiteral failed: %s\",\n" +
		"\t\t\t\t\tPQerrorMessage(conns[0].conn));\n" +
		"\t\t\texit(1);\n" +
		"\t\t}\n" +
		"\t\tappendPQExpBufferStr(&wait_query, escaped);\n" +
		"\t\tPQfreemem(escaped);\n" +
		"\t\tfor (i = 2; i < nconns; i++)\n" +
		"\t\t{\n" +
		"\t\t\tescaped = PQescapeLiteral(conns[0].conn, conns[i].application_name,\n" +
		"\t\t\t\t\t\t\t\t  strlen(conns[i].application_name));\n" +
		"\t\t\tif (escaped == NULL)\n" +
		"\t\t\t{\n" +
		"\t\t\t\tfprintf(stderr, \"PQescapeLiteral failed: %s\",\n" +
		"\t\t\t\t\t\tPQerrorMessage(conns[0].conn));\n" +
		"\t\t\t\texit(1);\n" +
		"\t\t\t}\n" +
		"\t\t\tappendPQExpBuffer(&wait_query, \",%s\", escaped);\n" +
		"\t\t\tPQfreemem(escaped);\n" +
		"\t\t}\n" +
		"\t}\n" +
		"\tappendPQExpBufferStr(&wait_query, \"]::text[])\");"
	if !bytes.Contains(patched, []byte(waitQueryOrig)) {
		return fmt.Errorf("%s: wait-query construction block not found (PG version drift?)", rel)
	}
	patched = bytes.Replace(patched, []byte(waitQueryOrig), []byte(waitQueryReplacement), 1)

	waitParamOrig := "&conns[step->session + 1].backend_pid_str"
	waitParamReplacement := "&conns[step->session + 1].application_name"
	if !bytes.Contains(patched, []byte(waitParamOrig)) {
		return fmt.Errorf("%s: wait-query parameter reference not found (PG version drift?)", rel)
	}
	patched = bytes.Replace(patched, []byte(waitParamOrig), []byte(waitParamReplacement), 1)

	if err := os.WriteFile(abs, patched, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", abs, err)
	}
	t.Logf("Patched %s: literal application_name + public.multigres_test_session_is_blocked", rel)
	return nil
}

// BuildIsolation builds the PostgreSQL isolation test tools (isolationtester and
// pg_isolation_regress). Must be called after Build().
func (pb *PostgresBuilder) BuildIsolation(t *testing.T, ctx context.Context) error {
	t.Helper()

	if err := pb.patchIsolationtester(t, ctx); err != nil {
		return fmt.Errorf("patch isolationtester: %w", err)
	}

	isolationDir := filepath.Join(pb.BuildDir, "src", "test", "isolation")

	t.Logf("Building isolation test tools in %s...", isolationDir)
	cmd := executil.Command(ctx, "make", "-C", isolationDir, "all")
	cmd.Cmd.Stdout = os.Stdout
	cmd.Cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("make isolation tools failed: %w", err)
	}

	t.Logf("Isolation test tools built successfully")
	return nil
}

// installPIDMappingFunction creates public.multigres_test_session_is_blocked
// in the target database so the patched isolationtester (see
// patchIsolationtester) can probe lock waits through the multigateway →
// multipooler → PostgreSQL hop.
//
// Multipooler is configured with --database=postgres and routes every
// query to a pooled connection against the postgres DB regardless of the
// dbname in the client startup packet, so the shim must live in postgres.
// Both isolation invocation paths (selective via PGISOLATION_TESTS and
// full-suite via the make installcheck target) force --dbname=postgres on
// pg_isolation_regress, so postgres is also the dbname the harness opens.
//
// The shim mirrors the upstream builtin: returns true if check_session is
// waiting on any session in blocked_by for heavyweight lock waits
// (pg_blocking_pids). For SSI safe-snapshot waits
// (pg_safe_snapshot_blocking_pids — required for SERIALIZABLE READ ONLY
// DEFERRABLE specs such as read-only-anomaly-3), PostgreSQL deliberately
// returns true when any safe-snapshot blocker exists; those blockers are
// never autovacuum/background workers, so no blocked_by intersection is
// needed. Inputs are application_name values assigned by the patched
// isolationtester. A given application_name can map to multiple PG backends in
// flight (a stale pooled backend plus the live reserved conn), so the
// wait-check aggregates over every matching backend rather than picking one
// non-deterministically.
func (pb *PostgresBuilder) installPIDMappingFunction(t *testing.T, pgPort int, password string) error {
	t.Helper()
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		pgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	stmts := []string{
		`DROP TABLE IF EXISTS public.isolation_debug_log`,
		// Debug table: every shim invocation logs its inputs/outputs and a
		// snapshot of every backend in this DB so failures can be diagnosed
		// post-hoc by querying isolation_debug_log (see dumpIsolationDebugLog).
		`CREATE TABLE public.isolation_debug_log (
			id serial PRIMARY KEY,
			ts timestamptz DEFAULT now(),
			check_session text,
			blocked_by text[],
			real_check_pids int4[],
			real_blocked_by int4[],
			blocking_pids int4[],
			session_entries text[],
			all_pg_backends text[],
			result boolean
		)`,
		`DROP FUNCTION IF EXISTS public.multigres_test_session_is_blocked(int4, int4[])`,
		`DROP FUNCTION IF EXISTS public.multigres_test_session_is_blocked(text, text[])`,
		`CREATE FUNCTION public.multigres_test_session_is_blocked(check_session text, blocked_by text[])
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
<<fn>>
DECLARE
    v_log_id int4;
    v_real_check_pids int4[];
    v_real_blocked_by int4[];
    v_heavyweight_blocking_pids int4[];
    v_safe_snapshot_blocking_pids int4[];
    v_blocking_pids int4[];
    v_session_entries text[];
    v_all_backends text[];
    v_result boolean;
BEGIN
    -- Capture the inserted id directly so the later UPDATE targets THIS
    -- invocation's row even under concurrent shim calls (parallel groups
    -- in the isolation schedule, or multiple sessions polling at once).
    INSERT INTO public.isolation_debug_log
        (check_session, blocked_by, result)
    VALUES
        (check_session, blocked_by, NULL)
    RETURNING id INTO v_log_id;

    SELECT array_agg(sa.application_name || '=' || sa.pid)
    INTO v_session_entries
    FROM pg_stat_activity sa
    WHERE sa.datname = current_database()
      AND (sa.application_name = check_session OR sa.application_name = ANY(blocked_by));

    SELECT array_agg(sa.pid || ':' || COALESCE(sa.application_name,'<null>') || ':' || COALESCE(sa.state,'<null>'))
    INTO v_all_backends
    FROM pg_stat_activity sa
    WHERE sa.datname = current_database();

    -- A session application_name can map to multiple PG backends (a stale
    -- pooled backend after the client ran a regular query, plus the live
    -- reserved conn). Aggregate over every matching backend rather than
    -- choosing one non-deterministically.
    SELECT array_agg(sa.pid) INTO v_real_check_pids
    FROM pg_stat_activity sa
    WHERE sa.datname = current_database()
      AND sa.application_name = check_session;

    SELECT array_agg(sa.pid) INTO v_real_blocked_by
    FROM pg_stat_activity sa
    WHERE sa.datname = current_database()
      AND sa.application_name = ANY(blocked_by);

    v_real_check_pids := COALESCE(v_real_check_pids, '{}'::int4[]);
    v_real_blocked_by := COALESCE(v_real_blocked_by, '{}'::int4[]);

    -- Aggregate heavyweight lock blockers across every PG backend currently
    -- carrying check_session's application_name. Aggregation handles the
    -- duplicate mapping case where one backend is the live reserved conn
    -- (potentially blocked) and another is a stale pooled conn (idle).
    SELECT COALESCE(array_agg(DISTINCT b), '{}'::int4[]) INTO v_heavyweight_blocking_pids
    FROM (
        SELECT unnest(pg_blocking_pids(sa.pid)) AS b
        FROM pg_stat_activity sa
        WHERE sa.datname = current_database()
          AND sa.application_name = check_session
    ) sub
    WHERE b IS NOT NULL;

    -- Mirror pg_isolation_test_session_is_blocked's safe-snapshot behavior:
    -- any safe-snapshot blocker means the isolation step is blocked. PostgreSQL
    -- intentionally does not intersect these pids with the interesting session
    -- list because safe-snapshot waits are not caused by autovacuum/background
    -- workers. This matters for SERIALIZABLE READ ONLY DEFERRABLE specs such as
    -- read-only-anomaly-3.
    SELECT COALESCE(array_agg(DISTINCT b), '{}'::int4[]) INTO v_safe_snapshot_blocking_pids
    FROM (
        SELECT unnest(pg_safe_snapshot_blocking_pids(sa.pid)) AS b
        FROM pg_stat_activity sa
        WHERE sa.datname = current_database()
          AND sa.application_name = check_session
    ) sub
    WHERE b IS NOT NULL;

    SELECT COALESCE(array_agg(DISTINCT b), '{}'::int4[]) INTO v_blocking_pids
    FROM (
        SELECT unnest(v_heavyweight_blocking_pids) AS b
        UNION ALL
        SELECT unnest(v_safe_snapshot_blocking_pids) AS b
    ) sub
    WHERE b IS NOT NULL;

    v_result := (v_heavyweight_blocking_pids && v_real_blocked_by)
        OR cardinality(v_safe_snapshot_blocking_pids) > 0;

    UPDATE public.isolation_debug_log
    SET real_check_pids = v_real_check_pids,
        real_blocked_by = v_real_blocked_by,
        blocking_pids = v_blocking_pids,
        session_entries = v_session_entries,
        all_pg_backends = v_all_backends,
        result = v_result
    WHERE id = v_log_id;

    RETURN v_result;
END fn;
$$`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute statement [%s]: %w", truncateForLog(stmt, 80), err)
		}
	}

	// Sanity check: the function exists and is plpgsql.
	var lang string
	if err := db.QueryRow(`
		SELECT l.lanname
		FROM pg_proc p JOIN pg_language l ON p.prolang = l.oid
		WHERE p.proname = 'multigres_test_session_is_blocked'
		  AND p.pronamespace = 'public'::regnamespace`).Scan(&lang); err != nil {
		return fmt.Errorf("verify multigres_test_session_is_blocked: %w", err)
	}
	if lang != "plpgsql" {
		return fmt.Errorf("multigres_test_session_is_blocked installed with lanname=%q (expected plpgsql)", lang)
	}

	t.Logf("Installed public.multigres_test_session_is_blocked on database \"postgres\"")
	return nil
}

// dumpIsolationDebugLog prints recent entries from
// public.isolation_debug_log so investigators can see the inputs/outputs
// of every shim invocation during the isolation run. Best-effort;
// failures are logged and ignored.
func (pb *PostgresBuilder) dumpIsolationDebugLog(t *testing.T, pgPort int, password string) {
	t.Helper()
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		pgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Logf("isolation_debug_log dump: connect failed: %v", err)
		return
	}
	defer db.Close()

	var total int
	if err := db.QueryRow(`SELECT count(*) FROM public.isolation_debug_log WHERE check_session IS NOT NULL`).Scan(&total); err != nil {
		t.Logf("isolation_debug_log dump: count failed: %v", err)
		return
	}
	t.Logf("isolation_debug_log: %d shim invocations recorded during run", total)
	if total == 0 {
		t.Logf("isolation_debug_log: shim never executed — wait-query is not reaching public.multigres_test_session_is_blocked")
		return
	}

	rows, err := db.Query(`
		SELECT id, check_session, blocked_by, real_check_pids, real_blocked_by,
		       blocking_pids, session_entries, all_pg_backends, result
		FROM public.isolation_debug_log
		WHERE check_session IS NOT NULL
		ORDER BY id DESC
		LIMIT 30`)
	if err != nil {
		t.Logf("isolation_debug_log dump: select failed: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var checkSession, blockedBy, realCheckPids, realBlockedBy, blockingPids, sessionEntries, allBackends sql.NullString
		var result sql.NullBool
		if err := rows.Scan(&id, &checkSession, &blockedBy, &realCheckPids, &realBlockedBy, &blockingPids, &sessionEntries, &allBackends, &result); err != nil {
			t.Logf("isolation_debug_log dump: scan failed: %v", err)
			continue
		}
		t.Logf("isolation_debug_log id=%d check_session=%s blocked_by=%s real_check=%s real_blocked=%s blocking=%s sessions=%s all=%s result=%v",
			id, checkSession.String, blockedBy.String, realCheckPids.String, realBlockedBy.String, blockingPids.String, sessionEntries.String, allBackends.String, result)
	}
}

// RunIsolationTests runs PostgreSQL isolation tests against multigateway.
// Isolation tests exercise multi-connection concurrency (deadlocks, serialization
// anomalies, lock contention, concurrent DDL) using isolationtester.
//
// directPgPort is the primary's direct PostgreSQL port; it's used to install
// the public.multigres_test_session_is_blocked shim that the patched
// isolationtester binary calls (see patchIsolationtester for the source-side
// rewrite that retargets the wait query at the public function).
//
// The isolation Makefile has no installcheck-tests target, so for selective tests
// we invoke pg_isolation_regress directly with test names as positional args.
func (pb *PostgresBuilder) RunIsolationTests(t *testing.T, ctx context.Context, multigatewayPort, directPgPort int, password string) (*TestResults, error) {
	t.Helper()

	t.Logf("Running PostgreSQL isolation tests against multigateway on port %d (harness db=postgres)...", multigatewayPort)

	// Install the lock-detection shim on PostgreSQL directly (bypassing
	// multigateway). Both the selective (PGISOLATION_TESTS) and full-suite
	// paths force --dbname=postgres on pg_isolation_regress (see the cmd
	// construction below), and multipooler routes every query to the
	// postgres DB anyway, so the shim only needs to live there.
	if err := pb.installPIDMappingFunction(t, directPgPort, password); err != nil {
		t.Logf("Warning: Failed to install isolation lock-detection shim: %v", err)
		t.Logf("Isolation tests that rely on lock detection (deadlock, etc.) may fail")
	}

	isolationBuildDir := filepath.Join(pb.BuildDir, "src", "test", "isolation")
	isolationSourceDir := filepath.Join(pb.SourceDir, "src", "test", "isolation")
	outputIsoDir := filepath.Join(isolationBuildDir, "output_iso")

	if err := os.MkdirAll(outputIsoDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output_iso directory: %w", err)
	}

	pgIsoRegress := filepath.Join(isolationBuildDir, "pg_isolation_regress")
	if _, err := os.Stat(pgIsoRegress); os.IsNotExist(err) {
		t.Logf("Building pg_isolation_regress...")
		buildCmd := executil.Command(ctx, "make", "-C", isolationBuildDir, "all")
		if out, err := buildCmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("failed to build pg_isolation_regress: %w\n%s", err, out)
		}
	}

	var cmd *executil.Cmd
	if testsEnv := os.Getenv("PGISOLATION_TESTS"); testsEnv != "" {
		args := []string{
			"--inputdir=" + isolationSourceDir,
			"--outputdir=" + outputIsoDir,
			"--host=localhost",
			fmt.Sprintf("--port=%d", multigatewayPort),
			"--user=postgres",
			"--dbname=postgres",
			"--use-existing",
			"--dlpath=" + isolationBuildDir,
		}
		args = append(args, strings.Fields(testsEnv)...)
		cmd = executil.Command(ctx, pgIsoRegress, args...).WithProcessGroup()
		t.Logf("Running selective isolation tests: %s", testsEnv)
	} else {
		cmd = executil.Command(ctx, "make", "-C", isolationBuildDir, "installcheck",
			"EXTRA_REGRESS_OPTS=--use-existing --dbname=postgres").WithProcessGroup()
		t.Logf("Running full PostgreSQL isolation test suite (installcheck)")
	}

	results, runErr := pb.runTestSuite(t, ctx, cmd, testSuiteConfig{
		suiteName: "Isolation",
		outputDir: filepath.Join(pb.OutputDir, "isolation"),
		srcOutDir: outputIsoDir,
	}, multigatewayPort, password)

	// Post-suite diagnostic: dump the last entries of isolation_debug_log
	// so investigators can see what the shim observed (or didn't) for
	// hung specs. The table lives in the postgres DB on the primary;
	// query it directly to bypass any multigateway routing that a
	// failing wait-query would have used.
	pb.dumpIsolationDebugLog(t, directPgPort, password)

	return results, runErr
}
