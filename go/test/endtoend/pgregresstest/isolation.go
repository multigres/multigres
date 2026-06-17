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

	"github.com/multigres/multigres/go/common/multigresschema"
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
//  2. Lock-wait probe function name. Upstream prepares
//     `pg_catalog.pg_isolation_test_session_is_blocked(...)`. Replacing
//     that builtin C function with a PL/pgSQL shim via CREATE OR REPLACE
//     proved unreliable (fresh backends were observed to still bind the
//     C entry, returning false for every probe and hanging every
//     blocking spec at max_step_wait). We point the harness at our own
//     function `public.multigres_test_session_is_blocked` (installed by
//     installPIDMappingFunction) with explicit arg casts so PG resolves
//     it under extended-protocol Parse with paramTypes=NULL.
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
		"\t\t * cannot use PQexecParams + current_setting() here.\n" +
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
		"\t\t\tescaped = PQescapeLiteral(conns[i].conn, combined, strlen(combined));\n" +
		"\t\t\tfree(combined);\n" +
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

	// Add explicit type casts on both args. isolationtester PQprepares the
	// wait-query with paramTypes=NULL so $1 enters parse as UNKNOWN, and the
	// '{...}' literal is also UNKNOWN. PG resolves this for the pg_catalog
	// C builtin via implicit catalog priority but fails for a public
	// PL/pgSQL function with "function public.X(unknown, unknown) does not
	// exist". Casting to int4 / int4[] removes the ambiguity.
	waitFnOrig := "\"SELECT pg_catalog.pg_isolation_test_session_is_blocked($1, '{\""
	waitFnReplacement := "\"SELECT public.multigres_test_session_is_blocked($1::int4, '{\""
	if !bytes.Contains(patched, []byte(waitFnOrig)) {
		return fmt.Errorf("%s: wait-query function reference not found (PG version drift?)", rel)
	}
	patched = bytes.Replace(patched, []byte(waitFnOrig), []byte(waitFnReplacement), 1)

	waitFnSuffixOrig := "appendPQExpBufferStr(&wait_query, \"}')\");"
	waitFnSuffixReplacement := "appendPQExpBufferStr(&wait_query, \"}'::int4[])\");"
	if !bytes.Contains(patched, []byte(waitFnSuffixOrig)) {
		return fmt.Errorf("%s: wait-query suffix not found (PG version drift?)", rel)
	}
	patched = bytes.Replace(patched, []byte(waitFnSuffixOrig), []byte(waitFnSuffixReplacement), 1)

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
// The shim mirrors the upstream builtin: returns true if check_pid is
// waiting on any pid in blocked_by, considering both heavyweight lock
// waits (pg_blocking_pids) and SSI safe-snapshot waits
// (pg_safe_snapshot_blocking_pids — required for SERIALIZABLE READ ONLY
// DEFERRABLE specs such as read-only-anomaly-3). Both inputs are
// multigateway virtual pids; we map them to real PostgreSQL backend pids
// via the multigres.backend_vpid table, which the multipooler upserts
// whenever it hands a backend to a gateway session (the row commits in
// autocommit before any BEGIN, so it is visible to this probe for the
// whole transaction). The row is deleted when the backend is released or
// recycled, so the table represents active gateway-to-backend associations.
// The table is also created here, idempotently and ahead of the suite, so the
// shim never races the pooler's own lazy creation. The wait-check aggregates
// over every matching backend rather than picking one non-deterministically;
// rows of dead backends are ignored via the join against pg_stat_activity as a
// defensive fallback for abrupt connection loss.
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
		// The vpid mapping table the multipooler upserts into. Created here
		// too (idempotent DDL shared via multigresschema) so the shim below
		// can reference it even before the pooler's own lazy creation ran.
		multigresschema.BackendVpidDDL,
		// Debug table: every shim invocation logs its inputs/outputs and a
		// snapshot of every backend in this DB so failures can be diagnosed
		// post-hoc by querying isolation_debug_log (see
		// dumpIsolationDebugLog).
		`CREATE TABLE IF NOT EXISTS public.isolation_debug_log (
			id serial PRIMARY KEY,
			ts timestamptz DEFAULT now(),
			check_pid int4,
			blocked_by int4[],
			real_check_pid int4,
			real_blocked_by int4[],
			blocking_pids int4[],
			vpid_entries text[],
			all_pg_backends text[],
			result boolean
		)`,
		// Non-destructive add for runs against a pre-existing table from an
		// earlier shim version that lacked all_pg_backends.
		`ALTER TABLE public.isolation_debug_log
		   ADD COLUMN IF NOT EXISTS all_pg_backends text[]`,
		`TRUNCATE public.isolation_debug_log`,
		`DROP FUNCTION IF EXISTS public.multigres_test_session_is_blocked(int4, int4[])`,
		`CREATE FUNCTION public.multigres_test_session_is_blocked(check_pid int4, blocked_by int4[])
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
<<fn>>
DECLARE
    v_log_id int4;
    v_real_check_pid int4;
    v_real_blocked_by int4[];
    v_blocking_pids int4[];
    v_vpid_entries text[];
    v_all_backends text[];
    v_stamp_found boolean;
    v_result boolean;
BEGIN
    -- Capture the inserted id directly so the later UPDATE targets THIS
    -- invocation's row even under concurrent shim calls (parallel groups
    -- in the isolation schedule, or multiple sessions polling at once).
    -- max(id) would race against any concurrent INSERT in between.
    INSERT INTO public.isolation_debug_log
        (check_pid, blocked_by, result)
    VALUES
        (check_pid, blocked_by, NULL)
    RETURNING id INTO v_log_id;

    -- Diagnostic snapshot of the mapping table, restricted to live
    -- backends as a defensive fallback for abrupt connection loss.
    SELECT array_agg(bv.vpid || '=' || bv.backend_pid)
    INTO v_vpid_entries
    FROM multigres.backend_vpid bv
    JOIN pg_stat_activity sa ON sa.pid = bv.backend_pid;

    SELECT array_agg(sa.pid || ':' || COALESCE(sa.application_name,'<null>') || ':' || COALESCE(sa.state,'<null>'))
    INTO v_all_backends
    FROM pg_stat_activity sa
    WHERE sa.datname = current_database();

    -- A client vpid should normally map to one live PG backend. Aggregate
    -- anyway so transient hand-off overlap or duplicate mappings cannot make
    -- the wait check pick a non-blocked backend non-deterministically.
    -- real_check_pid is kept for diagnostic display only; the actual block
    -- check below aggregates over every matching backend.
    SELECT bv.backend_pid INTO v_real_check_pid
    FROM multigres.backend_vpid bv
    JOIN pg_stat_activity sa ON sa.pid = bv.backend_pid
    WHERE bv.vpid = check_pid
    LIMIT 1;

    SELECT array_agg(bv.backend_pid) INTO v_real_blocked_by
    FROM multigres.backend_vpid bv
    JOIN pg_stat_activity sa ON sa.pid = bv.backend_pid
    WHERE bv.vpid = ANY(blocked_by);

    -- Direct connections (no multigateway) hand us real pids; preserve them.
    v_stamp_found := v_real_check_pid IS NOT NULL;
    v_real_check_pid := COALESCE(v_real_check_pid, check_pid);
    v_real_blocked_by := COALESCE(v_real_blocked_by, blocked_by);

    -- Aggregate heavyweight lock blockers and SSI safe-snapshot blockers
    -- across every live PG backend currently mapped to this vpid. SSI
    -- safe-snapshot wait is required for SERIALIZABLE READ ONLY
    -- DEFERRABLE specs (e.g. read-only-anomaly-3). Aggregation handles any
    -- transient duplicate mapping conservatively.
    --
    -- The direct-pid fallback only fires when no mapping was found for
    -- check_pid. vpids occupy the full 31-bit signed int32 space, so a
    -- vpid value can coincidentally equal an unrelated real PG backend
    -- PID; probing check_pid as a real pid unconditionally would surface
    -- that unrelated backend's blockers and risk a false positive.
    SELECT COALESCE(array_agg(DISTINCT b), '{}'::int4[]) INTO v_blocking_pids
    FROM (
        SELECT unnest(pg_blocking_pids(bv.backend_pid)) AS b
        FROM multigres.backend_vpid bv
        JOIN pg_stat_activity sa ON sa.pid = bv.backend_pid
        WHERE bv.vpid = check_pid
        UNION ALL
        SELECT unnest(pg_safe_snapshot_blocking_pids(bv.backend_pid)) AS b
        FROM multigres.backend_vpid bv
        JOIN pg_stat_activity sa ON sa.pid = bv.backend_pid
        WHERE bv.vpid = check_pid
        UNION ALL
        SELECT unnest(pg_blocking_pids(check_pid)) AS b WHERE NOT v_stamp_found
        UNION ALL
        SELECT unnest(pg_safe_snapshot_blocking_pids(check_pid)) AS b WHERE NOT v_stamp_found
    ) sub
    WHERE b IS NOT NULL;

    v_result := v_blocking_pids && v_real_blocked_by;

    UPDATE public.isolation_debug_log
    SET real_check_pid = v_real_check_pid,
        real_blocked_by = v_real_blocked_by,
        blocking_pids = v_blocking_pids,
        vpid_entries = v_vpid_entries,
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
	if err := db.QueryRow(`SELECT count(*) FROM public.isolation_debug_log WHERE check_pid > 0`).Scan(&total); err != nil {
		t.Logf("isolation_debug_log dump: count failed: %v", err)
		return
	}
	t.Logf("isolation_debug_log: %d shim invocations recorded during run", total)
	if total == 0 {
		t.Logf("isolation_debug_log: shim never executed — wait-query is not reaching public.multigres_test_session_is_blocked")
		return
	}

	rows, err := db.Query(`
		SELECT id, check_pid, blocked_by, real_check_pid, real_blocked_by,
		       blocking_pids, vpid_entries, all_pg_backends, result
		FROM public.isolation_debug_log
		WHERE check_pid > 0
		ORDER BY id DESC
		LIMIT 30`)
	if err != nil {
		t.Logf("isolation_debug_log dump: select failed: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, checkPid int
		var blockedBy, realBlockedBy, blockingPids, vpidEntries, allBackends sql.NullString
		var realCheckPid sql.NullInt32
		var result sql.NullBool
		if err := rows.Scan(&id, &checkPid, &blockedBy, &realCheckPid, &realBlockedBy, &blockingPids, &vpidEntries, &allBackends, &result); err != nil {
			t.Logf("isolation_debug_log dump: scan failed: %v", err)
			continue
		}
		t.Logf("isolation_debug_log id=%d check_pid=%d blocked_by=%s real_check=%v real_blocked=%s blocking=%s vpids=%s all=%s result=%v",
			id, checkPid, blockedBy.String, realCheckPid, realBlockedBy.String, blockingPids.String, vpidEntries.String, allBackends.String, result)
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
		t.Logf("Warning: Failed to install PID mapping function: %v", err)
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
