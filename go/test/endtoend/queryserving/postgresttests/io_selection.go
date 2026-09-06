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

package postgresttests

import (
	"os"
	"strings"
)

// PostgREST's test/io is a pytest suite that spawns real postgrest binaries and
// drives them over HTTP. Most of it validates PostgREST-internal behaviour (its
// CLI, admin server, structured logging, config-reload machinery) that has
// nothing to do with the proxied DB path — running those through the gateway
// would only test PostgREST, not multigres. So we deliberately run a curated
// subset: the tests that exercise the DB-interaction path we proxy (role
// settings hoisted per request, statement_timeout, function/hoisted tx
// settings, prepared statements). See io_tests.md for the full include/skip
// rationale.
//
// The ★ tests below exercise the per-request `set_config($1, $2, true)` PostgREST
// emits to apply a role's settings (e.g. a `statement_timeout` set via ALTER
// ROLE) through the gateway.

// ioTest is one selected pytest node plus why it's in scope. The `why` is
// documentation only (surfaced in io_tests.md), not used at runtime.
type ioTest struct {
	Node string // pytest node id relative to the container /src (repo root)
	Why  string
}

// ioSelectedTests is the proxy-relevant subset of test/io run through the
// gateway. All of these must pass on direct PostgreSQL (the asserted baseline);
// a failure through the gateway is a divergence. Kept intentionally tight and
// free of wall-clock-timing assertions so the signal stays trustworthy — the
// pooling/timing tests (test_pool_size, test_pool_acquisition_timeout,
// test_change_statement_timeout_held_connection) are left out of the default
// set because their sub-second timing bounds are flaky across the extra proxy
// hop; opt in with POSTGREST_IO_SELECT if you want them.
var ioSelectedTests = []ioTest{
	// ── Role settings hoisted per request ───────────────────────────────────
	{"test/io/test_io.py::test_role_settings", "★ per-role statement_timeout hoisted via set_config($1,$2,true)"},
	{"test/io/test_io.py::test_statement_timeout", "★ ALTER ROLE ... SET statement_timeout, then hoisted per request; slow stmt must cancel"},
	{"test/io/test_io.py::test_work_mem_in_role_settings", "role-level work_mem hoisted per request (upstream #4955)"},
	{"test/io/test_io.py::test_isolation_level", "default_transaction_isolation hoisted per role and per function"},

	// ── Function / hoisted-transaction settings ─────────────────────────────
	{"test/io/test_io.py::test_function_setting_statement_timeout_fails", "function-local SET statement_timeout applied in the request tx"},
	{"test/io/test_io.py::test_function_setting_statement_timeout_passes", "function-local SET statement_timeout applied in the request tx"},
	{"test/io/test_io.py::test_function_setting_work_mem", "function-local SET work_mem hoisted into the request tx"},
	{"test/io/test_io.py::test_multiple_func_settings", "multiple hoisted tx settings (work_mem + statement_timeout) applied together"},
	{"test/io/test_io.py::test_first_hoisted_setting_is_applied", "only the configured hoisted setting is applied, the other stays default"},
	{"test/io/test_io.py::test_second_hoisted_setting_is_applied", "only the configured hoisted setting is applied, the other stays default"},

	// ── Superuser-granted role settings (GRANT SET ON PARAMETER) ─────────────
	{"test/io/test_io.py::test_succeed_w_role_having_superuser_settings", "impersonated role carrying superuser-only settings must not error"},
	{"test/io/test_io.py::test_get_granted_superuser_setting", "GRANT SET ON PARAMETER lets a granted superuser setting be hoisted"},

	// ── Prepared statements through the pooler ──────────────────────────────
	{"test/io/test_io.py::test_db_prepared_statements_enable", "prepared statements used when enabled (pooler must preserve them)"},

	// ── LISTEN/NOTIFY-triggered config & schema reload ──────────────────────
	// PostgREST LISTENs on the "pgrst" channel; fixture RPCs pg_notify it to
	// trigger a config/schema reload. Both the LISTEN and the NOTIFY traverse the
	// gateway/pooler, so these exercise async notification delivery end to end.
	{"test/io/test_io.py::test_db_schema_notify_reload", "NOTIFY 'reload config' delivered through the gateway makes PostgREST re-read db-schemas"},
	{"test/io/test_io.py::test_max_rows_notify_reload", "NOTIFY 'reload config' delivered through the gateway makes PostgREST re-read db-max-rows"},
	{"test/io/test_io.py::test_invalid_role_claim_key_notify_reload", "NOTIFY 'reload config' delivered through the gateway (asserts PostgREST logs the received message)"},
	{"test/io/test_io.py::test_notify_do_nothing", "an unrecognized NOTIFY payload on the pgrst channel is delivered and harmlessly ignored"},
	// Currently an OPEN DIVERGENCE (fails through the gateway) — see io_tests.md.
	// After DROP+CREATE of a table + NOTIFY reload, PostgREST's next query still
	// uses the pre-DDL parameter type because the multipooler's backend prepared
	// statement (deduped by query text) is never re-parsed or closed after a DDL.
	{"test/io/test_io.py::test_notify_reloading_catalog_cache", "NOTIFY 'reload schema' after DROP+CREATE of a table; the recreated relation must be queryable through the pooler on the next request"},
}

// ioSkipNotes documents whole test files / groups we deliberately do NOT run,
// with the reason. Rendered into io_tests.md; not consumed at runtime.
var ioSkipNotes = []struct{ What, Why string }{
	{"test_cli.py", "PostgREST CLI parsing/--dump-config/--example — no DB path"},
	{"test_auth.py", "JWT/JWKS auth internals in PostgREST — not the proxied query path"},
	{"test_sanity.py", "harness self-checks, not multigres behaviour"},
	{"test_big_schema.py", "needs the 392KB big_schema.sql fixture + long introspection; separate concern"},
	{"test_replica.py", "needs a streaming replica wired into PostgREST's PGRST_DB_URI; separate concern"},
	{"test_io.py: admin/live/ready/metrics/*", "PostgREST admin server + Prometheus metrics — internal to PostgREST"},
	{"test_io.py: log_level/log_query/db_error_logging/*", "PostgREST structured-logging format — internal to PostgREST"},
	{"test_io.py: app_settings_reload/max_rows_reload/db_schema_reload", "SIGUSR2/SIGUSR1 signal-driven reload — PostgREST-internal, no DB path (the NOTIFY-driven variants ARE run — see the LISTEN/NOTIFY block above)"},
	{"test_io.py: db_prepared_statements_disable", "asserts NO server-side prepared statements when the client disables them; multigres does not support turning off prepared statements through the pooler — won't-fix, so not a tracked divergence"},
	{"test_io.py: connect_with_dburi/read_dburi/get_pgrst_version_*_connection_string", "build a socket-style DB URI with no port/password — incompatible with the TCP+password gateway repoint"},
	{"test_io.py: preflight/CORS/proxy_status_header", "HTTP response-header behaviour internal to PostgREST"},
	{"test_io.py: pool_size/pool_acquisition_timeout/change_statement_timeout_held_connection", "assert sub-second wall-clock bounds — flaky across the extra proxy hop; opt in via POSTGREST_IO_SELECT"},
	{"test_io.py: change_statement_timeout", "reconfigures timeout_authenticator's statement_timeout mid-run and depends on SIGUSR1 reload timing + cross-test role state not leaking; ordering-fragile on a shared cluster. The static form (test_statement_timeout) already covers the per-request role-hoisting path. Opt in via POSTGREST_IO_SELECT"},
}

// ioTestArgs returns the pytest node ids to run. POSTGREST_IO_SELECT overrides
// the curated list with a comma- or space-separated set of node ids (or a
// pytest -k expression prefixed with "-k "), e.g. to iterate on one test or to
// opt into the timing-sensitive group.
func ioTestArgs() []string {
	if sel := strings.TrimSpace(os.Getenv("POSTGREST_IO_SELECT")); sel != "" {
		if k, ok := strings.CutPrefix(sel, "-k "); ok {
			return []string{"test/io", "-k", strings.TrimSpace(k)}
		}
		return strings.Fields(strings.ReplaceAll(sel, ",", " "))
	}
	nodes := make([]string, 0, len(ioSelectedTests))
	for _, t := range ioSelectedTests {
		nodes = append(nodes, t.Node)
	}
	return nodes
}
