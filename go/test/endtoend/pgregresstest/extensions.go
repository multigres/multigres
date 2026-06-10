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
	"fmt"
	"sort"
	"strings"
)

// ExtKind is where an extension's code lives.
type ExtKind string

const (
	// KindContrib: the extension ships in the PostgreSQL source tree under
	// contrib/, so the pgregress harness can build and run its suite directly.
	KindContrib ExtKind = "contrib"
	// KindExternal: the extension lives in a separate repository (not in the
	// PostgreSQL source tree) and needs bespoke build infrastructure (clone its
	// repo at a pinned tag, build it as a PGXS module against the from-source
	// PostgreSQL, then run its shipped pg_regress suite — see externalSpecs and
	// Builder.InstallExternalExtension).
	KindExternal ExtKind = "external"
)

// ExtStatus is the test-coverage state of an extension in this harness.
type ExtStatus string

const (
	// StatusCovered: wired into a suite that runs the extension's pg_regress
	// tests through multigateway. For KindContrib that is the contrib suite
	// (ExtensionCatalog → CoveredContribModules); for KindExternal it is the
	// external suite (CoveredExternalExtensions, backed by externalSpecs).
	StatusCovered ExtStatus = "covered"
	// StatusPending: a core-contrib module with a pg_regress suite that this
	// harness could run but has not been wired up yet.
	StatusPending ExtStatus = "pending"
	// StatusUnsupported: cannot be covered by this harness (see Note for the
	// reason, e.g. pooler-incompatible or no installable pg_regress suite).
	StatusUnsupported ExtStatus = "unsupported"
	// StatusExternal: not in the PostgreSQL source tree; tracked as future work.
	StatusExternal ExtStatus = "external"
)

// ExtensionInfo is one extension's entry in the coverage catalog.
type ExtensionInfo struct {
	Name   string
	Kind   ExtKind
	Status ExtStatus
	// Note explains a non-covered status (reason) or a covered module's build
	// requirement. Empty when self-explanatory.
	Note string
}

// ExtensionCatalog is the coverage map for common PostgreSQL extensions. It is
// not the full pg_available_extensions list.
//
// The covered set is the source of truth for what the contrib suite runs
// (CoveredContribModules derives DefaultContribModules from it), so adding an
// extension here as StatusCovered is all that is needed to enroll it — the
// generated coverage report then reflects its per-test results automatically.
//
// Keep entries ordered by Name for easy diffing.
var ExtensionCatalog = []ExtensionInfo{
	{"btree_gin", KindContrib, StatusCovered, ""},
	{"btree_gist", KindContrib, StatusCovered, ""},
	{"citext", KindContrib, StatusCovered, ""},
	{"cube", KindContrib, StatusCovered, ""},
	{"dblink", KindContrib, StatusUnsupported, "pooler blocks outbound connections"},
	{"earthdistance", KindContrib, StatusCovered, "depends on cube"},
	{"fuzzystrmatch", KindContrib, StatusCovered, ""},
	{"hstore", KindContrib, StatusCovered, ""},
	{"http", KindExternal, StatusUnsupported, "suite needs a live HTTP echo server (local httpbin container, falling back to httpbin.org) — not deterministic without provisioning one"},
	{"hypopg", KindExternal, StatusUnsupported, "built as index_advisor's dependency (externalSpecs), but its own suite is not runnable through a transaction pooler: hypothetical indexes are backend-local and the tests are autocommit, so consecutive statements land on different pooled backends and the indexes vanish between them"},
	{"index_advisor", KindExternal, StatusCovered, "Supabase index advisor; pure-SQL PGXS module; depends on hypopg (built via DependsOn). Its tests are BEGIN/ROLLBACK-wrapped, so hypopg's backend-local hypothetical indexes stay on the pinned backend"},
	{"ltree", KindContrib, StatusCovered, ""},
	{"moddatetime", KindContrib, StatusUnsupported, "contrib/spi ships no pg_regress suite"},
	{"pg_cron", KindExternal, StatusCovered, "Citus pg_cron; built as a PGXS module from externalSpecs; needs shared_preload_libraries (see testdata/pg17/external/pg_cron.conf)"},
	{"pg_graphql", KindExternal, StatusCovered, "Supabase pg_graphql; Rust/pgrx crate built with cargo-pgrx; loads test/fixtures.sql before its pg_regress suite"},
	{"pg_jsonschema", KindExternal, StatusUnsupported, "Rust; ships no SQL test suite — its tests are pgrx #[pg_test] functions that run inside their own private server, not through a client connection"},
	{"pg_net", KindExternal, StatusUnsupported, "background worker making live HTTP requests; tests are pytest-based, not pg_regress"},
	{"pg_partman", KindExternal, StatusCovered, "pgTAP suite run via psql (not pg_regress); needs pgtap + max_locks_per_transaction>=128 (see testdata/pg17/external/pg_partman.conf). Runs the transaction-wrapped tests only (top-level + test_pg17plus/ + test_no_search_path/); autocommit/procedure subfolders can't run through a transaction pooler — see runExternalPgTAP. Also pgmq's build dependency (pgmq.create_partitioned → create_parent)."},
	{"pg_stat_statements", KindContrib, StatusUnsupported, "NO_INSTALLCHECK; records query text the gateway rewrites"},
	{"pg_trgm", KindContrib, StatusCovered, ""},
	{"pgaudit", KindExternal, StatusUnsupported, "suite repeatedly \\connects as freshly created password-auth test users; pg_regress assumes trust auth on a temp instance, and a failed \\connect aborts psql, so the suite is not runnable through the gateway's auth"},
	{"pgcrypto", KindContrib, StatusCovered, "needs --with-ssl=openssl"},
	{"pgjwt", KindExternal, StatusCovered, "pure-SQL JWT extension; pgTAP suite (single BEGIN…ROLLBACK-wrapped test.sql) run via psql. Depends on pgcrypto (contrib) and pgtap; upstream never tags releases, so it is pinned to a commit"},
	{"pgmq", KindExternal, StatusCovered, "tembo-io/pgmq; pure-SQL queue built as a PGXS module from pgmq-extension/; partitioned-queue tests depend on pg_partman"},
	{"pgsodium", KindExternal, StatusCovered, "libsodium crypto wrapper (needs libsodium via pkg-config to build); pgTAP suite (single BEGIN…ROLLBACK-wrapped test.sql) run via psql in keyless mode — server-key/TCE tests self-skip via \\if :serverkeys since pgsodium is not in shared_preload_libraries"},
	{"pgtap", KindExternal, StatusCovered, "runs its own pg_regress suite (every test wrapped in BEGIN…ROLLBACK by test/setup.sql); extension.sql needs contrib citext/isn/ltree installed (ContribDeps). Also the test dependency of pg_partman, pgjwt, and pgsodium"},
	{"plpgsql", KindContrib, StatusUnsupported, "built-in PL; exercised by the core regression suite, not contrib"},
	{"plpgsql_check", KindExternal, StatusCovered, "plpgsql linter/profiler; needs shared_preload_libraries (PreloadLibraries) so the passive-mode hooks and shared-memory profiler work on every pooled backend; the gateway-blocked LOAD statements its tests open with are patched"},
	{"postgis", KindExternal, StatusExternal, ""},
	{"postgis_topology", KindExternal, StatusExternal, "PostGIS"},
	{"postgres_fdw", KindContrib, StatusUnsupported, "pooler blocks CREATE SERVER / outbound connections"},
	{"supabase_vault", KindExternal, StatusUnsupported, "autocommit pgTAP suite (no BEGIN/ROLLBACK wrapper): pgTAP's session-temp plan state can't survive a transaction pooler — see runExternalPgTAP"},
	{"unaccent", KindContrib, StatusCovered, ""},
	{"uuid-ossp", KindContrib, StatusCovered, "needs --with-uuid"},
	{"vector", KindExternal, StatusCovered, "pgvector; built as a PGXS module from externalSpecs"},
	{"wrappers", KindExternal, StatusExternal, "Rust"},
}

// TestHarness selects how an external extension's shipped test suite is
// executed. The zero value (HarnessPgRegress) drives the pg_regress binary and
// diffs each test's output against expected/*.out — the model the contrib suite
// and pgvector/pg_cron use. HarnessPgTAP instead feeds each test .sql to psql and
// parses the TAP stream the pgTAP assertions emit server-side; correctness is
// decided in-database (no expected-output files, no patch pipeline). Extensions
// like pg_partman ship pgTAP suites.
type TestHarness string

const (
	// HarnessPgRegress runs the extension's tests through pg_regress (default).
	HarnessPgRegress TestHarness = ""
	// HarnessPgTAP runs the extension's pgTAP tests through psql and parses TAP.
	HarnessPgTAP TestHarness = "pgtap"
)

// ExtensionInstall names an extension to CREATE before a pgTAP suite, with an
// optional target schema. When Schema is non-empty the harness creates that
// schema first and installs the extension into it (CREATE EXTENSION ... SCHEMA
// <Schema>); when empty the extension lands in the current schema (public). See
// ExternalExtension.PreCreateExtensions.
type ExtensionInstall struct {
	Name   string
	Schema string
}

// ExternalExtension describes one external (non-contrib) extension wired into
// the external suite: its catalog name, the git coordinates the harness clones
// and builds it from, and a few knobs for the places extensions diverge from
// the pgvector baseline (test layout, extension lifecycle, server config).
type ExternalExtension struct {
	Name string
	Repo string
	// Tag is the git tag the harness clones. Exactly one of Tag and Commit must
	// be set.
	Tag string

	// Commit pins a full commit SHA instead of a tag, for upstreams that never
	// tag releases (pgjwt's last release predates its tags entirely — the repo
	// has none). A SHA is as reproducible as a tag; see pgbuilder.cloneExtension.
	Commit string

	// BuildSubdir is the directory within the checkout that holds the PGXS
	// Makefile, relative to the clone root. pgvector and pg_cron keep it at the
	// repo root (""), so the harness builds there; pgmq keeps the extension under
	// pgmq-extension/, so it uses "pgmq-extension". Empty means the repo root.
	BuildSubdir string

	// Harness selects the test runner (see TestHarness). The zero value runs the
	// pg_regress path; HarnessPgTAP runs the psql+TAP path. Fields below tagged
	// "(pgTAP)" apply only to the HarnessPgTAP path; the patch pipeline applies only
	// to the pg_regress path; PreCreateExtensions is used by both.
	Harness TestHarness

	// TestGlobs (pgTAP) are the filename globs, relative to TestSubdir, selecting
	// the test files to run (their union, deduped). pgTAP suites ship flat *.sql
	// files rather than the pg_regress sql/+expected/ layout, so listRegressTests
	// does not apply. Defaults to ["*.sql"] when empty.
	//
	// pg_partman is restricted to its self-contained, transaction-wrapped tests:
	// the top-level test-*.sql plus the rolled-back tests under test_pg17plus/ and
	// test_no_search_path/. The OTHER subfolders are deliberately excluded, and the
	// reason is a hard limit of running pgTAP through a transaction pooler, not a
	// scoping whim — see the long note on runExternalPgTAP. In short: pgTAP keeps
	// its plan/results in session-temp tables that plan() creates *inside* a
	// function body, so the gateway can't observe them. Inside a BEGIN…ROLLBACK the
	// visible BEGIN pins the backend and the ROLLBACK discards that temp state, so
	// these tests are clean. The excluded folders run pgTAP in autocommit (their
	// procedures COMMIT, so they can't be wrapped): plan()'s temp table is then
	// created on an unpinned pooled backend and never discarded, leaking into the
	// next file as "You tried to plan twice!". They also need infrastructure the
	// pooled path can't provide — background workers (test_bgw/), tablespaces
	// (test_tablespace/), non-superuser roles (test_nonsuperuser/), or manual
	// multi-stage orchestration with out-of-band commits (test_procedure/).
	TestGlobs []string

	// ExcludeGlobs removes files the harness would otherwise select, matched
	// against the path relative to TestSubdir. On the pgTAP path it filters the
	// TestGlobs matches; on the pg_regress path it filters the derived
	// sql/*.sql wildcard (so entries look like "sql/<name>.sql"; it does not
	// apply when RegressTests pins an explicit list). Use it for a test that
	// runs cleanly but isn't a reliable signal — a date-calibrated expectation
	// that drifts with the calendar (pg_partman) — or one whose entire subject
	// is a pattern the gateway redefines by design — pgtap's prepared-statement
	// fixture files (see the pgtap spec below).
	ExcludeGlobs []string

	// PreCreateExtensions lists extensions to CREATE EXTENSION through multigateway,
	// in order, before the suite runs — each optionally into a specific schema. Used
	// by both harnesses for fixtures that assume an extension already exists:
	//   - pg_regress: pgvector's fixtures open with a bare CREATE TABLE ...
	//     vector(3) and never CREATE EXTENSION, so it lists {Name: "vector"}.
	//   - pgTAP: pg_partman's test files never CREATE EXTENSION; they expect pgtap
	//     in public and pg_partman in the `partman` schema, referencing partman.*
	//     explicitly.
	// The Schema field matters because pg_partman's control file is
	// relocatable=false with no `schema=` default, so CREATE EXTENSION without a
	// SCHEMA clause lands it in public (first in search_path) and every
	// schema-qualified partman.* reference then fails with "schema partman does not
	// exist". The pgTAP path tears these down after its suite (see runExternalPgTAP);
	// the pg_regress path relies on resetContribState clearing public before the
	// next extension.
	PreCreateExtensions []ExtensionInstall

	// TestSubdir is the directory within the checkout that holds the shipped
	// pg_regress fixtures (sql/ + expected/), relative to the clone root.
	// pgvector keeps them under test/; pg_cron keeps them at the repo root, so
	// it uses "." (filepath.Join collapses it back to the clone root). pgmq keeps
	// them under pgmq-extension/test, alongside its BuildSubdir.
	TestSubdir string

	// ExpectedSubdir is the directory within the checkout that holds the
	// expected/ output files, when it differs from TestSubdir. Empty means
	// TestSubdir (the common layout: sql/ and expected/ side by side). hypopg
	// splits them — sql under test/sql but expected at the repo root — because
	// pg_regress's --expecteddir defaults to the CWD (the module root under
	// `make installcheck`), not to --inputdir. Mirrored here so the patch
	// pipeline diffs against the right files.
	ExpectedSubdir string

	// RegressTests, when non-empty, is the explicit pg_regress test list — a
	// mirror of the extension's REGRESS Makefile variable — used instead of
	// deriving the list from <TestSubdir>/sql/*.sql. Needed when the wildcard
	// convention doesn't hold: plpgsql_check ships per-major-version test files
	// (plpgsql_check_active-14 … -19) and its Makefile selects only the pair
	// matching $(MAJORVERSION), so globbing would run other majors' tests against
	// a PG17 server. Keep in sync with the pinned tag's Makefile.
	RegressTests []string

	// ScratchDatabases names databases the harness creates directly on the
	// primary (bypassing multigateway, like the public-schema reset) before the
	// suite runs and drops afterward. This is a TEST-ONLY accommodation, NOT a
	// product capability: multigres is one-database-per-postgres-instance by
	// design (multigateway blocks CREATE/DROP DATABASE as Tier 2 statements;
	// adding a database is a provisioning operation that brings up a new
	// cluster, see docs/query_serving/unsafe_statement_rejection.md). pg_cron's
	// test, though, uses other databases purely as *metadata*: it passes their
	// names to cron.schedule_in_database / cron.alter_job(database := ...) and
	// reads their ACLs from the shared pg_database catalog, all over the same
	// `postgres` connection — it never opens a session against them (nothing in
	// the multigres stack does; only pg_cron's in-process launcher would, and
	// the test doesn't wait for it). So creating the physical databases here is
	// enough to make those catalog/privilege checks run for real, while the
	// test's own CREATE/DROP DATABASE statements still hit the gateway block
	// (the only lines left in the patch). Reusable for any extension whose suite
	// references databases by name without connecting to them.
	ScratchDatabases []string

	// ServerConfigFile, when non-empty, names a postgresql.conf snippet under
	// testdata/pg<major>/external/ that the cluster must apply before postgres
	// starts (appended at initdb time, last-write-wins over the template). Use
	// it for extensions that need server-level configuration the pooled query
	// path can't set, e.g. pg_partman's max_locks_per_transaction. Empty for
	// extensions that need nothing beyond the stock cluster (pgvector).
	//
	// Do NOT put shared_preload_libraries in these snippets: each GUC is
	// last-write-wins across appended snippets, so two extensions' files would
	// silently clobber each other's library list. Use PreloadLibraries instead,
	// which the harness merges into one generated snippet.
	ServerConfigFile string

	// PreloadLibraries names the shared libraries this extension needs in
	// shared_preload_libraries before postgres starts. The harness takes the
	// union across the selected extensions and writes a single generated
	// shared_preload_libraries line (see externalServerConfPaths), because the
	// GUC is one list and snippet files would clobber each other. pg_cron's
	// background worker can't start without it; plpgsql_check needs it so its
	// passive-mode hooks and shared-memory profiler are active on every pooled
	// backend (a session-level LOAD would only affect one backend, and the
	// gateway blocks LOAD anyway).
	PreloadLibraries []string

	// PkgConfigDeps names pkg-config packages whose headers/libs the PGXS build
	// needs (pgsodium: libsodium). Resolved to -I/-L flags at build time; see
	// pgbuilder.ExtensionBuildSpec.PkgConfigDeps.
	PkgConfigDeps []string

	// DependsOn names other externalSpecs the harness must clone, build, and
	// install before this extension's suite runs, because the suite CREATEs those
	// extensions too. They are build-only: installed so CREATE EXTENSION resolves,
	// but not tested on their own (they need not ship a pg_regress suite). pgmq's
	// base.sql creates partitioned queues via pg_partman's create_parent, so pgmq
	// DependsOn pg_partman. ExternalBuildList orders dependencies before the
	// extensions that need them. Empty for self-contained extensions (pgvector).
	DependsOn []string

	// BuildSystem selects the build toolchain: "" (or "pgxs") builds a PGXS
	// module with make; "pgrx" builds a Rust crate with cargo-pgrx. pgvector,
	// pg_cron, and pgmq are PGXS; pg_graphql is pgrx.
	BuildSystem string

	// PgrxVersion pins the cargo-pgrx CLI version for BuildSystem=="pgrx". It must
	// equal the crate's pinned pgrx dependency (pg_graphql 1.6.1 → pgrx 0.16.1),
	// or cargo-pgrx refuses to build. Empty (and ignored) for PGXS extensions.
	PgrxVersion string

	// ContribDeps names contrib modules (by directory name) the harness must
	// install before this extension's suite runs, because the suite CREATEs them.
	// Unlike DependsOn (external repos), these ship in the PostgreSQL source tree
	// and are installed with InstallContribModules. pg_graphql's tests
	// `create extension citext`, so it sets ContribDeps: {"citext"}; without this
	// an external-only run (no contrib suite) fails those tests with "extension
	// citext is not available". Harmless in a full run where all contrib is
	// already installed.
	ContribDeps []string

	// FixturesFile, when non-empty, names a SQL file (relative to TestSubdir) the
	// harness loads through multigateway with psql before pg_regress runs, the way
	// the extension's own runner does. pg_graphql's bin/installcheck loads
	// test/fixtures.sql (it CREATEs the extension and sets the graphql schema
	// comment) before the suite, so the fixtures must run first here too. Empty
	// for extensions whose .sql files are self-contained (pgmq, pgvector).
	FixturesFile string
}

// externalSpecs holds the build coordinates (git repo + pinned tag) and the
// per-extension knobs for every external extension the harness can build. An
// ExtensionCatalog entry with Kind==KindExternal can only be StatusCovered if it
// also has a spec here; the pinned tag keeps the suite reproducible (and matches
// the ABI the from-source PostgreSQL was built against). Keyed by catalog Name.
var externalSpecs = map[string]ExternalExtension{
	"vector": {
		Name: "vector", Repo: "https://github.com/pgvector/pgvector", Tag: "v0.8.1",
		// pgvector's fixtures assume the extension already exists (they open with a
		// bare CREATE TABLE ... vector(3) and never CREATE EXTENSION), so preload it.
		TestSubdir: "test", PreCreateExtensions: []ExtensionInstall{{Name: "vector"}},
	},
	"pg_graphql": {
		Name: "pg_graphql", Repo: "https://github.com/supabase/pg_graphql", Tag: "v1.6.1",
		// Rust crate built with cargo-pgrx; the pgrx version must match the crate's
		// pinned dependency (Cargo.toml: pgrx = "=0.16.1"). Build entry point and
		// fixtures are at the repo root / test/.
		BuildSystem: "pgrx", PgrxVersion: "0.16.1", TestSubdir: "test",
		// test/fixtures.sql opens with `drop extension if exists pg_graphql;
		// create extension pg_graphql cascade;` and sets the graphql schema
		// comment, so the harness loads it first (FixturesFile) and must not also
		// preload the extension itself (PreCreateExtensions left empty).
		FixturesFile: "fixtures.sql",
		// Several tests `create extension citext` — install it first (see
		// ContribDeps), or they fail with "extension citext is not available".
		ContribDeps: []string{"citext"},
		// resolve_error_mutation_no_field carries a patch: a pg_graphql
		// backend-local schema-cache staleness that shows on reused pooled
		// backends — not a multigres bug. Full rationale is in that patch file's
		// header comment (testdata/pg17/patches/external/pg_graphql/).
	},
	"pg_cron": {
		Name: "pg_cron", Repo: "https://github.com/citusdata/pg_cron", Tag: "v1.6.4",
		// pg_cron's fixtures manage the extension themselves (CREATE EXTENSION pg_cron
		// VERSION '1.0' is the first statement, then they DROP and recreate it at a
		// newer version), so PreCreateExtensions is left empty to avoid colliding.
		// The background worker (job launcher) can only start when the library is
		// preloaded — CREATE EXTENSION pg_cron errors out otherwise; the conf snippet
		// carries the rest (cron.database_name).
		TestSubdir: ".", ServerConfigFile: "pg_cron.conf", PreloadLibraries: []string{"pg_cron"},
		// pg_cron-test.sql references pgcron_dbno/pgcron_dbyes by name (it REVOKEs
		// CONNECT on one and schedules/alters jobs targeting both) but never
		// connects to them; front-load them on the primary so those metadata and
		// CONNECT-privilege checks run for real. See ScratchDatabases.
		ScratchDatabases: []string{"pgcron_dbno", "pgcron_dbyes"},
	},
	// pgtap is both a test dependency (pg_partman, pgjwt, and pgsodium need the
	// pgtap extension installed before their suites run) and covered in its own
	// right: it ships a classic pg_regress suite under test/{sql,expected}.
	// Every test file starts with `\i test/setup.sql`, which opens a BEGIN that
	// the file ROLLBACKs at the end — so the whole file runs on one pinned
	// backend and pgTAP's session-temp plan state is consistent and discarded
	// (the same property that makes pg_partman's wrapped tests runnable; see
	// runExternalPgTAP). That `\i` is CWD-relative and runExternalRegress runs
	// pg_regress from the clone root, matching upstream's `make installcheck`.
	"pgtap": {
		Name: "pgtap", Repo: "https://github.com/theory/pgtap", Tag: "v1.3.4",
		TestSubdir: "test",
		// The tests never CREATE EXTENSION pgtap themselves (upstream's
		// installcheck machinery does it out of band), so preload it.
		PreCreateExtensions: []ExtensionInstall{{Name: "pgtap"}},
		// extension.sql CREATEs citext, isn, and ltree inside its transaction;
		// upstream's Makefile excludes the file when their control files are
		// missing. Install them so the test runs instead of being excluded.
		ContribDeps: []string{"citext", "isn", "ltree"},
		// Excluded: the files whose entire subject is passing a SQL-level prepared
		// statement NAME into pgTAP assertions (set_eq('mytest', …),
		// performs_ok('mytest', …)), which pgTAP implements as `EXECUTE mytest`
		// inside a plpgsql function. multigateway owns SQL-level PREPARE by design
		// (the statement lives in the gateway's consolidator under a canonical
		// name; the backend session never sees one named `mytest` — see
		// planner/execute_unwrap.go), and an EXECUTE inside a function body is
		// invisible to the gateway, so it fails with "prepared statement does not
		// exist". That first error aborts each file's single wrapping transaction,
		// so the rest of the file can't produce comparable output — a patch would
		// have to absorb the whole file, hiding real regressions. throwtap is NOT
		// excluded: its four prepared/execute assertions run inside throws_ok's
		// exception trap, so the file completes and a narrow patch documents just
		// those by-design failures.
		ExcludeGlobs: []string{
			"sql/performs_ok.sql",
			"sql/performs_within.sql",
			"sql/resultset.sql",
			"sql/valueset.sql",
		},
	},
	"pg_partman": {
		Name: "pg_partman", Repo: "https://github.com/pgpartman/pg_partman", Tag: "v5.4.3",
		Harness:    HarnessPgTAP,
		TestSubdir: "test",
		// The self-contained, transaction-wrapped tests only: the top-level
		// test-*.sql plus the rolled-back tests under test_pg17plus/ and
		// test_no_search_path/. The other subfolders are excluded — see TestGlobs.
		TestGlobs: []string{"test-*.sql", "test_pg17plus/*.sql", "test_no_search_path/*.sql"},
		// test-time-monthly-source-generated asserts an exact post-undo_partition
		// row count (ARRAY[91]) calibrated to a specific run date: the data spans a
		// fixed now()-relative 12-month window, but the monthly partition boundaries
		// and premake shift with the calendar, so undo_partition(p_loop_count=>20)
		// moves a date-dependent number of rows. It fails identically with and
		// without the gateway (74≠91 on 2026-06-08) — the test's own date assumption,
		// not a multigres behavior — so it's excluded from the deterministic set.
		ExcludeGlobs: []string{"test_pg17plus/test-time-monthly-source-generated.sql"},
		// pgtap (public) and pg_partman (partman schema) must both exist before any
		// test file runs — the files assume them and never CREATE EXTENSION. pgtap
		// goes in public; pg_partman MUST go in the `partman` schema (its tests
		// reference partman.* explicitly), so it carries an explicit Schema.
		DependsOn:           []string{"pgtap"},
		PreCreateExtensions: []ExtensionInstall{{Name: "pgtap"}, {Name: "pg_partman", Schema: "partman"}},
		// Subpartition tests create/drop several hundred tables in one transaction;
		// the default max_locks_per_transaction (64) risks a cluster crash. pgmq,
		// which DependsOn pg_partman, runs fine with this raised too.
		ServerConfigFile: "pg_partman.conf",
	},
	// hypopg is a build dependency only: index_advisor's tests `create extension
	// index_advisor cascade`, which pulls in hypopg (its control file requires
	// it). hypopg's OWN suite cannot run through the gateway: hypothetical
	// indexes live in backend-local memory and its tests are autocommit, so
	// hypopg_create_index and the EXPLAINs that should see the index land on
	// different pooled backends. index_advisor's tests dodge that by wrapping
	// everything in BEGIN…ROLLBACK (one pinned backend), which is why it is
	// covered and hypopg is not.
	"hypopg": {
		Name: "hypopg", Repo: "https://github.com/HypoPG/hypopg", Tag: "1.4.2",
	},
	"index_advisor": {
		Name: "index_advisor", Repo: "https://github.com/supabase/index_advisor", Tag: "v0.2.0",
		// Standard PGXS test layout (test/sql + test/expected, REGRESS_OPTS
		// --use-existing --inputdir=test). Every test file CREATEs the extension
		// itself (create extension index_advisor cascade) inside its transaction,
		// so nothing is preloaded.
		TestSubdir: "test",
		DependsOn:  []string{"hypopg"},
	},
	"plpgsql_check": {
		Name: "plpgsql_check", Repo: "https://github.com/okbob/plpgsql_check", Tag: "v2.9.1",
		// sql/ and expected/ live at the repo root (like pg_cron).
		TestSubdir: ".",
		// Mirror of the Makefile's REGRESS list for MAJORVERSION=17:
		// plpgsql_check_passive plpgsql_check_active plpgsql_check_active-17
		// plpgsql_check_passive-17 plpgsql_check_profiler. The sql/ dir also ships
		// -14…-19 files for other majors, so the wildcard derivation would run the
		// wrong ones; see RegressTests.
		RegressTests: []string{
			"plpgsql_check_passive",
			"plpgsql_check_active",
			"plpgsql_check_active-17",
			"plpgsql_check_passive-17",
			"plpgsql_check_profiler",
		},
		// The passive tests configure checking via the plpgsql_check.mode GUC and
		// the profiler test reads execution counters back — both need the library
		// active on EVERY pooled backend, and the profiler's counters must be in
		// shared memory (which plpgsql_check only uses when preloaded) so a read
		// from one backend sees executions counted on another. The tests' own
		// `load 'plpgsql_check'` would do neither through the pooler — it is
		// blocked by the gateway (patched as an extra ERROR line) and would only
		// affect a single backend anyway.
		PreloadLibraries: []string{"plpgsql_check"},
		// NOTE: this preload is exactly why the external server config is scoped
		// to the external phase's cluster (see externalServerConfPaths). Most of
		// plpgsql_check is inert when preloaded (mode defaults to by_function,
		// profiler/tracer to off) but cursors_leaks defaults to ON and emits
		// "cursor ... is not closed" WARNINGs that the core regression suite's
		// plpgsql test does not expect — and it cannot simply be disabled: with
		// the library preloaded and cursors_leaks turned off (conf or session),
		// any exception-trapping plpgsql function HANGS. That hang is an upstream
		// plpgsql_check bug, reproduced on stock PostgreSQL 17.6 (no multigres)
		// with both v2.9.1 and master.
	},
	// pgjwt's upstream has never tagged a release, so it is pinned to the current
	// HEAD commit (2023; the project is mature and dormant) — same
	// reproducibility as a tag, see ExternalExtension.Commit.
	"pgjwt": {
		Name: "pgjwt", Repo: "https://github.com/michelp/pgjwt",
		Commit:  "f3d82fd30151e754e19ce5d6a06c71c20689ce3d",
		Harness: HarnessPgTAP,
		// One pgTAP file at the repo root. It CREATEs pgcrypto, pgtap, and pgjwt
		// itself (in autocommit, on a public schema the harness has just reset),
		// then wraps all assertions in BEGIN … plan(23) … ROLLBACK — the
		// transaction-wrapped shape the pooled pgTAP path requires.
		TestSubdir: ".", TestGlobs: []string{"test.sql"},
		DependsOn: []string{"pgtap"},
		// pgcrypto ships in contrib and needs --with-ssl=openssl at configure time;
		// the harness enables that automatically when an extension lists pgcrypto
		// here (see TestPostgreSQLRegression).
		ContribDeps: []string{"pgcrypto"},
	},
	"pgsodium": {
		Name: "pgsodium", Repo: "https://github.com/michelp/pgsodium",
		// Pinned to a commit, not the last tag: v3.1.9 (2023) predates PostgreSQL
		// 17, whose automatic array types for composite types add entries to
		// pg_depend that the tag's "Check extension object list" fixture doesn't
		// expect, so the tag's suite fails against ANY PG17 server (gateway or
		// not). This commit is upstream main with the PG17 fixture fix; the
		// extension itself is still version 3.1.9 (the control file is unchanged).
		Commit:  "38d22897822191079bb494bd30af2ba37e32b3a0",
		Harness: HarnessPgTAP,
		// test/test.sql is the single entry point; it \ir-includes the per-API
		// files next to it (psql resolves \ir relative to the including file).
		// All assertions run inside one BEGIN … no_plan() … ROLLBACK.
		TestSubdir: "test", TestGlobs: []string{"test.sql"},
		// Keyless mode — pgsodium is deliberately NOT in shared_preload_libraries
		// and no server-key getkey script is provisioned. The suite detects that
		// (`\if :serverkeys` on pg_settings) and self-skips the server-key/TCE
		// sections; the pure-libsodium crypto APIs are what gets exercised.
		// test.sql does CREATE EXTENSION IF NOT EXISTS itself, but the extension is
		// also preloaded here so the pgTAP teardown drops it afterwards: its
		// control file pins schema=pgsodium, which the public-schema reset between
		// extensions would never clear, and a leftover pgsodium schema would change
		// later suites' catalog-introspection output (pgtap's schemas_are).
		PreCreateExtensions: []ExtensionInstall{{Name: "pgsodium", Schema: "pgsodium"}},
		DependsOn:           []string{"pgtap"},
		// libsodium headers/libs via pkg-config (libsodium-dev on CI; Homebrew
		// keg paths on macOS).
		PkgConfigDeps: []string{"libsodium"},
	},
	"pgmq": {
		Name: "pgmq", Repo: "https://github.com/tembo-io/pgmq", Tag: "v1.11.1",
		// The extension lives under pgmq-extension/ (PGXS Makefile + test/), not at
		// the repo root, so both the build and the fixtures hang off that subdir.
		BuildSubdir: "pgmq-extension", TestSubdir: "pgmq-extension/test",
		// Every test file CREATEs the extension itself (the topic/fifo files open
		// with DROP EXTENSION IF EXISTS pgmq CASCADE; CREATE EXTENSION pgmq), so the
		// harness must not preload it (PreCreateExtensions left empty).
		//
		// base.sql creates partitioned queues via pg_partman's create_parent and
		// CREATEs pg_partman directly; install it first. (base.sql also calls
		// pgmq.create_unlogged, whose CREATE UNLOGGED TABLE runs as dynamic SQL
		// inside a plpgsql function — the gateway only sees the SELECT, so the
		// top-level unlogged-table rejection does not fire and it succeeds.)
		DependsOn: []string{"pg_partman"},
	},
}

// CoveredExternalExtensions returns the external extensions the suite builds and
// tests, derived from ExtensionCatalog (every KindExternal+StatusCovered entry)
// joined with its build spec. An entry marked covered without a matching spec is
// a configuration error and is skipped (CheckExternalSpecs surfaces it as a
// hard failure so it can't silently drop coverage).
func CoveredExternalExtensions() []ExternalExtension {
	var exts []ExternalExtension
	for _, e := range ExtensionCatalog {
		if e.Kind == KindExternal && e.Status == StatusCovered {
			if spec, ok := externalSpecs[e.Name]; ok {
				exts = append(exts, spec)
			}
		}
	}
	return exts
}

// ExternalBuildList returns every external extension the suite must clone, build,
// and install: the extensions selected for this run (ExternalModules, which
// honors PGEXTERNAL_TESTS) plus their build-only dependencies (DependsOn), with
// each dependency ordered before the extension that needs it and every entry
// deduplicated. Dependencies are resolved through externalSpecs. The build phase
// iterates this so a narrowed run (e.g. PGEXTERNAL_TESTS="pgmq") builds only the
// selected extensions and their deps; the test phase iterates ExternalModules
// (dependencies ship no pg_regress suite we run).
func ExternalBuildList() []ExternalExtension {
	var out []ExternalExtension
	seen := map[string]bool{}
	add := func(spec ExternalExtension) {
		if seen[spec.Name] {
			return
		}
		seen[spec.Name] = true
		out = append(out, spec)
	}
	for _, e := range ExternalModules() {
		for _, dep := range e.DependsOn {
			if spec, ok := externalSpecs[dep]; ok {
				add(spec)
			}
		}
		add(e)
	}
	return out
}

// ExternalContribDeps returns the deduplicated contrib modules the selected
// external extensions need installed before their suites run (ExternalExtension.
// ContribDeps), honoring PGEXTERNAL_TESTS via ExternalModules. The build phase
// installs these so external-only runs work; a full run has already installed
// all of contrib, which makes the targeted install a harmless no-op.
func ExternalContribDeps() []string {
	var deps []string
	seen := map[string]bool{}
	for _, e := range ExternalModules() {
		for _, d := range e.ContribDeps {
			if !seen[d] {
				seen[d] = true
				deps = append(deps, d)
			}
		}
	}
	return deps
}

// ExternalPreloadLibraries returns the deduplicated union of the shared
// libraries the selected external extensions need preloaded
// (ExternalExtension.PreloadLibraries), honoring PGEXTERNAL_TESTS via
// ExternalModules, in selection order. shared_preload_libraries is a single
// list-valued GUC, so the harness composes ONE generated snippet from this
// union (see externalServerConfPaths) rather than letting per-extension conf
// files overwrite each other.
func ExternalPreloadLibraries() []string {
	var libs []string
	seen := map[string]bool{}
	for _, e := range ExternalModules() {
		for _, l := range e.PreloadLibraries {
			if !seen[l] {
				seen[l] = true
				libs = append(libs, l)
			}
		}
	}
	return libs
}

// CheckExternalSpecs verifies every covered external extension has a build spec.
// Returns the names missing a spec so the caller can fail loudly rather than
// silently testing nothing.
func CheckExternalSpecs() []string {
	var missing []string
	for _, e := range ExtensionCatalog {
		if e.Kind == KindExternal && e.Status == StatusCovered {
			if _, ok := externalSpecs[e.Name]; !ok {
				missing = append(missing, e.Name)
			}
		}
	}
	return missing
}

// CoveredContribModules returns the contrib module directories the suite runs,
// derived from ExtensionCatalog (every KindContrib+StatusCovered entry). This is
// the single source of truth; DefaultContribModules is built from it. External
// covered extensions are intentionally excluded — they ship outside the
// PostgreSQL source tree and run through the separate external suite
// (CoveredExternalExtensions).
func CoveredContribModules() []string {
	var mods []string
	for _, e := range ExtensionCatalog {
		if e.Kind == KindContrib && e.Status == StatusCovered {
			mods = append(mods, e.Name)
		}
	}
	return mods
}

// statusRank orders statuses in the coverage table: covered first, then the
// actionable backlog, then the out-of-scope buckets.
func statusRank(s ExtStatus) int {
	switch s {
	case StatusCovered:
		return 0
	case StatusPending:
		return 1
	case StatusUnsupported:
		return 2
	case StatusExternal:
		return 3
	default:
		return 4
	}
}

func statusCell(s ExtStatus) string {
	switch s {
	case StatusCovered:
		return "✅ covered"
	case StatusPending:
		return "⏳ pending"
	case StatusUnsupported:
		return "🚫 unsupported"
	case StatusExternal:
		return "📦 external"
	default:
		return string(s)
	}
}

// ExtensionCoverageMarkdown renders the catalog as a coverage table, merged
// with a contrib run's per-test results. Covered extensions expand to one row
// per sub-test (Result filled from the run); the Extension/Kind/Coverage cells
// are populated only on the first row of each extension and left blank on the
// rest so the grouping reads cleanly. Non-covered extensions get a single row
// with the reason in Notes.
//
// suites are this run's per-test result sets whose tests are named "mod/test"
// (the contrib and external suites). Any may be nil (that suite did not run), in
// which case its covered extensions show "—" results.
func ExtensionCoverageMarkdown(suites ...*TestResults) string {
	// Group this run's per-test results by module ("mod/test" → mod) across
	// every suite (contrib + external share the same module-prefixed naming).
	byModule := map[string][]IndividualTestResult{}
	for _, suite := range suites {
		if suite == nil {
			continue
		}
		for _, tr := range suite.Tests {
			mod, test, ok := strings.Cut(tr.Name, "/")
			if !ok {
				continue
			}
			t := tr
			t.Name = test
			byModule[mod] = append(byModule[mod], t)
		}
	}

	entries := make([]ExtensionInfo, len(ExtensionCatalog))
	copy(entries, ExtensionCatalog)
	sort.SliceStable(entries, func(i, j int) bool {
		if ri, rj := statusRank(entries[i].Status), statusRank(entries[j].Status); ri != rj {
			return ri < rj
		}
		return entries[i].Name < entries[j].Name
	})

	// Tallies for the summary line.
	var covered, contribTotal, externalTotal int
	for _, e := range ExtensionCatalog {
		switch e.Kind {
		case KindContrib:
			contribTotal++
		case KindExternal:
			externalTotal++
		}
		if e.Status == StatusCovered {
			covered++
		}
	}

	var sb strings.Builder
	sb.WriteString("### Extension Coverage\n\n")
	fmt.Fprintf(&sb, "Most-installed extensions (top ~%d by usage). %d covered "+
		"(%d contrib, %d external). Covered extensions run their shipped pg_regress "+
		"suite through multigateway; the per-test result below is from this run.\n\n",
		len(ExtensionCatalog), covered, contribTotal, externalTotal)
	sb.WriteString("| Extension | Kind | Coverage | Test | Result | Notes |\n")
	sb.WriteString("|-----------|------|----------|------|--------|-------|\n")

	for _, e := range entries {
		extCell, kindCell, covCell := e.Name, string(e.Kind), statusCell(e.Status)

		if e.Status == StatusCovered {
			tests := byModule[e.Name]
			if len(tests) == 0 {
				// Covered but not exercised in this run (e.g. PGCONTRIB_TESTS
				// selected a subset).
				fmt.Fprintf(&sb, "| %s | %s | %s | — | — (not run) | %s |\n",
					extCell, kindCell, covCell, e.Note)
				continue
			}
			// One row per extension: stack each sub-test and its result on its
			// own line within the cell (GitHub renders <br> inside table cells),
			// so the Test and Result columns line up row-for-row. Per-test
			// "patched" is appended inline to keep it aligned.
			names := make([]string, len(tests))
			results := make([]string, len(tests))
			for i, tr := range tests {
				names[i] = tr.Name
				r := resultCell(tr.Status)
				if tr.PatchApplied {
					r += " (patched)"
				}
				results[i] = r
			}
			fmt.Fprintf(&sb, "| %s | %s | %s | %s | %s | %s |\n",
				extCell, kindCell, covCell,
				strings.Join(names, "<br>"), strings.Join(results, "<br>"), e.Note)
			continue
		}

		// Non-covered: one row, reason in Notes.
		fmt.Fprintf(&sb, "| %s | %s | %s | — | — | %s |\n",
			extCell, kindCell, covCell, e.Note)
	}
	sb.WriteString("\n")
	return sb.String()
}

func resultCell(status string) string {
	switch status {
	case "pass":
		return "✅ pass"
	case "fail":
		return "❌ fail"
	case "skip":
		return "⏭️ skip"
	default:
		return status
	}
}
