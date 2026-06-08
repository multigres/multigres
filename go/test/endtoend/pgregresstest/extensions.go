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
	{"http", KindExternal, StatusExternal, ""},
	{"hypopg", KindExternal, StatusExternal, ""},
	{"index_advisor", KindExternal, StatusExternal, "depends on hypopg"},
	{"ltree", KindContrib, StatusCovered, ""},
	{"moddatetime", KindContrib, StatusUnsupported, "contrib/spi ships no pg_regress suite"},
	{"pg_cron", KindExternal, StatusCovered, "Citus pg_cron; built as a PGXS module from externalSpecs; needs shared_preload_libraries (see testdata/pg17/external/pg_cron.conf)"},
	{"pg_graphql", KindExternal, StatusCovered, "Supabase pg_graphql; Rust/pgrx crate built with cargo-pgrx; loads test/fixtures.sql before its pg_regress suite"},
	{"pg_jsonschema", KindExternal, StatusExternal, "Rust"},
	{"pg_net", KindExternal, StatusExternal, "background worker"},
	{"pg_partman", KindExternal, StatusUnsupported, "ships a pgTAP suite, not pg_regress; built and installed as a build dependency of pgmq (create_partitioned → create_parent)"},
	{"pg_stat_statements", KindContrib, StatusUnsupported, "NO_INSTALLCHECK; records query text the gateway rewrites"},
	{"pg_trgm", KindContrib, StatusCovered, ""},
	{"pgaudit", KindExternal, StatusExternal, ""},
	{"pgcrypto", KindContrib, StatusCovered, "needs --with-ssl=openssl"},
	{"pgjwt", KindExternal, StatusExternal, "depends on pgcrypto"},
	{"pgmq", KindExternal, StatusCovered, "tembo-io/pgmq; pure-SQL queue built as a PGXS module from pgmq-extension/; partitioned-queue tests depend on pg_partman"},
	{"pgsodium", KindExternal, StatusExternal, "libsodium"},
	{"pgtap", KindExternal, StatusExternal, ""},
	{"plpgsql", KindContrib, StatusUnsupported, "built-in PL; exercised by the core regression suite, not contrib"},
	{"plpgsql_check", KindExternal, StatusExternal, ""},
	{"postgis", KindExternal, StatusExternal, ""},
	{"postgis_topology", KindExternal, StatusExternal, "PostGIS"},
	{"postgres_fdw", KindContrib, StatusUnsupported, "pooler blocks CREATE SERVER / outbound connections"},
	{"supabase_vault", KindExternal, StatusExternal, ""},
	{"unaccent", KindContrib, StatusCovered, ""},
	{"uuid-ossp", KindContrib, StatusCovered, "needs --with-uuid"},
	{"vector", KindExternal, StatusCovered, "pgvector; built as a PGXS module from externalSpecs"},
	{"wrappers", KindExternal, StatusExternal, "Rust"},
}

// ExternalExtension describes one external (non-contrib) extension wired into
// the external suite: its catalog name, the git coordinates the harness clones
// and builds it from, and a few knobs for the places extensions diverge from
// the pgvector baseline (test layout, extension lifecycle, server config).
type ExternalExtension struct {
	Name string
	Repo string
	Tag  string

	// BuildSubdir is the directory within the checkout that holds the PGXS
	// Makefile, relative to the clone root. pgvector and pg_cron keep it at the
	// repo root (""), so the harness builds there; pgmq keeps the extension under
	// pgmq-extension/, so it uses "pgmq-extension". Empty means the repo root.
	BuildSubdir string

	// TestSubdir is the directory within the checkout that holds the shipped
	// pg_regress fixtures (sql/ + expected/), relative to the clone root.
	// pgvector keeps them under test/; pg_cron keeps them at the repo root, so
	// it uses "." (filepath.Join collapses it back to the clone root). pgmq keeps
	// them under pgmq-extension/test, alongside its BuildSubdir.
	TestSubdir string

	// CreateExtension controls whether the harness pre-creates the extension
	// through multigateway (and passes pg_regress --load-extension) before the
	// suite runs. pgvector's fixtures assume it already exists (they open with a
	// bare CREATE TABLE ... vector(3) and never CREATE EXTENSION), so it needs
	// the preload. pg_cron's fixtures manage the extension themselves
	// (CREATE EXTENSION pg_cron VERSION '1.0' is the first statement, then they
	// DROP and recreate it at a newer version), so preloading would make that
	// first statement fail with "extension already exists" — it must be false.
	CreateExtension bool

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
	// path can't set, e.g. pg_cron's background worker requires
	// shared_preload_libraries = 'pg_cron' or CREATE EXTENSION errors out. Empty
	// for extensions that need nothing beyond the stock cluster (pgvector).
	ServerConfigFile string

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
		TestSubdir: "test", CreateExtension: true,
	},
	"pg_graphql": {
		Name: "pg_graphql", Repo: "https://github.com/supabase/pg_graphql", Tag: "v1.6.1",
		// Rust crate built with cargo-pgrx; the pgrx version must match the crate's
		// pinned dependency (Cargo.toml: pgrx = "=0.16.1"). Build entry point and
		// fixtures are at the repo root / test/.
		BuildSystem: "pgrx", PgrxVersion: "0.16.1", TestSubdir: "test",
		// test/fixtures.sql opens with `drop extension if exists pg_graphql;
		// create extension pg_graphql cascade;` and sets the graphql schema
		// comment, so the harness loads it first and must not also preload the
		// extension itself.
		CreateExtension: false, FixturesFile: "fixtures.sql",
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
		TestSubdir: ".", CreateExtension: false, ServerConfigFile: "pg_cron.conf",
		// pg_cron-test.sql references pgcron_dbno/pgcron_dbyes by name (it REVOKEs
		// CONNECT on one and schedules/alters jobs targeting both) but never
		// connects to them; front-load them on the primary so those metadata and
		// CONNECT-privilege checks run for real. See ScratchDatabases.
		ScratchDatabases: []string{"pgcron_dbno", "pgcron_dbyes"},
	},
	// pg_partman is a build-only dependency of pgmq, never tested on its own (it
	// ships a pgTAP suite, not pg_regress — see its StatusUnsupported catalog
	// entry). pgmq.create_partitioned calls partman's create_parent, which works
	// without the background worker, so no ServerConfigFile is needed; the PGXS
	// Makefile is at the repo root, so BuildSubdir stays empty.
	"pg_partman": {
		Name: "pg_partman", Repo: "https://github.com/pgpartman/pg_partman", Tag: "v5.4.3",
	},
	"pgmq": {
		Name: "pgmq", Repo: "https://github.com/tembo-io/pgmq", Tag: "v1.11.1",
		// The extension lives under pgmq-extension/ (PGXS Makefile + test/), not at
		// the repo root, so both the build and the fixtures hang off that subdir.
		BuildSubdir: "pgmq-extension", TestSubdir: "pgmq-extension/test",
		// Every test file CREATEs the extension itself (the topic/fifo files open
		// with DROP EXTENSION IF EXISTS pgmq CASCADE; CREATE EXTENSION pgmq), so the
		// harness must not preload it.
		CreateExtension: false,
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
