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
	{"pg_cron", KindExternal, StatusExternal, ""},
	{"pg_graphql", KindExternal, StatusExternal, "Rust"},
	{"pg_jsonschema", KindExternal, StatusExternal, "Rust"},
	{"pg_net", KindExternal, StatusExternal, "background worker"},
	{"pg_stat_statements", KindContrib, StatusUnsupported, "NO_INSTALLCHECK; records query text the gateway rewrites"},
	{"pg_trgm", KindContrib, StatusCovered, ""},
	{"pgaudit", KindExternal, StatusExternal, ""},
	{"pgcrypto", KindContrib, StatusCovered, "needs --with-ssl=openssl"},
	{"pgjwt", KindExternal, StatusExternal, "depends on pgcrypto"},
	{"pgmq", KindExternal, StatusExternal, ""},
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
// the external suite: its catalog name plus the git coordinates the harness
// clones and builds it from.
type ExternalExtension struct {
	Name string
	Repo string
	Tag  string
}

// externalSpecs holds the build coordinates (git repo + pinned tag) for every
// external extension the harness can build. An ExtensionCatalog entry with
// Kind==KindExternal can only be StatusCovered if it also has a spec here; the
// pinned tag keeps the suite reproducible (and matches the pgvector ABI the
// from-source PostgreSQL was built against). Keyed by catalog Name.
var externalSpecs = map[string]ExternalExtension{
	"vector": {Name: "vector", Repo: "https://github.com/pgvector/pgvector", Tag: "v0.8.1"},
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
