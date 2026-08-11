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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// postgrestReport is the classified outcome of one TestPostgREST run, ready to
// render as a markdown summary. Gateway is always set once the gateway arm has
// run; Baseline is nil when the gateway was clean (the classifier is skipped).
// Divergences and EnvFailures are populated only once both arms have run.
type postgrestReport struct {
	Gateway     *specResult // gateway arm; nil if the gateway arm never produced a result
	Baseline    *specResult // direct-PostgreSQL classifier arm; nil if it was skipped
	Divergences []string    // fail through gateway, pass on direct PostgreSQL (gateway bugs)
	EnvFailures []string    // fail on both paths (harness/env, not gateway bugs)
}

// writeReport renders the run summary as markdown and writes it to the results
// dir, mirroring it to GITHUB_STEP_SUMMARY when running in CI so the divergence
// count is visible on the job page without downloading artifacts. It is a no-op
// when the gateway arm never produced a result (e.g. an early t.Fatalf during
// setup), since there is nothing to summarize.
func writeReport(t *testing.T, r *postgrestReport) {
	t.Helper()
	if r.Gateway == nil {
		return
	}

	summary := renderReport(r)
	path, err := suiteutil.WriteMarkdown(resultsDir(), "compatibility-report.md", summary)
	if err != nil {
		t.Logf("write PostgREST summary: %v", err)
		return
	}
	t.Logf("Markdown summary written to: %s", path)
}

// renderReport builds the markdown summary: a shields.io badge for the gateway
// pass rate, a counts table, and the divergence / environment-failure lists.
func renderReport(r *postgrestReport) string {
	gw := r.Gateway

	var sb strings.Builder
	sb.WriteString("## PostgREST Spec Suite (through the multigateway)\n\n")

	// The headline signal is gateway divergences (fail proxied, pass direct) —
	// the only failures attributable to multigres. The badge tracks them: green
	// only at zero. When the classifier did not run (gateway clean), there are
	// no divergences by construction.
	div := len(r.Divergences)
	sb.WriteString(suiteutil.BadgeMarkdown("PostgREST_gateway_divergences", 0, div, 0, false))
	sb.WriteString("\n\n")

	sb.WriteString("| Metric | Count |\n|---|---|\n")
	fmt.Fprintf(&sb, "| Examples run (gateway) | %d |\n", gw.Total)
	fmt.Fprintf(&sb, "| Passed | %d |\n", gw.Passed())
	fmt.Fprintf(&sb, "| Pending | %d |\n", gw.Pending)
	fmt.Fprintf(&sb, "| Failed (gateway) | %d |\n", gw.Failures)
	fmt.Fprintf(&sb, "| — Gateway divergences (fail proxied, pass direct) | %d |\n", div)
	fmt.Fprintf(&sb, "| — Environment failures (fail on direct PostgreSQL too) | %d |\n", len(r.EnvFailures))
	sb.WriteString("\n")

	if r.Baseline == nil && gw.Failures == 0 {
		sb.WriteString("Gateway clean — all examples passed; the direct-PostgreSQL classifier was skipped.\n\n")
	}

	if div > 0 {
		fmt.Fprintf(&sb, "### %d gateway divergence(s) — see DIVERGENCES.md\n\n", div)
		fmt.Fprintf(&sb, "Fail through the gateway but pass on direct PostgreSQL — real behavioural gaps on the proxied path.\n\n")
		for _, f := range r.Divergences {
			fmt.Fprintf(&sb, "- `%s`\n", f)
		}
		sb.WriteString("\n")
	}

	if len(r.EnvFailures) > 0 {
		fmt.Fprintf(&sb, "### %d environment failure(s) — not gateway bugs\n\n", len(r.EnvFailures))
		fmt.Fprintf(&sb, "Fail on direct PostgreSQL too (harness / PG version / config), so they do not fail the test.\n\n")
		for _, f := range r.EnvFailures {
			fmt.Fprintf(&sb, "- `%s`\n", f)
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// resultsDir is where the markdown summary is written. Defaults to a results
// subdir under the suite cache root; override with POSTGREST_RESULTS_DIR (CI
// points its artifact upload at this dir).
func resultsDir() string {
	if d := os.Getenv("POSTGREST_RESULTS_DIR"); d != "" {
		return d
	}
	return filepath.Join(cacheRoot(), "results")
}
