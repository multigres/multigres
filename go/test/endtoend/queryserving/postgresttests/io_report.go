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
	"strings"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// writeIOReport renders the io-suite run summary as markdown and writes it to
// the results dir (mirrored to GITHUB_STEP_SUMMARY in CI). No-op if the gateway
// arm never produced a result (e.g. an early t.Fatalf during setup). Mirrors
// report.go's writeReport but for the pytest io suite.
func writeIOReport(t *testing.T, r *postgrestReport) {
	t.Helper()
	if r.Gateway == nil {
		return
	}

	path, err := suiteutil.WriteMarkdown(resultsDir(), "io-compatibility-report.md", renderIOReport(r))
	if err != nil {
		t.Logf("write PostgREST io summary: %v", err)
		return
	}
	t.Logf("Markdown io summary written to: %s", path)
}

// renderIOReport builds the markdown summary: a shields.io badge for the gateway
// divergence count, a counts table, and the divergence / environment-failure
// lists. Same shape as renderReport, retitled for the io suite.
func renderIOReport(r *postgrestReport) string {
	gw := r.Gateway

	var sb strings.Builder
	sb.WriteString("## PostgREST IO Suite (through the multigateway)\n\n")
	sb.WriteString("Proxy-relevant subset of PostgREST's `test/io` pytest suite — role settings, ")
	sb.WriteString("statement_timeout, hoisted tx settings, prepared statements. See `io_tests.md`.\n\n")

	div := len(r.Divergences)
	sb.WriteString(suiteutil.BadgeMarkdown("PostgREST_io_gateway_divergences", 0, div, 0, false))
	sb.WriteString("\n\n")

	sb.WriteString("| Metric | Count |\n|---|---|\n")
	fmt.Fprintf(&sb, "| Examples run (gateway) | %d |\n", gw.Total)
	fmt.Fprintf(&sb, "| Passed | %d |\n", gw.Passed())
	fmt.Fprintf(&sb, "| Pending/skipped | %d |\n", gw.Pending)
	if r.Baseline == nil {
		fmt.Fprintf(&sb, "| Failed — gateway divergences (baseline asserted green) | %d |\n", div)
	} else {
		fmt.Fprintf(&sb, "| Failed (gateway) | %d |\n", gw.Failures)
		fmt.Fprintf(&sb, "| — Gateway divergences (fail proxied, pass direct) | %d |\n", div)
		fmt.Fprintf(&sb, "| — Environment failures (fail on direct PostgreSQL too) | %d |\n", len(r.EnvFailures))
		fmt.Fprintf(&sb, "| Direct-baseline failures (invariant broken if > 0) | %d |\n", r.Baseline.Failures)
	}
	sb.WriteString("\n")

	if div == 0 && gw.Failures == 0 {
		sb.WriteString("Gateway clean — all selected io tests passed.\n\n")
	}

	if div > 0 {
		fmt.Fprintf(&sb, "### %d gateway divergence(s)\n\n", div)
		sb.WriteString("Fail through the gateway — behavioural gaps on the proxied path (the direct-PostgreSQL baseline is asserted green).\n\n")
		for _, f := range r.Divergences {
			fmt.Fprintf(&sb, "- `%s`\n", f)
		}
		sb.WriteString("\n")
	}

	if len(r.EnvFailures) > 0 {
		fmt.Fprintf(&sb, "### %d environment failure(s) — not gateway bugs\n\n", len(r.EnvFailures))
		sb.WriteString("Fail on direct PostgreSQL too (harness / PG version / config), so they do not fail the test.\n\n")
		for _, f := range r.EnvFailures {
			fmt.Fprintf(&sb, "- `%s`\n", f)
		}
		sb.WriteString("\n")
	}

	return sb.String()
}
