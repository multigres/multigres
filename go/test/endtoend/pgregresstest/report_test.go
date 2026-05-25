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
	"strings"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
)

// newReportBuilder constructs a PostgresBuilder pointed at a per-test temp
// directory. The Builder is never asked to do real work in these tests — only
// pb.OutputDir is read, so the cheaper New() path is sufficient.
func newReportBuilder(t *testing.T) *PostgresBuilder {
	t.Helper()
	return &PostgresBuilder{Builder: pgbuilder.New(t)}
}

// TestMarkdownSummary_SingleTarget verifies the wide-table renderer still
// produces the expected layout when only one target ran (the default
// multigateway-only path). Patch and Duration columns must be absent, the
// pgbouncer version must be "n/a", and the per-target column must be present.
func TestMarkdownSummary_SingleTarget(t *testing.T) {
	pb := newReportBuilder(t)
	suites := []SuiteResult{{
		Name:          "Regression Tests",
		ExpectedTests: 3,
		PerTarget: map[Target]*TestResults{
			TargetMultigateway: {
				TotalTests:  3,
				PassedTests: 2,
				FailedTests: 1,
				Tests: []IndividualTestResult{
					{Name: "boolean", Status: "pass"},
					{Name: "char", Status: "pass"},
					{Name: "numeric", Status: "fail"},
				},
			},
		},
	}}

	out, err := pb.WriteMarkdownSummary(t, suites)
	if err != nil {
		t.Fatalf("WriteMarkdownSummary: %v", err)
	}

	mustContain(t, out, "**PostgreSQL Version:** `"+PostgresVersion+"`")
	mustContain(t, out, "**Pgbouncer Version:** `n/a`")
	mustContain(t, out, "| # | Test | multigateway |")
	mustContain(t, out, "| 1 | boolean | ✅ |")
	mustContain(t, out, "| 3 | numeric | ❌ |")
	mustNotContain(t, out, "Duration")
	mustNotContain(t, out, "Patch")
}

// TestMarkdownSummary_AllTargets verifies the wide-table renderer when all
// three targets ran. Every target gets a badge per suite, the table header
// lists all three columns, and tests missing from one target render "-".
func TestMarkdownSummary_AllTargets(t *testing.T) {
	pb := newReportBuilder(t)
	suites := []SuiteResult{{
		Name:          "Regression Tests",
		ExpectedTests: 3,
		PerTarget: map[Target]*TestResults{
			TargetMultigateway: {
				TotalTests: 3, PassedTests: 3,
				Tests: []IndividualTestResult{
					{Name: "boolean", Status: "pass"},
					{Name: "char", Status: "pass"},
					{Name: "numeric", Status: "pass"},
				},
			},
			TargetPgbouncerSession: {
				TotalTests: 3, PassedTests: 2, FailedTests: 1,
				Tests: []IndividualTestResult{
					{Name: "boolean", Status: "pass"},
					{Name: "char", Status: "pass"},
					{Name: "numeric", Status: "fail"},
				},
			},
			TargetPgbouncerTx: {
				TotalTests: 2, PassedTests: 1, FailedTests: 1,
				// numeric absent on tx target to exercise the "-" path.
				Tests: []IndividualTestResult{
					{Name: "boolean", Status: "pass"},
					{Name: "char", Status: "fail"},
				},
			},
		},
	}}

	out, err := pb.WriteMarkdownSummary(t, suites)
	if err != nil {
		t.Fatalf("WriteMarkdownSummary: %v", err)
	}

	mustContain(t, out, "**Pgbouncer Version:** `"+PinnedPgbouncerVersion+"`")
	mustContain(t, out, "Regression — multigateway")
	mustContain(t, out, "Regression — pgbouncer-session")
	mustContain(t, out, "Regression — pgbouncer-tx")
	mustContain(t, out, "| # | Test | multigateway | pgbouncer-session | pgbouncer-tx |")
	mustContain(t, out, "| 1 | boolean | ✅ | ✅ | ✅ |")
	mustContain(t, out, "| 2 | char | ✅ | ✅ | ❌ |")
	mustContain(t, out, "| 3 | numeric | ✅ | ❌ | - |")
}

// TestMarkdownSummary_PerTargetTimeout asserts each target gets its own
// timeout callout so a comparison run does not lose which target died.
func TestMarkdownSummary_PerTargetTimeout(t *testing.T) {
	pb := newReportBuilder(t)
	suites := []SuiteResult{{
		Name:          "Isolation Tests",
		ExpectedTests: 5,
		PerTarget: map[Target]*TestResults{
			TargetMultigateway: {
				TotalTests:  5,
				PassedTests: 5,
				Tests: []IndividualTestResult{
					{Name: "a", Status: "pass"},
				},
			},
			TargetPgbouncerTx: {
				TotalTests: 2,
				TimedOut:   true,
				Tests: []IndividualTestResult{
					{Name: "a", Status: "pass"},
				},
			},
		},
	}}

	out, err := pb.WriteMarkdownSummary(t, suites)
	if err != nil {
		t.Fatalf("WriteMarkdownSummary: %v", err)
	}
	mustContain(t, out, "**pgbouncer-tx timed out**")
	mustNotContain(t, out, "**multigateway timed out**")
}

func mustContain(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("expected substring %q in output:\n%s", sub, s)
	}
}

func mustNotContain(t *testing.T, s, sub string) {
	t.Helper()
	if strings.Contains(s, sub) {
		t.Fatalf("did not expect substring %q in output:\n%s", sub, s)
	}
}
