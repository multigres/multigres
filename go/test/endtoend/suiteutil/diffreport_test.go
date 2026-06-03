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

package suiteutil

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNewSuiteReportCounters(t *testing.T) {
	files := []DiffFile{
		{Name: "both", PGPassed: true, MGPassed: true},
		{Name: "via-patch", PGPassed: true, MGPassed: true, PatchApplied: true},
		{Name: "pg-only", PGPassed: true, MGPassed: false}, // proxy divergence
		{Name: "mg-only", PGPassed: false, MGPassed: true},
		{Name: "neither", PGPassed: false, MGPassed: false},
	}
	r := NewSuiteReport(SuiteMeta{Name: "X", StartedAt: time.Now()}, files)

	checks := map[string]int{
		"TotalFiles":     5,
		"PassedBoth":     2,
		"PassedViaPatch": 1,
		"PassedPGOnly":   1,
		"PassedMGOnly":   1,
		"FailedBoth":     1,
		"PGPassed":       3,
		"GatewayPassed":  3,
	}
	got := map[string]int{
		"TotalFiles":     r.TotalFiles,
		"PassedBoth":     r.PassedBoth,
		"PassedViaPatch": r.PassedViaPatch,
		"PassedPGOnly":   r.PassedPGOnly,
		"PassedMGOnly":   r.PassedMGOnly,
		"FailedBoth":     r.FailedBoth,
		"PGPassed":       r.PGPassed,
		"GatewayPassed":  r.GatewayPassed,
	}
	for k, want := range checks {
		if got[k] != want {
			t.Errorf("%s = %d, want %d", k, got[k], want)
		}
	}

	// Status: both-pass files are "pass", everything else "fail".
	wantStatus := map[string]string{
		"both": "pass", "via-patch": "pass", "pg-only": "fail", "mg-only": "fail", "neither": "fail",
	}
	for _, tr := range r.Tests {
		if tr.Status != wantStatus[tr.Name] {
			t.Errorf("%s status = %q, want %q", tr.Name, tr.Status, wantStatus[tr.Name])
		}
	}
}

// TestSuiteReportJSONShape guards the on-disk field names downstream tooling
// (detect-regressions.sh) reads, and that the patch fields stay omitted for a
// non-patch suite so sqllogictest's JSON is unchanged.
func TestSuiteReportJSONShape(t *testing.T) {
	r := NewSuiteReport(SuiteMeta{Name: "X", StartedAt: time.Now()}, []DiffFile{
		{Name: "f", PGPassed: true, MGPassed: false, Note: "n"},
	})
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, key := range []string{
		`"passed_both"`, `"passed_pg_only"`, `"passed_mg_only"`,
		`"failed_both"`, `"postgres_passed"`, `"multigateway_passed"`, `"tests"`,
	} {
		if !strings.Contains(s, key) {
			t.Errorf("expected key %s in JSON: %s", key, s)
		}
	}
	// No patch was applied anywhere → patch fields omitted entirely.
	if strings.Contains(s, "passed_via_patch") || strings.Contains(s, "patch_applied") || strings.Contains(s, "patch_path") {
		t.Errorf("patch fields should be omitted for a non-patch suite: %s", s)
	}
}
