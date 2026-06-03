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

import "time"

// Differential report types shared by the query-serving suites that diff a
// candidate (the multigateway) against a PostgreSQL baseline file-by-file
// (pgproto, sqllogictest). PostgreSQL is the oracle; each file is classified as
// passed-on-both, proxy-divergence (PG only), unexpected MG-only, or
// failed-on-both, and the per-suite counters are derived identically.
//
// What differs per suite is how each file's pass verdict is computed (intrinsic
// exit code vs. differential trace match), the human note, and how the report
// is rendered (markdown layout, log labels). So the caller computes the
// per-file DiffFile values and renders its own markdown/log summary; this core
// only owns the JSON shapes and the counting/assembly that is byte-identical
// across suites.

// PerRun is one target's outcome for a single file in the serialized report.
type PerRun struct {
	Passed   bool   `json:"passed"`
	TimedOut bool   `json:"timed_out,omitempty"`
	Duration string `json:"duration"`
	Output   string `json:"output,omitempty"`
	ExecErr  string `json:"exec_err,omitempty"`
}

// FileReport is the per-file record in the serialized report. The field layout
// matches pgregresstest's so .github/scripts/detect-regressions.sh can consume
// any suite's output. The patch fields are omitempty so suites that don't use
// patch-based verification (e.g. sqllogictest) keep their existing JSON shape.
type FileReport struct {
	Name         string `json:"name"`           // relative path within the corpus
	Status       string `json:"status"`         // "pass" | "fail"
	Duration     string `json:"duration"`       // combined duration across targets
	Postgres     PerRun `json:"postgres"`       // baseline (direct PG) — the oracle
	Gateway      PerRun `json:"multigateway"`   // candidate (multigres)
	Note         string `json:"note,omitempty"` // human note (divergence, exec err, timeout)
	PatchApplied bool   `json:"patch_applied,omitempty"`
	PatchPath    string `json:"patch_path,omitempty"`
}

// SuiteReport is the serialized per-suite result. A "suite" is one comparison
// dimension (e.g. pgproto emits one; sqllogictest emits one per wire protocol).
type SuiteReport struct {
	Name           string       `json:"name"`
	CorpusDir      string       `json:"corpus_dir"`
	CorpusCommit   string       `json:"corpus_commit"`
	TimedOut       bool         `json:"timed_out"`
	TotalFiles     int          `json:"total_files"`
	PassedBoth     int          `json:"passed_both"`
	PassedViaPatch int          `json:"passed_via_patch,omitempty"` // subset of passed_both absorbed by a patch
	PassedPGOnly   int          `json:"passed_pg_only"`             // proxy divergence
	PassedMGOnly   int          `json:"passed_mg_only"`
	FailedBoth     int          `json:"failed_both"`
	PGPassed       int          `json:"postgres_passed"`
	GatewayPassed  int          `json:"multigateway_passed"`
	StartedAt      time.Time    `json:"started_at"`
	Duration       string       `json:"duration"`
	Tests          []FileReport `json:"tests"`
}

// SuiteMeta carries the suite-level identifiers for NewSuiteReport.
type SuiteMeta struct {
	Name         string
	CorpusDir    string
	CorpusCommit string
	StartedAt    time.Time
	TimedOut     bool
}

// DiffFile is one file's already-computed comparison, supplied by the caller.
// PGPassed/MGPassed are the suite's pass verdicts (intrinsic or differential);
// Postgres/Gateway are the serialized per-target records; Combined is the total
// wall time across both targets. PatchApplied/PatchPath are optional and only
// meaningful for patch-based suites.
type DiffFile struct {
	Name         string
	PGPassed     bool
	MGPassed     bool
	Postgres     PerRun
	Gateway      PerRun
	Note         string
	Combined     time.Duration
	PatchApplied bool
	PatchPath    string
}

// NewSuiteReport assembles a SuiteReport from per-file comparisons, deriving the
// pass/divergence counters. A file passes when both targets pass; a pass that
// rode in on a patch also bumps PassedViaPatch. PG-only passes are proxy
// divergences; MG-only passes are unexpected (usually a corpus/baseline issue).
func NewSuiteReport(meta SuiteMeta, files []DiffFile) *SuiteReport {
	r := &SuiteReport{
		Name:         meta.Name,
		CorpusDir:    meta.CorpusDir,
		CorpusCommit: meta.CorpusCommit,
		TimedOut:     meta.TimedOut,
		TotalFiles:   len(files),
		StartedAt:    meta.StartedAt,
		Duration:     time.Since(meta.StartedAt).Round(time.Second).String(),
	}

	for _, f := range files {
		if f.PGPassed {
			r.PGPassed++
		}
		if f.MGPassed {
			r.GatewayPassed++
		}

		status := "fail"
		switch {
		case f.PGPassed && f.MGPassed:
			r.PassedBoth++
			status = "pass"
			if f.PatchApplied {
				r.PassedViaPatch++
			}
		case f.PGPassed && !f.MGPassed:
			r.PassedPGOnly++
		case !f.PGPassed && f.MGPassed:
			r.PassedMGOnly++
		default:
			r.FailedBoth++
		}

		r.Tests = append(r.Tests, FileReport{
			Name:         f.Name,
			Status:       status,
			Duration:     f.Combined.Round(time.Millisecond).String(),
			Postgres:     f.Postgres,
			Gateway:      f.Gateway,
			Note:         f.Note,
			PatchApplied: f.PatchApplied,
			PatchPath:    f.PatchPath,
		})
	}

	return r
}
