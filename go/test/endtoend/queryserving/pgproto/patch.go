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

package pgproto

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// Patch-based verification records a known, accepted divergence between
// PostgreSQL and the multigateway as a per-file patch under
// testdata/patches/<name>.patch. The patch is applied to PostgreSQL's
// (normalized) trace to produce the *expected multigateway trace*; the run then
// passes iff the multigateway's trace matches that patched baseline. A
// divergence not covered by a patch leaves a residual diff and is flagged.
//
// The apply/diff/regenerate mechanics are shared with the pgregresstest suite
// and live in suiteutil; this file holds the pgproto-specific parts: locating a
// patch file from a corpus name, normalizing the trailing newline before
// comparison, and mapping the shared outcome into a patchOutcome.

// PatchModeEnv selects the mode. Unset/empty means verify.
const PatchModeEnv = "PGPROTO_PATCH_MODE"

// getPatchMode reads the mode from the environment, defaulting to verify.
func getPatchMode() suiteutil.PatchMode {
	return suiteutil.PatchModeFromEnv(PatchModeEnv)
}

// patchOutcome is the result of patch-aware comparison of one file's traces.
type patchOutcome struct {
	// Matched is true when the multigateway trace equals the (possibly patched)
	// PostgreSQL baseline.
	Matched bool
	// PatchApplied is true when a patch file existed and was applied to reach the
	// match — i.e. this file passes via a recorded known divergence.
	PatchApplied bool
	// PatchPath is the patch file path (relative to the package) for reports.
	PatchPath string
	// ResidualDiff is the unified diff (patched-postgres vs multigateway) when
	// Matched is false, or the patch-apply error when a patch failed to apply.
	ResidualDiff string
}

// patchFilePath maps a corpus file name (relative to the corpus root, e.g.
// "error_recovery.pgproto") to its patch file under patchDir, dropping the
// .pgproto suffix: "<patchDir>/error_recovery.patch".
func patchFilePath(patchDir, name string) string {
	base := strings.TrimSuffix(name, ".pgproto")
	return filepath.Join(patchDir, base+".patch")
}

// verifyTracePatch compares the multigateway trace against PostgreSQL's trace,
// applying a per-file patch if one exists. It normalizes the trailing newline of
// each trace, delegates the apply/diff/regenerate mechanics to
// suiteutil.VerifyPatch, and maps the shared outcome into a patchOutcome.
//
//   - verify mode: a missing/failed patch or a residual diff -> not Matched, with
//     the failure detail recorded in ResidualDiff.
//   - generate mode: if pgTrace already equals mgTrace, remove any stale patch;
//     otherwise (re)write the patch from pgTrace -> mgTrace (preserving any
//     comment preamble) and report Matched.
func verifyTracePatch(ctx context.Context, pgTrace, mgTrace, patchDir, name string, mode suiteutil.PatchMode) (patchOutcome, error) {
	var out patchOutcome

	patchPath := patchFilePath(patchDir, name)
	res, err := suiteutil.VerifyPatch(ctx, suiteutil.PatchInput{
		Expected:  []byte(ensureTrailingNewline(pgTrace)),
		Actual:    []byte(ensureTrailingNewline(mgTrace)),
		PatchPath: patchPath,
	}, mode)
	if err != nil {
		return out, err
	}

	out.Matched = res.Matched
	out.PatchApplied = res.PatchApplied
	if res.PatchApplied {
		out.PatchPath = relPatchPath(patchPath)
	}
	// Surface a stale-patch apply failure in the same slot the report renders
	// for an unpatched divergence, so either failure mode shows its detail.
	switch {
	case res.ApplyErr != "":
		out.ResidualDiff = res.ApplyErr
	case !res.Matched:
		out.ResidualDiff = res.ResidualDiff
	}
	return out, nil
}

// ensureTrailingNewline appends a newline if missing, so diff/patch operate on
// whole lines and don't emit "\ No newline at end of file" noise.
func ensureTrailingNewline(s string) string {
	if s == "" || strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}

// relPatchPath returns patchPath relative to the current working directory (the
// package dir during `go test`) so report output is portable, falling back to
// the input when a relative path can't be computed.
func relPatchPath(patchPath string) string {
	if wd, err := os.Getwd(); err == nil {
		if rel, relErr := filepath.Rel(wd, patchPath); relErr == nil && !strings.HasPrefix(rel, "..") {
			return rel
		}
	}
	return patchPath
}
