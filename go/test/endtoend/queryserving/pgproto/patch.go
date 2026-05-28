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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/multigres/multigres/go/tools/executil"
)

// Patch-based verification mirrors the pgregresstest suite: a known, accepted
// divergence between PostgreSQL and the multigateway is recorded as a per-file
// patch under testdata/patches/<name>.patch. The patch is applied to
// PostgreSQL's (normalized) trace to produce the *expected multigateway trace*;
// the run then passes iff the multigateway's trace matches that patched
// baseline. A divergence not covered by a patch leaves a residual diff and is
// flagged. This turns "we know this differs and it's acceptable" into a
// checked-in expectation, while still catching new/changed divergences.

// PatchMode selects the behavior of verifyTracePatch.
type PatchMode string

const (
	// PatchModeVerify is strict: the multigateway trace must match the patched
	// PostgreSQL baseline exactly, or the file is reported as a divergence.
	PatchModeVerify PatchMode = "verify"
	// PatchModeGenerate absorbs any residual divergence by (re)writing the
	// per-file patch from (postgres trace -> multigateway trace), so the file
	// ends up matching. Stale patches (files that now match exactly) are removed.
	PatchModeGenerate PatchMode = "generate"
)

// PatchModeEnv selects the mode. Unset/empty means verify.
const PatchModeEnv = "PGPROTO_PATCH_MODE"

// getPatchMode reads the mode from the environment, defaulting to verify.
func getPatchMode() PatchMode {
	switch PatchMode(os.Getenv(PatchModeEnv)) {
	case PatchModeGenerate:
		return PatchModeGenerate
	default:
		return PatchModeVerify
	}
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
	// Matched is false.
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
// applying a per-file patch if one exists.
//
//   - verify mode: apply patches/<name>.patch to pgTrace (if present), diff vs
//     mgTrace. Empty diff -> Matched. A missing/failed patch or a residual diff
//     -> not Matched, with the residual diff recorded.
//   - generate mode: if pgTrace already equals mgTrace, remove any stale patch;
//     otherwise (re)write the patch from pgTrace -> mgTrace and report Matched.
func verifyTracePatch(ctx context.Context, pgTrace, mgTrace, patchDir, name string, mode PatchMode) (patchOutcome, error) {
	var out patchOutcome

	expected := []byte(ensureTrailingNewline(pgTrace))
	actual := []byte(ensureTrailingNewline(mgTrace))

	patchPath := patchFilePath(patchDir, name)
	patchExists := fileExists(patchPath)

	// Baseline = postgres trace, optionally transformed by the patch.
	baseline := expected
	if patchExists {
		patched, err := applyPatch(ctx, expected, patchPath)
		switch {
		case err == nil:
			baseline = patched
			out.PatchApplied = true
			out.PatchPath = patchPath
		case mode == PatchModeVerify:
			out.ResidualDiff = fmt.Sprintf("patch %s failed to apply (likely stale): %v", patchPath, err)
			return out, nil
		default:
			baseline = expected // generate mode: regenerate below
		}
	}

	diff, err := generateDiff(ctx, baseline, actual)
	if err != nil {
		return out, fmt.Errorf("diff %s: %w", name, err)
	}

	if len(diff) == 0 {
		out.Matched = true
		// Generate mode: drop a patch that is no longer needed (exact match).
		if mode == PatchModeGenerate && patchExists && bytes.Equal(baseline, expected) {
			_ = os.Remove(patchPath)
			out.PatchApplied = false
			out.PatchPath = ""
		}
		return out, nil
	}

	if mode == PatchModeVerify {
		out.ResidualDiff = string(diff)
		return out, nil
	}

	// Generate mode: (re)write the patch from postgres -> multigateway.
	newPatch, err := generateDiff(ctx, expected, actual)
	if err != nil {
		return out, fmt.Errorf("regenerate patch for %s: %w", name, err)
	}
	if len(newPatch) == 0 {
		if patchExists {
			_ = os.Remove(patchPath)
		}
		out.Matched = true
		return out, nil
	}
	// Preserve any hand-written comment preamble from the existing patch so a
	// regeneration doesn't drop the explanation of why this divergence exists.
	var comment []byte
	if patchExists {
		if raw, rerr := os.ReadFile(patchPath); rerr == nil {
			comment, _ = splitPatchComment(raw)
		}
	}
	if err := os.MkdirAll(filepath.Dir(patchPath), 0o755); err != nil {
		return out, fmt.Errorf("mkdir for patch %s: %w", patchPath, err)
	}
	finalPatch := append(append([]byte{}, comment...), newPatch...)
	if err := os.WriteFile(patchPath, finalPatch, 0o644); err != nil {
		return out, fmt.Errorf("write patch %s: %w", patchPath, err)
	}
	out.Matched = true
	out.PatchApplied = true
	out.PatchPath = patchPath
	return out, nil
}

// applyPatch applies patchPath to original via the system `patch` utility and
// returns the patched bytes, without mutating any input file. The diff body is
// the same `diff -U3` a developer would produce by hand or via generate mode;
// any leading comment preamble (see splitPatchComment) is stripped first.
func applyPatch(ctx context.Context, original []byte, patchPath string) ([]byte, error) {
	rawPatch, err := os.ReadFile(patchPath)
	if err != nil {
		return nil, err
	}
	_, body := splitPatchComment(rawPatch)

	tmpDir, err := os.MkdirTemp("", "pgproto-patch-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "expected")
	dstPath := filepath.Join(tmpDir, "patched")
	if err := os.WriteFile(srcPath, original, 0o644); err != nil {
		return nil, err
	}

	cmd := executil.Command(ctx, "patch",
		"--no-backup-if-mismatch", "--force", "--silent",
		"-o", dstPath, srcPath,
	)
	cmd.Cmd.Stdin = bytes.NewReader(body)
	var stderr bytes.Buffer
	cmd.Cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("patch exited with error: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return os.ReadFile(dstPath)
}

// splitPatchComment splits a patch file into its leading comment preamble (any
// lines before the first unified-diff header line, "--- ") and the diff body.
// The preamble lets a patch explain the known divergence it records; it is
// stripped before the diff is applied and preserved across regeneration in
// generate mode, so hand-written explanations survive `make
// pgproto-update-patches`.
func splitPatchComment(content []byte) (comment, body []byte) {
	prefix := []byte("--- ")
	if bytes.HasPrefix(content, prefix) {
		return nil, content
	}
	if i := bytes.Index(content, append([]byte("\n"), prefix...)); i >= 0 {
		return content[:i+1], content[i+1:]
	}
	// No diff header found: treat the whole file as body (lets a malformed
	// patch surface as a patch-apply error rather than being silently dropped).
	return nil, content
}

// generateDiff runs `diff -U3` against the two byte slices and returns the
// unified diff (empty when identical). Stable `--label`s keep patch headers free
// of temp paths so regenerated patches don't churn.
func generateDiff(ctx context.Context, a, b []byte) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "pgproto-diff-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	aPath := filepath.Join(tmpDir, "a")
	bPath := filepath.Join(tmpDir, "b")
	if err := os.WriteFile(aPath, a, 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(bPath, b, 0o644); err != nil {
		return nil, err
	}

	cmd := executil.Command(ctx, "diff", "-U3", "--label", "a", "--label", "b", aPath, bPath)
	var stdout, stderr bytes.Buffer
	cmd.Cmd.Stdout = &stdout
	cmd.Cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return stdout.Bytes(), nil // exit 1 = files differ
		}
		return nil, fmt.Errorf("diff failed: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil, nil // exit 0 = identical
}

// ensureTrailingNewline appends a newline if missing, so diff/patch operate on
// whole lines and don't emit "\ No newline at end of file" noise.
func ensureTrailingNewline(s string) string {
	if s == "" || strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
