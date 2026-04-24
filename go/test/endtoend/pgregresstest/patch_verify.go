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

// PatchMode selects the behavior of VerifyTest:
//   - PatchModeVerify: strict diff check; existing patches must produce a clean
//     match (no residual diff), missing patches cause fail when a diff exists.
//   - PatchModeGenerate: any residual diff is absorbed by rewriting the
//     per-test patch file, so the test ends up passing. Stale patches (tests
//     that now match upstream exactly) have their patch files deleted.
type PatchMode string

const (
	PatchModeVerify   PatchMode = "verify"
	PatchModeGenerate PatchMode = "generate"
)

// PatchModeEnv is the environment variable that selects the mode. When unset
// or empty, verify mode is used.
const PatchModeEnv = "PGREGRESS_PATCH_MODE"

// GetPatchMode reads the mode from the environment. Defaults to verify.
func GetPatchMode() PatchMode {
	v := os.Getenv(PatchModeEnv)
	switch PatchMode(v) {
	case PatchModeGenerate:
		return PatchModeGenerate
	case "", PatchModeVerify:
		return PatchModeVerify
	default:
		// Unknown value: treat as verify to stay strict by default.
		return PatchModeVerify
	}
}

// VerifyOutcome is the result of applying patch-based verification to one test.
type VerifyOutcome struct {
	Name         string `json:"name"`
	Status       string `json:"status"` // "pass" | "fail"
	PatchApplied bool   `json:"patch_applied"`
	// PatchPath is the path to the patch file relative to the repo root, for
	// links in status reports. Empty if no patch was used.
	PatchPath string `json:"patch_path,omitempty"`
	// Reason is a short human-readable description when Status == "fail".
	Reason string `json:"reason,omitempty"`
	// Diff is the residual unified diff on failure. Populated only on fail.
	Diff string `json:"diff,omitempty"`
}

// VerifyInput describes one test's inputs to VerifyTest. All paths are absolute.
type VerifyInput struct {
	// Name is the bare test name (e.g. "boolean").
	Name string
	// ExpectedPath points at postgres's checked-in expected output
	// (e.g. /tmp/multigres_pg_cache/source/postgres/src/test/regress/expected/boolean.out).
	ExpectedPath string
	// ActualPath points at the output produced by this test run
	// (e.g. <buildDir>/src/test/regress/results/boolean.out).
	ActualPath string
	// PatchDir is the directory containing per-test patch files
	// (e.g. <repo>/go/test/endtoend/pgregresstest/testdata/pg17/patches).
	PatchDir string
	// RepoRoot is used only to compute a nice relative PatchPath for reports.
	RepoRoot string
}

// VerifyTest runs the patch-based verification pipeline for one test.
//
// Pipeline:
//  1. Read expected and actual.
//  2. If a patch file exists at PatchDir/<Name>.patch, apply it to expected.
//     - verify mode: patch failure => fail with reason "patch did not apply".
//     - generate mode: patch failure just means we'll regenerate from scratch.
//  3. Diff patched-expected against actual.
//  4. Empty diff => pass.
//  5. Non-empty diff:
//     - verify mode => fail with the residual diff.
//     - generate mode => write a fresh patch from (expected -> actual) and
//     report pass with PatchApplied=true.
//
// generate mode also deletes stale patches (when the current test now matches
// upstream exactly, any existing patch is removed).
func VerifyTest(ctx context.Context, in VerifyInput, mode PatchMode) (*VerifyOutcome, error) {
	out := &VerifyOutcome{Name: in.Name}

	expected, err := os.ReadFile(in.ExpectedPath)
	if err != nil {
		return nil, fmt.Errorf("read expected %q: %w", in.ExpectedPath, err)
	}
	actual, err := os.ReadFile(in.ActualPath)
	if err != nil {
		return nil, fmt.Errorf("read actual %q: %w", in.ActualPath, err)
	}

	patchPath := filepath.Join(in.PatchDir, in.Name+".patch")
	patchExists := fileExists(patchPath)

	// Stage 1: compute the baseline expected (possibly patched).
	var baseline []byte
	baseline = expected
	if patchExists {
		patched, applyErr := applyPatch(ctx, expected, patchPath)
		switch {
		case applyErr == nil:
			baseline = patched
			out.PatchApplied = true
			out.PatchPath = relForReport(in.RepoRoot, patchPath)
		case mode == PatchModeVerify:
			out.Status = "fail"
			out.Reason = fmt.Sprintf("patch %s failed to apply (likely stale after an upstream change): %v", relForReport(in.RepoRoot, patchPath), applyErr)
			return out, nil
		default:
			// generate mode: treat as if no patch existed, we'll regenerate below.
			baseline = expected
		}
	}

	// Stage 2: strict diff baseline vs actual.
	diff, err := generateDiff(ctx, baseline, actual)
	if err != nil {
		return nil, fmt.Errorf("diff %s: %w", in.Name, err)
	}
	if len(diff) == 0 {
		out.Status = "pass"
		// Generate mode: if a patch exists but is unnecessary (stock upstream
		// already matches), remove it so patches don't accumulate cruft.
		if mode == PatchModeGenerate && patchExists {
			// Only remove if not needed: patched baseline happens to equal
			// unpatched expected ⇒ patch is a no-op. Safer heuristic: only
			// remove when the patched content actually equals the unpatched,
			// meaning the patch is truly redundant.
			if bytes.Equal(baseline, expected) {
				_ = os.Remove(patchPath)
				out.PatchApplied = false
				out.PatchPath = ""
			}
		}
		return out, nil
	}

	// Stage 3: residual diff present.
	if mode == PatchModeVerify {
		out.Status = "fail"
		out.Reason = "actual output does not match patched expected"
		out.Diff = string(diff)
		return out, nil
	}

	// generate mode: regenerate the patch from unpatched expected vs actual.
	newPatch, err := generateDiff(ctx, expected, actual)
	if err != nil {
		return nil, fmt.Errorf("regenerate patch for %s: %w", in.Name, err)
	}
	if len(newPatch) == 0 {
		// Should not happen given we're here with a non-empty residual diff,
		// but be defensive: remove any existing patch and treat as pass.
		if patchExists {
			_ = os.Remove(patchPath)
		}
		out.Status = "pass"
		out.PatchApplied = false
		out.PatchPath = ""
		return out, nil
	}

	if err := os.MkdirAll(filepath.Dir(patchPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir for patch %s: %w", patchPath, err)
	}
	if err := os.WriteFile(patchPath, newPatch, 0o644); err != nil {
		return nil, fmt.Errorf("write patch %s: %w", patchPath, err)
	}
	out.Status = "pass"
	out.PatchApplied = true
	out.PatchPath = relForReport(in.RepoRoot, patchPath)
	return out, nil
}

// applyPatch applies the contents of patchPath to original and returns the
// patched bytes. Shells out to the system `patch` utility so that the file
// format matches exactly what a developer would produce with
// `diff -U3 expected actual > patch` or `make pgregress-update-patches`.
//
// The original is fed to patch on stdin via a temp file, and patched output is
// written to a temp file then read back. This avoids patch(1)'s in-place
// mutations.
func applyPatch(ctx context.Context, original []byte, patchPath string) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "pgregress-patch-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "expected")
	dstPath := filepath.Join(tmpDir, "patched")
	if err := os.WriteFile(srcPath, original, 0o644); err != nil {
		return nil, err
	}

	// `patch -o dstPath srcPath < patchPath` applies the patch to srcPath and
	// writes the result to dstPath, without touching srcPath.
	cmd := executil.Command(ctx, "patch",
		"--no-backup-if-mismatch",
		"--force",
		"--silent",
		"-o", dstPath,
		srcPath,
	)
	patchReader, err := os.Open(patchPath)
	if err != nil {
		return nil, err
	}
	defer patchReader.Close()
	cmd.Stdin = patchReader
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("patch exited with error: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return os.ReadFile(dstPath)
}

// generateDiff runs `diff -U3 -b --label a --label b` and returns the unified
// diff bytes. Returns an empty slice when files are identical (modulo
// whitespace).
//
// The `-b` flag ignores changes in the amount of whitespace — lines that
// differ only in spacing (e.g. the `^` caret position under a `LINE N:`
// error block) are treated as matching. This lets us accept psql-format
// nits that don't represent real regressions without maintaining a patch
// per test file for every column-alignment tweak.
//
// The `--label` flags replace the `---`/`+++` header lines with stable
// literals so patch files don't embed absolute temp-directory paths or
// per-run timestamps. Without this, every regenerated patch would churn
// on its header even when the hunks are unchanged.
//
// Errors are returned only on actual failure (exit code > 1); exit code 1
// ("differences found") is normal.
func generateDiff(ctx context.Context, a, b []byte) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "pgregress-diff-*")
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

	cmd := executil.Command(ctx, "diff", "-U3", "-b",
		"--label", "a",
		"--label", "b",
		aPath, bPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			// Exit 1 just means "files differ".
			return stdout.Bytes(), nil
		}
		return nil, fmt.Errorf("diff failed: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	// Exit 0: identical.
	return nil, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// relForReport returns path relative to repoRoot when possible, otherwise the
// absolute path unchanged. Used purely to prettify PatchPath in JSON output.
func relForReport(repoRoot, path string) string {
	if repoRoot == "" {
		return path
	}
	rel, err := filepath.Rel(repoRoot, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return path
	}
	return rel
}
