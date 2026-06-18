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

// Patch-based differential verification is shared by the pgregresstest and
// pgproto end-to-end suites. Both treat PostgreSQL as an oracle, accept a set of
// known divergences recorded as per-case unified-diff patches, and gate on
// "zero unpatched divergences". The mechanics — apply a patch to the expected
// bytes, diff against the actual bytes, and in generate mode (re)write the patch
// to absorb a residual diff — are identical across suites and live here.
//
// What stays in each suite is domain-specific *normalization* of the bytes
// (pgregresstest collapses whitespace in .out files; pgproto reduces wire-trace
// Error lines to their SQLSTATE) and the conventions for locating a patch file
// and rendering its path in a report. Callers normalize first, compute the
// patch path, then hand pre-normalized bytes to VerifyPatch.

// PatchMode selects the behavior of VerifyPatch.
type PatchMode string

const (
	// PatchModeVerify is strict: the actual bytes must match the (possibly
	// patched) expected bytes exactly, or the case is reported as a divergence.
	PatchModeVerify PatchMode = "verify"
	// PatchModeGenerate absorbs any residual divergence by (re)writing the
	// per-case patch from (expected -> actual), so the case ends up matching.
	// Stale patches (cases that now match exactly) are removed.
	PatchModeGenerate PatchMode = "generate"
)

// PatchModeFromEnv reads the mode from the named environment variable,
// defaulting to verify when unset, empty, or unrecognized (stay strict by
// default). The variable name differs per suite (e.g. PGREGRESS_PATCH_MODE,
// PGPROTO_PATCH_MODE), so it is supplied by the caller.
func PatchModeFromEnv(envVar string) PatchMode {
	if PatchMode(os.Getenv(envVar)) == PatchModeGenerate {
		return PatchModeGenerate
	}
	return PatchModeVerify
}

// PatchInput is one differential comparison. Expected and Actual are the two
// byte streams to compare; the caller has already normalized them (whitespace
// collapse, trace reduction, trailing-newline handling — all domain-specific).
// PatchPath is the absolute path to the per-case patch file; it need not exist.
type PatchInput struct {
	Expected  []byte
	Actual    []byte
	PatchPath string
}

// PatchOutcome is the result of a patch-based comparison. The caller maps it
// into its own report shape and renders PatchPath for display.
type PatchOutcome struct {
	// Matched is true when Actual equals the (possibly patched) Expected.
	Matched bool
	// PatchApplied is true when a patch file existed and was applied to reach
	// the match — i.e. the case passes via a recorded known divergence.
	PatchApplied bool
	// ResidualDiff is the unified diff (patched-expected vs actual) when the
	// case does not match in verify mode.
	ResidualDiff string
	// ApplyErr is non-empty when a patch existed but failed to apply in verify
	// mode (typically a stale patch after an upstream change). Kept separate
	// from ResidualDiff so callers can phrase the two failure modes distinctly.
	ApplyErr string
}

// VerifyPatch runs the shared verify/generate pipeline against pre-normalized
// bytes.
//
//   - verify mode: apply PatchPath to Expected (if present), diff vs Actual.
//     Empty diff -> Matched. A patch that fails to apply sets ApplyErr; a
//     residual diff sets ResidualDiff. Neither writes any file.
//   - generate mode: if the (possibly patched) baseline already equals Actual,
//     drop a now-redundant patch; otherwise (re)write PatchPath from
//     (Expected -> Actual) and report Matched. Any leading comment preamble in
//     an existing patch is preserved across regeneration (see SplitPatchComment).
func VerifyPatch(ctx context.Context, in PatchInput, mode PatchMode) (PatchOutcome, error) {
	var out PatchOutcome

	expected := in.Expected
	actual := in.Actual
	patchPath := in.PatchPath
	patchExists := FileExists(patchPath)

	// Baseline = expected bytes, optionally transformed by the patch.
	baseline := expected
	if patchExists {
		patched, applyErr := ApplyPatch(ctx, expected, patchPath)
		switch {
		case applyErr == nil:
			baseline = patched
			out.PatchApplied = true
		case mode == PatchModeVerify:
			out.ApplyErr = fmt.Sprintf("patch %s failed to apply (likely stale after an upstream change): %v", patchPath, applyErr)
			return out, nil
		default:
			baseline = expected // generate mode: regenerate below
		}
	}

	diff, err := GenerateDiff(ctx, baseline, actual)
	if err != nil {
		return out, fmt.Errorf("diff %s: %w", patchPath, err)
	}

	if len(diff) == 0 {
		out.Matched = true
		// Generate mode: drop a patch that is no longer needed. Only remove when
		// the patched baseline equals the unpatched expected, meaning the patch
		// is truly redundant (the case now matches upstream exactly).
		if mode == PatchModeGenerate && patchExists && bytes.Equal(baseline, expected) {
			_ = os.Remove(patchPath)
			out.PatchApplied = false
		}
		return out, nil
	}

	if mode == PatchModeVerify {
		out.ResidualDiff = string(diff)
		return out, nil
	}

	// Generate mode: (re)write the patch from unpatched expected -> actual.
	newPatch, err := GenerateDiff(ctx, expected, actual)
	if err != nil {
		return out, fmt.Errorf("regenerate patch %s: %w", patchPath, err)
	}
	if len(newPatch) == 0 {
		// Should not happen given a non-empty residual diff above, but be
		// defensive: remove any existing patch and treat as a clean match.
		if patchExists {
			_ = os.Remove(patchPath)
		}
		out.Matched = true
		out.PatchApplied = false
		return out, nil
	}

	// Preserve any hand-written comment preamble from the existing patch so a
	// regeneration doesn't drop the explanation of why this divergence exists.
	var comment []byte
	if patchExists {
		if raw, rerr := os.ReadFile(patchPath); rerr == nil {
			comment, _ = SplitPatchComment(raw)
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
	return out, nil
}

// ApplyPatch applies patchPath to original via the system `patch` utility and
// returns the patched bytes, without mutating any input file. Shelling out keeps
// the on-disk format exactly what a developer would produce by hand with
// `diff -U3 expected actual > patch`. Any leading comment preamble (see
// SplitPatchComment) is stripped before the diff body is applied.
//
// The original is written to a temp file and patch writes to a separate temp
// file (`-o`), so patch(1) never mutates anything in place.
func ApplyPatch(ctx context.Context, original []byte, patchPath string) ([]byte, error) {
	rawPatch, err := os.ReadFile(patchPath)
	if err != nil {
		return nil, err
	}
	_, body := SplitPatchComment(rawPatch)

	tmpDir, err := os.MkdirTemp("", "suiteutil-patch-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "expected")
	dstPath := filepath.Join(tmpDir, "patched")
	if err := os.WriteFile(srcPath, original, 0o644); err != nil {
		return nil, err
	}

	// `patch -o dstPath srcPath < body` applies the patch to srcPath and writes
	// the result to dstPath, without touching srcPath.
	cmd := executil.Command(ctx, "patch",
		"--no-backup-if-mismatch", "--force", "--silent",
		"-o", dstPath, srcPath,
	)
	cmd.Stdin = bytes.NewReader(body)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("patch exited with error: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return os.ReadFile(dstPath)
}

// GenerateDiff runs `diff -U3 --label a --label b` against the two byte slices
// and returns the unified diff (empty when identical). Inputs are expected to
// already be canonicalized by the caller, so no `-b` is passed (BSD and GNU diff
// drift on `-b` for some whitespace-only changes).
//
// The stable `--label`s replace the `---`/`+++` header lines with fixed literals
// so patch files don't embed absolute temp paths or per-run timestamps;
// otherwise every regenerated patch would churn on its header.
//
// Errors are returned only on actual failure (exit code > 1); exit code 1
// ("differences found") is normal.
func GenerateDiff(ctx context.Context, a, b []byte) ([]byte, error) {
	tmpDir, err := os.MkdirTemp("", "suiteutil-diff-*")
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
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return stdout.Bytes(), nil // exit 1 = files differ
		}
		return nil, fmt.Errorf("diff failed: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil, nil // exit 0 = identical
}

// SplitPatchComment splits a patch file into its leading comment preamble (any
// lines before the first unified-diff header line, "--- ") and the diff body.
// The preamble lets a patch explain the known divergence it records; it is
// stripped before the diff is applied and preserved across regeneration in
// generate mode, so hand-written explanations survive patch updates.
//
// A patch with no comment (starting directly with "--- ") returns an empty
// comment and the whole content as the body. A file with no diff header at all
// is treated as all-body, so a malformed patch surfaces as a patch-apply error
// rather than being silently dropped.
func SplitPatchComment(content []byte) (comment, body []byte) {
	prefix := []byte("--- ")
	if bytes.HasPrefix(content, prefix) {
		return nil, content
	}
	if i := bytes.Index(content, append([]byte("\n"), prefix...)); i >= 0 {
		return content[:i+1], content[i+1:]
	}
	return nil, content
}

// FileExists reports whether path exists (any stat error, including
// not-exist, is treated as "does not exist").
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
