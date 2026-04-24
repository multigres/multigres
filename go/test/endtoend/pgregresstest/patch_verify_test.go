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
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// fixture builds expected/actual/patchDir with the given contents and returns
// a VerifyInput pointing at them.
func fixture(t *testing.T, name, expected, actual, patch string) VerifyInput {
	t.Helper()
	dir := t.TempDir()

	expDir := filepath.Join(dir, "expected")
	actDir := filepath.Join(dir, "results")
	patchDir := filepath.Join(dir, "patches")
	for _, d := range []string{expDir, actDir, patchDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	expPath := filepath.Join(expDir, name+".out")
	actPath := filepath.Join(actDir, name+".out")
	if err := os.WriteFile(expPath, []byte(expected), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(actPath, []byte(actual), 0o644); err != nil {
		t.Fatal(err)
	}
	if patch != "" {
		if err := os.WriteFile(filepath.Join(patchDir, name+".patch"), []byte(patch), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	return VerifyInput{
		Name:         name,
		ExpectedPath: expPath,
		ActualPath:   actPath,
		PatchDir:     patchDir,
		RepoRoot:     dir,
	}
}

func requirePatchTool(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("patch"); err != nil {
		t.Skip("patch tool not available")
	}
	if _, err := exec.LookPath("diff"); err != nil {
		t.Skip("diff tool not available")
	}
}

func TestVerify_NoDiff_NoPatch_Passes(t *testing.T) {
	requirePatchTool(t)
	content := "SELECT 1;\n one\n-----\n   1\n(1 row)\n\n"
	in := fixture(t, "simple", content, content, "")
	out, err := VerifyTest(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "pass" {
		t.Errorf("got status %q, want pass; reason=%q diff=%q", out.Status, out.Reason, out.Diff)
	}
	if out.PatchApplied {
		t.Errorf("patch should not be applied")
	}
}

func TestVerify_WhitespaceOnlyDiff_Passes(t *testing.T) {
	requirePatchTool(t)
	// Actual differs from expected only in the position of the caret `^`
	// under the LINE N: error context — a classic psql caret-shift. With
	// `diff -b` this should match without needing a patch.
	exp := "ERROR:  syntax error\nLINE 1: INSERT INTO t VALUES ('x');\n                              ^\n"
	act := "ERROR:  syntax error\nLINE 1: INSERT INTO t VALUES ('x');\n                          ^\n"
	in := fixture(t, "wsonly", exp, act, "")
	out, err := VerifyTest(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "pass" {
		t.Errorf("whitespace-only diff should pass in verify mode, got %q (diff=%q)", out.Status, out.Diff)
	}
	if out.PatchApplied {
		t.Errorf("no patch should have been applied for a whitespace-only diff, got PatchApplied=true path=%q", out.PatchPath)
	}
}

func TestVerify_DiffWithoutPatch_Fails(t *testing.T) {
	requirePatchTool(t)
	exp := "SELECT 1;\n one\n-----\n   1\n(1 row)\n"
	act := "SELECT 1;\n one\n-----\n   2\n(1 row)\n"
	in := fixture(t, "mismatch", exp, act, "")
	out, err := VerifyTest(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "fail" {
		t.Errorf("expected fail, got %q", out.Status)
	}
	if out.Diff == "" {
		t.Error("expected non-empty diff on failure")
	}
}

func TestVerify_DiffAbsorbedByPatch_Passes(t *testing.T) {
	requirePatchTool(t)
	exp := "line1\nERROR:  invalid input syntax for type boolean: \"XXX\"\nLINE 2:    VALUES (bool 'XXX');\n                        ^\nline5\n"
	act := "line1\nERROR:  parse error at position 64: invalid boolean\nline5\n"

	// Generate the patch by running diff between exp and act. In real usage
	// this is what `make pgregress-update-patches` produces.
	patch, err := generateDiff(t.Context(), []byte(exp), []byte(act))
	if err != nil || len(patch) == 0 {
		t.Fatalf("could not generate test patch: %v (len=%d)", err, len(patch))
	}

	in := fixture(t, "boolean", exp, act, string(patch))
	out, err := VerifyTest(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "pass" {
		t.Errorf("expected pass, got %q (reason=%q diff=%q)", out.Status, out.Reason, out.Diff)
	}
	if !out.PatchApplied {
		t.Errorf("expected PatchApplied=true")
	}
	if out.PatchPath == "" {
		t.Error("expected non-empty PatchPath")
	}
}

func TestVerify_StalePatchFailsInVerifyMode(t *testing.T) {
	requirePatchTool(t)
	// A patch that won't apply to the current expected file.
	stalePatch := `--- a/x
+++ b/x
@@ -1,1 +1,1 @@
-this_text_is_not_in_expected
+anything
`
	in := fixture(t, "stale", "totally different content\n", "totally different content\n", stalePatch)
	out, err := VerifyTest(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "fail" {
		t.Errorf("expected fail for stale patch, got %q", out.Status)
	}
	if !contains(out.Reason, "patch") {
		t.Errorf("expected failure reason to mention patch, got %q", out.Reason)
	}
}

func TestGenerate_WritesPatchOnDiff(t *testing.T) {
	requirePatchTool(t)
	exp := "hello\nworld\n"
	act := "hello\nmultigres\n"
	in := fixture(t, "newdiff", exp, act, "")

	out, err := VerifyTest(t.Context(), in, PatchModeGenerate)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "pass" {
		t.Errorf("generate mode should always pass, got %q", out.Status)
	}
	if !out.PatchApplied {
		t.Errorf("expected PatchApplied=true after generating patch")
	}

	// A patch file should now exist and re-running verify should pass.
	patchFile := filepath.Join(in.PatchDir, in.Name+".patch")
	if _, err := os.Stat(patchFile); err != nil {
		t.Fatalf("patch file not written: %v", err)
	}
	out2, err := VerifyTest(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if out2.Status != "pass" {
		t.Errorf("verify after generate should pass, got %q (reason=%q diff=%q)", out2.Status, out2.Reason, out2.Diff)
	}
}

func TestGenerate_RemovesRedundantPatch(t *testing.T) {
	requirePatchTool(t)
	// Expected and actual match exactly, but a stale patch exists.
	content := "exactly matching\n"
	// Stale patch with trivially applicable no-op would fail to apply to an
	// empty-or-redundant context; simulate a redundant patch by constructing
	// a patch that happens to equal the original (context-only, no change).
	// Simpler: put a patch file that doesn't apply at all. In generate mode,
	// apply failure is absorbed, and since the files already match, the patch
	// is deemed redundant and removed.
	stalePatch := `--- a/x
+++ b/x
@@ -1,1 +1,1 @@
-obsolete
+obsolete_too
`
	in := fixture(t, "removable", content, content, stalePatch)

	out, err := VerifyTest(t.Context(), in, PatchModeGenerate)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "pass" {
		t.Errorf("expected pass, got %q", out.Status)
	}
	if out.PatchApplied {
		t.Errorf("stale patch should not be reported as applied after cleanup")
	}
	patchFile := filepath.Join(in.PatchDir, in.Name+".patch")
	if _, err := os.Stat(patchFile); !os.IsNotExist(err) {
		t.Errorf("expected stale patch to be removed; stat err=%v", err)
	}
}

func contains(s, substr string) bool {
	return len(substr) == 0 || (len(s) >= len(substr) && stringsContains(s, substr))
}

func stringsContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
