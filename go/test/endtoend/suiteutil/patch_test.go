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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func requirePatchTools(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("patch"); err != nil {
		t.Skip("patch tool not available")
	}
	if _, err := exec.LookPath("diff"); err != nil {
		t.Skip("diff tool not available")
	}
}

// patchInput writes a patch file (when non-empty) under a fresh temp dir and
// returns a PatchInput for the given expected/actual bytes.
func patchInput(t *testing.T, expected, actual, patch string) PatchInput {
	t.Helper()
	dir := t.TempDir()
	patchPath := filepath.Join(dir, "patches", "case.patch")
	if patch != "" {
		if err := os.MkdirAll(filepath.Dir(patchPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(patchPath, []byte(patch), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return PatchInput{Expected: []byte(expected), Actual: []byte(actual), PatchPath: patchPath}
}

func TestPatchModeFromEnv(t *testing.T) {
	const env = "SUITEUTIL_TEST_PATCH_MODE"
	cases := []struct {
		val  string
		want PatchMode
	}{
		{"", PatchModeVerify},
		{"verify", PatchModeVerify},
		{"generate", PatchModeGenerate},
		{"nonsense", PatchModeVerify},
	}
	for _, tc := range cases {
		t.Setenv(env, tc.val)
		if got := PatchModeFromEnv(env); got != tc.want {
			t.Errorf("PatchModeFromEnv(%q) = %q, want %q", tc.val, got, tc.want)
		}
	}
}

func TestVerifyPatch_NoDiff_NoPatch_Matches(t *testing.T) {
	requirePatchTools(t)
	content := "SELECT 1;\n one\n-----\n   1\n(1 row)\n"
	in := patchInput(t, content, content, "")
	out, err := VerifyPatch(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Matched || out.PatchApplied {
		t.Errorf("got %+v, want Matched=true PatchApplied=false", out)
	}
}

func TestVerifyPatch_DiffWithoutPatch_Fails(t *testing.T) {
	requirePatchTools(t)
	in := patchInput(t, "value 1\n", "value 2\n", "")
	out, err := VerifyPatch(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if out.Matched {
		t.Error("expected no match")
	}
	if out.ResidualDiff == "" {
		t.Error("expected a non-empty residual diff")
	}
	if out.ApplyErr != "" {
		t.Errorf("expected no apply error, got %q", out.ApplyErr)
	}
}

func TestVerifyPatch_DiffAbsorbedByPatch_Matches(t *testing.T) {
	requirePatchTools(t)
	exp := "line1\nERROR:  original message\nline3\n"
	act := "line1\nERROR:  proxy message\nline3\n"
	patch, err := GenerateDiff(t.Context(), []byte(exp), []byte(act))
	if err != nil || len(patch) == 0 {
		t.Fatalf("could not generate test patch: %v (len=%d)", err, len(patch))
	}
	in := patchInput(t, exp, act, string(patch))
	out, err := VerifyPatch(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Matched || !out.PatchApplied {
		t.Errorf("got %+v, want Matched=true PatchApplied=true", out)
	}
}

func TestVerifyPatch_StalePatch_SetsApplyErr(t *testing.T) {
	requirePatchTools(t)
	stalePatch := "--- a\n+++ b\n@@ -1,1 +1,1 @@\n-this_text_is_not_in_expected\n+anything\n"
	in := patchInput(t, "totally different content\n", "totally different content\n", stalePatch)
	out, err := VerifyPatch(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if out.Matched {
		t.Error("expected no match for a stale patch")
	}
	if !strings.Contains(out.ApplyErr, "patch") {
		t.Errorf("expected ApplyErr to mention the patch, got %q", out.ApplyErr)
	}
}

func TestVerifyPatch_Generate_WritesPatch(t *testing.T) {
	requirePatchTools(t)
	in := patchInput(t, "hello\nworld\n", "hello\nmultigres\n", "")

	out, err := VerifyPatch(t.Context(), in, PatchModeGenerate)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Matched || !out.PatchApplied {
		t.Errorf("generate mode should match and apply, got %+v", out)
	}
	if _, err := os.Stat(in.PatchPath); err != nil {
		t.Fatalf("patch file not written: %v", err)
	}

	// Re-running in verify mode should now match via the generated patch.
	out2, err := VerifyPatch(t.Context(), in, PatchModeVerify)
	if err != nil {
		t.Fatal(err)
	}
	if !out2.Matched || !out2.PatchApplied {
		t.Errorf("verify after generate should match via patch, got %+v", out2)
	}
}

func TestVerifyPatch_Generate_RemovesRedundantPatch(t *testing.T) {
	requirePatchTools(t)
	content := "exactly matching\n"
	// A patch that does not apply; in generate mode the apply failure is
	// absorbed, and since expected == actual the patch is deemed redundant and
	// removed.
	stalePatch := "--- a\n+++ b\n@@ -1,1 +1,1 @@\n-obsolete\n+obsolete_too\n"
	in := patchInput(t, content, content, stalePatch)

	out, err := VerifyPatch(t.Context(), in, PatchModeGenerate)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Matched || out.PatchApplied {
		t.Errorf("got %+v, want Matched=true PatchApplied=false", out)
	}
	if _, err := os.Stat(in.PatchPath); !os.IsNotExist(err) {
		t.Errorf("expected stale patch to be removed; stat err=%v", err)
	}
}

func TestVerifyPatch_Generate_PreservesComment(t *testing.T) {
	requirePatchTools(t)
	exp := "alpha\nbravo\n"
	act := "alpha\ncharlie\n"
	body, err := GenerateDiff(t.Context(), []byte(exp), []byte(act))
	if err != nil || len(body) == 0 {
		t.Fatalf("could not generate seed patch: %v", err)
	}
	const comment = "# why this diverges\n# second line\n"
	in := patchInput(t, exp, act, comment+string(body))

	// Change actual so the patch must be regenerated; the comment must survive.
	in.Actual = []byte("alpha\ndelta\n")
	out, err := VerifyPatch(t.Context(), in, PatchModeGenerate)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Matched || !out.PatchApplied {
		t.Errorf("got %+v, want Matched=true PatchApplied=true", out)
	}
	got, err := os.ReadFile(in.PatchPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(got), comment) {
		t.Errorf("regenerated patch lost its comment preamble; got:\n%s", got)
	}
}

func TestSplitPatchComment(t *testing.T) {
	t.Run("with comment preamble", func(t *testing.T) {
		in := "# why this diverges\n#\n# details\n--- a\n+++ b\n@@ -1 +1 @@\n-x\n+y\n"
		comment, body := SplitPatchComment([]byte(in))
		if string(comment) != "# why this diverges\n#\n# details\n" {
			t.Errorf("comment = %q", comment)
		}
		if string(body) != "--- a\n+++ b\n@@ -1 +1 @@\n-x\n+y\n" {
			t.Errorf("body = %q", body)
		}
	})

	t.Run("no comment (starts with diff header)", func(t *testing.T) {
		in := "--- a\n+++ b\n@@ -1 +1 @@\n-x\n+y\n"
		comment, body := SplitPatchComment([]byte(in))
		if len(comment) != 0 {
			t.Errorf("expected empty comment, got %q", comment)
		}
		if string(body) != in {
			t.Errorf("body = %q", body)
		}
	})
}

// TestApplyPatchWithComment proves a comment preamble is ignored when applying:
// generate a real diff, prepend a comment, and confirm ApplyPatch reproduces
// the target. Exercises the system `patch`/`diff` utilities (no cluster needed).
func TestApplyPatchWithComment(t *testing.T) {
	requirePatchTools(t)
	ctx := context.Background()
	original := []byte("FE=> Sync\n<= BE ParseComplete\n<= BE ErrorResponse(C 22012)\n")
	target := []byte("FE=> Sync\n<= BE ParseComplete\n<= BE BindComplete\n<= BE ErrorResponse(C 22012)\n")

	body, err := GenerateDiff(ctx, original, target)
	if err != nil {
		t.Fatalf("GenerateDiff: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("expected a non-empty diff")
	}

	patchPath := filepath.Join(t.TempDir(), "x.patch")
	commented := append([]byte("# explanatory comment\n# second line\n"), body...)
	if err := os.WriteFile(patchPath, commented, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := ApplyPatch(ctx, original, patchPath)
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if string(got) != string(target) {
		t.Errorf("ApplyPatch result mismatch\n got: %q\nwant: %q", got, target)
	}
}
