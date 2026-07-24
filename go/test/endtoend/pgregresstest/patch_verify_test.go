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

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
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

	// Generate the patch by running diff between normalized exp and normalized
	// act. In real usage this is what VerifyTest does internally on every
	// run; tests that bypass VerifyTest must normalize first so the patch
	// applies to the normalized expected that VerifyTest will hand back.
	patch, err := suiteutil.GenerateDiff(t.Context(), normalizeWhitespace([]byte(exp)), normalizeWhitespace([]byte(act)))
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

func TestExpectedVariantsIncludesZeroThroughNine(t *testing.T) {
	regressDir := t.TempDir()
	expectedDir := filepath.Join(regressDir, "expected")
	if err := os.Mkdir(expectedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"variant.out", "variant_0.out", "variant_1.out", "variant_9.out", "variant_10.out"} {
		if err := os.WriteFile(filepath.Join(expectedDir, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	got := expectedVariants(regressDir, "variant")
	want := []string{
		filepath.Join(expectedDir, "variant.out"),
		filepath.Join(expectedDir, "variant_0.out"),
		filepath.Join(expectedDir, "variant_1.out"),
		filepath.Join(expectedDir, "variant_9.out"),
	}
	if len(got) != len(want) {
		t.Fatalf("expectedVariants() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expectedVariants()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPreferExpectedVariant(t *testing.T) {
	variants := []string{"/expected/xml.out", "/expected/xml_1.out", "/expected/xml_2.out"}
	got, err := preferExpectedVariant(variants, "xml_1.out")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/expected/xml_1.out", "/expected/xml.out", "/expected/xml_2.out"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("preferExpectedVariant()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if _, err := preferExpectedVariant(variants, "xml_9.out"); err == nil {
		t.Fatal("missing preferred variant should fail")
	}
}

func TestExpectedFileForPatchUsesExplicitVariant(t *testing.T) {
	regressDir := t.TempDir()
	expectedDir := filepath.Join(regressDir, "expected")
	patchDir := filepath.Join(regressDir, "patches")
	for _, dir := range []string{expectedDir, patchDir} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	canonical := filepath.Join(expectedDir, "xmlmap.out")
	alternate := filepath.Join(expectedDir, "xmlmap_1.out")
	for _, path := range []string{canonical, alternate} {
		if err := os.WriteFile(path, []byte("expected\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	patch := "# no-libxml baseline\n# pgregress-expected-file: xmlmap_1.out\n--- a\n+++ b\n"
	if err := os.WriteFile(filepath.Join(patchDir, "xmlmap.patch"), []byte(patch), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := expectedFileForPatch(regressDir, "xmlmap", patchDir, []string{canonical, alternate})
	if err != nil {
		t.Fatal(err)
	}
	if got != alternate {
		t.Fatalf("expectedFileForPatch() = %q, want %q", got, alternate)
	}
}

func TestExpectedFileForPatchRejectsInvalidMetadata(t *testing.T) {
	regressDir := t.TempDir()
	expectedDir := filepath.Join(regressDir, "expected")
	patchDir := filepath.Join(regressDir, "patches")
	for _, dir := range []string{expectedDir, patchDir} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	canonical := filepath.Join(expectedDir, "xml.out")
	if err := os.WriteFile(canonical, []byte("expected\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	patch := "# pgregress-expected-file: ../xml_1.out\n--- a\n+++ b\n"
	if err := os.WriteFile(filepath.Join(patchDir, "xml.patch"), []byte(patch), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := expectedFileForPatch(regressDir, "xml", patchDir, []string{canonical}); err == nil {
		t.Fatal("expected invalid expected-file metadata to fail")
	}
}

func TestVerifyWithPatchesAcceptsCoreAlternateExpected(t *testing.T) {
	sourceDir := filepath.Join(t.TempDir(), "source")
	regressDir := filepath.Join(sourceDir, "src", "test", "regress")
	buildRegressDir := filepath.Join(t.TempDir(), "regress")
	for _, dir := range []string{filepath.Join(regressDir, "expected"), filepath.Join(buildRegressDir, "results")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	const testName = "core_variant_unit"
	if err := os.WriteFile(filepath.Join(regressDir, "expected", testName+".out"), []byte("canonical\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(regressDir, "expected", testName+"_1.out"), []byte("alternate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(buildRegressDir, "results", testName+".out"), []byte("alternate\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv(PatchModeEnv, string(PatchModeVerify))
	pb := &PostgresBuilder{Builder: &pgbuilder.Builder{SourceDir: sourceDir}}
	results := &TestResults{
		TotalTests:  1,
		FailedTests: 1,
		Tests:       []IndividualTestResult{{Name: testName, Status: "fail"}},
	}
	if err := pb.VerifyWithPatches(t, t.Context(), results, buildRegressDir, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if results.PassedTests != 1 || results.FailedTests != 0 || results.Tests[0].PatchApplied {
		t.Fatalf("alternate core output not accepted cleanly: %+v", results)
	}
}

func TestMarkdownSummaryClassifiesCompatibilityResults(t *testing.T) {
	pb := &PostgresBuilder{Builder: &pgbuilder.Builder{OutputDir: t.TempDir()}}
	results := &TestResults{
		TotalTests: 3,
		Tests: []IndividualTestResult{
			{Name: "stock", Status: "pass"},
			{Name: "accepted", Status: "pass", PatchApplied: true, PatchPath: "accepted.patch"},
			{Name: "residual", Status: "fail"},
		},
	}
	summary, err := pb.WriteMarkdownSummary(t, []SuiteResult{{Name: "Regression Tests", Results: results}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"✅ compatible", "✅ accepted divergence", "❌ residual failure"} {
		if !contains(summary, want) {
			t.Fatalf("summary does not contain %q:\n%s", want, summary)
		}
	}
}

func TestMarkUpstreamCompatibleRemovesStalePatchInGenerateMode(t *testing.T) {
	patchDir := t.TempDir()
	patchPath := filepath.Join(patchDir, "variant.patch")
	if err := os.WriteFile(patchPath, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	test := &IndividualTestResult{Name: "variant", Status: "fail", PatchApplied: true, PatchPath: patchPath, FailReason: "old"}

	markUpstreamCompatible(test, patchDir, PatchModeGenerate)

	if test.Status != "pass" || test.PatchApplied || test.PatchPath != "" || test.FailReason != "" {
		t.Fatalf("markUpstreamCompatible() left stale state: %+v", test)
	}
	if _, err := os.Stat(patchPath); !os.IsNotExist(err) {
		t.Fatalf("stale patch was not removed: %v", err)
	}
}

func TestNormalizeIsolationStats(t *testing.T) {
	in := "test_stat_func| 2|t |t\n" +
		"test_stat_func2| 9|t |t\n" +
		"1|2|3|4|5|6|7|8\n" +
		"seq_scan|seq_tup_read|n_tup_ins|n_tup_upd|n_tup_del|n_live_tup|n_dead_tup|vacuum_count\n" +
		"--------+------------+---------+---------+---------+----------+----------+------------\n" +
		" 9| 31| 4| 5| 1| 3| 6| 0\n"
	want := "test_stat_func|<calls>|t|t\n" +
		"test_stat_func2|<calls>|t|t\n" +
		"1|2|3|4|5|6|7|8\n" +
		"seq_scan|seq_tup_read|n_tup_ins|n_tup_upd|n_tup_del|n_live_tup|n_dead_tup|vacuum_count\n" +
		"--------+------------+---------+---------+---------+----------+----------+------------\n" +
		"<seq_scan>|<seq_tup_read>|4|5|1|3|6|0\n"
	if got := string(normalizeTestOutput("stats", "/patches/isolation", []byte(in))); got != want {
		t.Fatalf("normalizeIsolationStats = %q, want %q", got, want)
	}
	if got := string(normalizeTestOutput("stats", "/patches", []byte(in))); got != in {
		t.Fatalf("regression stats output changed: %q", got)
	}
}

func TestNormalizePoolerPreparedNames(t *testing.T) {
	in := "ppstmt1 ppstmt987 prepstmt2 stmt3"
	want := "ppstmt<ID> ppstmt<ID> prepstmt2 stmt3"
	if got := string(normalizeTestOutput("prepare", "/patches", []byte(in))); got != want {
		t.Fatalf("normalize prepare names = %q, want %q", got, want)
	}
	if got := string(normalizeTestOutput("boolean", "/patches", []byte(in))); got != in {
		t.Fatalf("unrelated test output changed: %q", got)
	}
}

func TestNormalizePreparedStatementCatalog(t *testing.T) {
	in := "SELECT name, statement FROM pg_prepared_statements\n" +
		"ORDER BY name;\n" +
		"name | statement\n" +
		"-----+----------\n" +
		"q1 | SELECT 1\n" +
		"ppstmt42 | SELECT 1\n" +
		"(2 rows)\n" +
		"SELECT 1;\n"
	want := "SELECT name, statement FROM pg_prepared_statements\n" +
		"ORDER BY name;\n" +
		"<pg_prepared_statements result>\n" +
		"SELECT 1;\n"
	if got := string(normalizeTestOutput("prepare", "/patches", []byte(in))); got != want {
		t.Fatalf("normalize prepared statement catalog = %q, want %q", got, want)
	}
	if got := string(normalizeTestOutput("boolean", "/patches", []byte(in))); got != in {
		t.Fatalf("unrelated catalog output changed: %q", got)
	}
}

func TestNormalizeNotificationPIDs(t *testing.T) {
	in := "listener: NOTIFY \"c1\" with payload \"\" from PID 12345\nAsynchronous notification \"c1\" received from server process with PID 67890.\n"
	want := "listener: NOTIFY \"c1\" with payload \"\" from PostgreSQL backend PID\nAsynchronous notification \"c1\" received from PostgreSQL backend PID.\n"
	got := string(normalizeNotificationPIDs([]byte(in)))
	if got != want {
		t.Errorf("normalizeNotificationPIDs() = %q, want %q", got, want)
	}
}

func TestNormalizeWhitespace(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"single line, no whitespace", "abc", "abc"},
		{"trailing newline preserved", "abc\n", "abc\n"},
		{"trailing whitespace trimmed", "abc   \n", "abc\n"},
		{"leading whitespace trimmed", "   abc\n", "abc\n"},
		{"runs collapsed within line", "a    b\tc\n", "a b c\n"},
		{
			"caret shift becomes equal — BSD/GNU divergence case",
			"   ^\n", "    ^\n",
		}, // both inputs normalize to identical output below
		// Separate cases for the divergence to make the equivalence explicit.
		{"caret with 3 leading spaces", "   ^\n", "^\n"},
		{"caret with 4 leading spaces", "    ^\n", "^\n"},
		{
			"multiline mixed",
			"   line1   \n  a  b  \n   ^\n",
			"line1\na b\n^\n",
		},
		{
			"no trailing newline",
			"   ^",
			"^",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := string(normalizeWhitespace([]byte(tc.in)))
			if tc.name == "caret shift becomes equal — BSD/GNU divergence case" {
				// Special case: assert that two different inputs both
				// collapse to the same canonical form.
				a := string(normalizeWhitespace([]byte("   ^\n")))
				b := string(normalizeWhitespace([]byte("    ^\n")))
				if a != b {
					t.Errorf("caret-shift inputs should normalize equal; a=%q b=%q", a, b)
				}
				return
			}
			if got != tc.want {
				t.Errorf("normalizeWhitespace(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
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

// TestNormalizeRunPaths pins the per-run build-directory masking: psql output
// that embeds the timestamped build path (largeobject's "could not open file"
// lines) must normalize to a stable token, or no committed patch containing
// such a line could ever verify on a later run.
func TestNormalizeRegressionStats(t *testing.T) {
	a := []byte("trunc_stats_test1 | 4 | 2 | 1 | 1 | 0\nWHERE st.relname='tenk2' AND cl.relname='tenk2';\n?column? | ?column?\n----------+----------\nt | t\nSET temp_buffers TO 100;\n:io_sum_local_after_extends > :io_sum_local_before_extends;\n?column? | ?column? | ?column? | ?column?\n----------+----------+----------+----------\nt | t | t | t\n")
	b := []byte("trunc_stats_test1 | 0 | 0 | 0 | 0 | 0\nWHERE st.relname='tenk2' AND cl.relname='tenk2';\n?column? | ?column?\n----------+----------\nf | t\nSET temp_buffers TO 100;\nERROR: invalid value for parameter \"temp_buffers\": 100\nDETAIL: \"temp_buffers\" cannot be changed after any temporary tables have been accessed in the session.\n:io_sum_local_after_extends > :io_sum_local_before_extends;\n?column? | ?column? | ?column? | ?column?\n----------+----------+----------+----------\nf | f | f | t\n")
	if got, want := string(normalizeRegressionStats(a)), string(normalizeRegressionStats(b)); got != want {
		t.Fatalf("backend-local stats did not normalize equally:\n%s\n---\n%s", got, want)
	}
}

func TestNormalizeRunPaths(t *testing.T) {
	in := `could not open file "/tmp/multigres_pg_cache/builds/20260703-091540.026410/build/src/test/regress/results/lotest.txt": No such file or directory`
	want := `could not open file "/tmp/multigres_pg_cache/builds/[RUN]/build/src/test/regress/results/lotest.txt": No such file or directory`
	if got := string(normalizeRunPaths([]byte(in))); got != want {
		t.Fatalf("normalizeRunPaths = %q, want %q", got, want)
	}

	// Lines without a run path pass through untouched.
	if got := string(normalizeRunPaths([]byte("SELECT 1;\n"))); got != "SELECT 1;\n" {
		t.Fatalf("plain line changed: %q", got)
	}
}
