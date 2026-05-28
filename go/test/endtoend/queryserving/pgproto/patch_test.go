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
	"testing"
)

func TestSplitPatchComment(t *testing.T) {
	t.Run("with comment preamble", func(t *testing.T) {
		in := "# why this diverges\n#\n# details\n--- a\n+++ b\n@@ -1 +1 @@\n-x\n+y\n"
		comment, body := splitPatchComment([]byte(in))
		if string(comment) != "# why this diverges\n#\n# details\n" {
			t.Errorf("comment = %q", comment)
		}
		if string(body) != "--- a\n+++ b\n@@ -1 +1 @@\n-x\n+y\n" {
			t.Errorf("body = %q", body)
		}
	})

	t.Run("no comment (starts with diff header)", func(t *testing.T) {
		in := "--- a\n+++ b\n@@ -1 +1 @@\n-x\n+y\n"
		comment, body := splitPatchComment([]byte(in))
		if len(comment) != 0 {
			t.Errorf("expected empty comment, got %q", comment)
		}
		if string(body) != in {
			t.Errorf("body = %q", body)
		}
	})
}

// TestApplyPatchWithComment proves a comment preamble is ignored when applying:
// generate a real diff, prepend a comment, and confirm applyPatch reproduces
// the target. Exercises the system `patch`/`diff` utilities (no cluster needed).
func TestApplyPatchWithComment(t *testing.T) {
	ctx := context.Background()
	original := []byte("FE=> Sync\n<= BE ParseComplete\n<= BE ErrorResponse(C 22012)\n")
	target := []byte("FE=> Sync\n<= BE ParseComplete\n<= BE BindComplete\n<= BE ErrorResponse(C 22012)\n")

	body, err := generateDiff(ctx, original, target)
	if err != nil {
		t.Fatalf("generateDiff: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("expected a non-empty diff")
	}

	patchPath := filepath.Join(t.TempDir(), "x.patch")
	commented := append([]byte("# explanatory comment\n# second line\n"), body...)
	if err := os.WriteFile(patchPath, commented, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := applyPatch(ctx, original, patchPath)
	if err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if string(got) != string(target) {
		t.Errorf("applyPatch result mismatch\n got: %q\nwant: %q", got, target)
	}
}
