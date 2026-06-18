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
	"os"
	"path/filepath"
	"testing"
)

func TestGlobToRegexp(t *testing.T) {
	cases := []struct {
		pat   string
		input string
		want  bool
	}{
		{"**/*.pgproto", "a/b/c.pgproto", true},
		{"**/*.pgproto", "c.pgproto", true},
		{"**/*.pgproto", "c.txt", false},
		{"test/**/*.test", "test/x/y.test", true},
		{"test/**/*.test", "test/y.test", true}, // `**/` collapses to zero segments
		{"test/**/*.test", "other/y.test", false},
		{"*.test", "a.test", true},
		{"*.test", "a/b.test", false}, // `*` does not cross a separator
		{"a?c.test", "abc.test", true},
		{"a?c.test", "a/c.test", false}, // `?` does not match `/`
		{"file.test", "file.test", true},
		{"file.test", "file_test", false}, // `.` stays literal
	}
	for _, tc := range cases {
		re, err := GlobToRegexp(tc.pat)
		if err != nil {
			t.Fatalf("GlobToRegexp(%q): %v", tc.pat, err)
		}
		if got := re.MatchString(tc.input); got != tc.want {
			t.Errorf("glob %q match %q = %v, want %v", tc.pat, tc.input, got, tc.want)
		}
	}
}

func TestListCorpusFiles(t *testing.T) {
	dir := t.TempDir()
	for _, rel := range []string{"a.pgproto", "sub/b.pgproto", "sub/deep/c.pgproto", "ignore.txt"} {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("default glob, sorted, absolute", func(t *testing.T) {
		files, err := ListCorpusFiles(dir, "SUITEUTIL_TEST_GLOB", "**/*.pgproto")
		if err != nil {
			t.Fatal(err)
		}
		want := []string{
			filepath.Join(dir, "a.pgproto"),
			filepath.Join(dir, "sub/b.pgproto"),
			filepath.Join(dir, "sub/deep/c.pgproto"),
		}
		if len(files) != len(want) {
			t.Fatalf("got %d files %v, want %d", len(files), files, len(want))
		}
		for i := range want {
			if files[i] != want[i] {
				t.Errorf("files[%d] = %q, want %q", i, files[i], want[i])
			}
		}
	})

	t.Run("env override scopes the run", func(t *testing.T) {
		t.Setenv("SUITEUTIL_TEST_GLOB", "sub/*.pgproto")
		files, err := ListCorpusFiles(dir, "SUITEUTIL_TEST_GLOB", "**/*.pgproto")
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 1 || files[0] != filepath.Join(dir, "sub/b.pgproto") {
			t.Errorf("env override = %v, want [sub/b.pgproto]", files)
		}
	})
}
