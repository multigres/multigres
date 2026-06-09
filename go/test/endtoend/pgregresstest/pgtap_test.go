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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePgTAPOutput(t *testing.T) {
	tests := []struct {
		name        string
		output      string
		wantPlanned int
		wantPassed  int
		wantFailed  []string
	}{
		{
			name: "all pass with plan",
			output: "1..3\n" +
				"ok 1 - table partman_test.foo exists\n" +
				"ok 2 - column id is pk\n" +
				"ok 3 - has 4 partitions\n",
			wantPlanned: 3,
			wantPassed:  3,
			wantFailed:  nil,
		},
		{
			name: "some fail",
			output: "1..3\n" +
				"ok 1 - first\n" +
				"not ok 2 - second should match\n" +
				"ok 3 - third\n",
			wantPlanned: 3,
			wantPassed:  2,
			wantFailed:  []string{"2 - second should match"},
		},
		{
			name: "not ok never miscounted as ok",
			output: "1..1\n" +
				"not ok 1 - boom\n",
			wantPlanned: 1,
			wantPassed:  0,
			wantFailed:  []string{"1 - boom"},
		},
		{
			name: "diagnostics and command tags ignored",
			output: "BEGIN\n" +
				"CREATE SCHEMA\n" +
				"1..2\n" +
				"ok 1 - one\n" +
				"# a diagnostic line about a failure\n" +
				"not ok 2 - two\n" +
				"# Looks like you failed 1 test of 2\n",
			wantPlanned: 2,
			wantPassed:  1,
			wantFailed:  []string{"2 - two"},
		},
		{
			name:        "no plan line",
			output:      "ERROR:  relation does not exist\n",
			wantPlanned: -1,
			wantPassed:  0,
			wantFailed:  nil,
		},
		{
			name:        "plan present but no assertions ran (plan mismatch)",
			output:      "1..5\n",
			wantPlanned: 5,
			wantPassed:  0,
			wantFailed:  nil,
		},
		{
			name: "carriage returns trimmed",
			output: "1..1\r\n" +
				"ok 1 - crlf\r\n",
			wantPlanned: 1,
			wantPassed:  1,
			wantFailed:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			planned, passed, failed := parsePgTAPOutput(tc.output)
			assert.Equal(t, tc.wantPlanned, planned, "planned")
			assert.Equal(t, tc.wantPassed, passed, "passed")
			assert.Equal(t, tc.wantFailed, failed, "failed")
		})
	}
}

func TestSummarizePgTAPFailures(t *testing.T) {
	t.Run("under cap lists all", func(t *testing.T) {
		got := summarizePgTAPFailures([]string{"a", "b"})
		assert.Equal(t, "2 assertion(s) failed: a | b", got)
	})
	t.Run("over cap truncates with count", func(t *testing.T) {
		got := summarizePgTAPFailures([]string{"a", "b", "c", "d", "e", "f", "g"})
		assert.Equal(t, "7 assertion(s) failed: a | b | c | d | e … (+2 more)", got)
	})
}

// TestExternalBuildList_IncludesPgTAPDependency asserts that pg_partman pulls in
// its pgtap build dependency, dependencies-first, when selected via PGEXTERNAL_TESTS.
func TestExternalBuildList_IncludesPgTAPDependency(t *testing.T) {
	t.Setenv("PGEXTERNAL_TESTS", "pg_partman")

	specs := ExternalBuildList()
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name
	}
	require.Equal(t, []string{"pgtap", "pg_partman"}, names,
		"pgtap must be installed before pg_partman")
}

// TestSelectPgTAPFiles covers glob-union, subfolder patterns, exclude filtering,
// and the default glob, against a temp tree mirroring pg_partman's layout.
func TestSelectPgTAPFiles(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"test-a.sql", "test-b.sql", "helper.txt",
		"test_pg17plus/keep.sql", "test_pg17plus/drop-me.sql",
	}
	for _, f := range files {
		p := filepath.Join(dir, f)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte("-- test"), 0o644))
	}
	rel := func(paths []string) []string {
		out := make([]string, len(paths))
		for i, p := range paths {
			r, _ := filepath.Rel(dir, p)
			out[i] = r
		}
		return out
	}

	t.Run("globs union, excludes applied, non-.sql ignored", func(t *testing.T) {
		got, err := selectPgTAPFiles(dir,
			[]string{"test-*.sql", "test_pg17plus/*.sql"},
			[]string{"test_pg17plus/drop-me.sql"})
		require.NoError(t, err)
		assert.Equal(t, []string{"test-a.sql", "test-b.sql", "test_pg17plus/keep.sql"}, rel(got))
	})

	t.Run("default glob when none given", func(t *testing.T) {
		got, err := selectPgTAPFiles(dir, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"test-a.sql", "test-b.sql"}, rel(got)) // top-level *.sql only
	})
}

// TestPgPartmanSpec_IsPgTAP guards the wiring that selects the pgTAP harness and
// the dependency/preload setup pg_partman needs.
func TestPgPartmanSpec_IsPgTAP(t *testing.T) {
	spec, ok := externalSpecs["pg_partman"]
	require.True(t, ok, "pg_partman must have a build spec")
	assert.Equal(t, HarnessPgTAP, spec.Harness)
	assert.Equal(t, []string{"test-*.sql", "test_pg17plus/*.sql", "test_no_search_path/*.sql"}, spec.TestGlobs)
	assert.Equal(t, []string{"pgtap"}, spec.DependsOn)
	assert.Equal(t, []ExtensionInstall{{Name: "pgtap"}, {Name: "pg_partman", Schema: "partman"}}, spec.PreCreateExtensions)
	assert.Equal(t, []string{"test_pg17plus/test-time-monthly-source-generated.sql"}, spec.ExcludeGlobs)
	assert.Equal(t, "pg_partman.conf", spec.ServerConfigFile)

	// pg_partman must be marked covered so the suite actually runs it.
	var covered bool
	for _, e := range ExtensionCatalog {
		if e.Name == "pg_partman" {
			covered = e.Status == StatusCovered
		}
	}
	assert.True(t, covered, "pg_partman must be StatusCovered in the catalog")
}
