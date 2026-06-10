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

// TestExternalSpecs_Coherent guards the catalog/spec invariants so a config
// edit can't silently break the external suite: every covered external
// extension has a build spec, every spec pins exactly one of Tag/Commit, and
// every DependsOn entry resolves to a spec.
func TestExternalSpecs_Coherent(t *testing.T) {
	require.Empty(t, CheckExternalSpecs(),
		"every covered external extension needs an externalSpecs entry")

	for name, spec := range externalSpecs {
		assert.Equal(t, name, spec.Name, "spec key and Name must match")
		assert.NotEmpty(t, spec.Repo, "%s: Repo required", name)
		assert.True(t, (spec.Tag == "") != (spec.Commit == ""),
			"%s: exactly one of Tag and Commit must be set (tag=%q commit=%q)",
			name, spec.Tag, spec.Commit)
		for _, dep := range spec.DependsOn {
			_, ok := externalSpecs[dep]
			assert.True(t, ok, "%s: DependsOn %q has no externalSpecs entry", name, dep)
		}
	}
}

// TestExternalBuildList_OrdersHypopgBeforeIndexAdvisor mirrors the pgtap/
// pg_partman ordering test for the hypopg → index_advisor dependency.
func TestExternalBuildList_OrdersHypopgBeforeIndexAdvisor(t *testing.T) {
	t.Setenv("PGEXTERNAL_TESTS", "index_advisor")

	specs := ExternalBuildList()
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name
	}
	require.Equal(t, []string{"hypopg", "index_advisor"}, names,
		"hypopg must be installed before index_advisor")
}

// TestExternalPreloadLibraries_MergesSelection verifies the preload union
// honors PGEXTERNAL_TESTS and merges across extensions, since the generated
// shared_preload_libraries snippet is built from it.
func TestExternalPreloadLibraries_MergesSelection(t *testing.T) {
	t.Setenv("PGEXTERNAL_TESTS", "pg_cron plpgsql_check")
	assert.Equal(t, []string{"pg_cron", "plpgsql_check"}, ExternalPreloadLibraries())

	t.Setenv("PGEXTERNAL_TESTS", "vector")
	assert.Empty(t, ExternalPreloadLibraries(),
		"a selection with no preload needs must generate no snippet")
}

// TestListRegressTests covers the three selection modes of the pg_regress
// external path: the derived sql/*.sql wildcard, ExcludeGlobs filtering
// (pgtap's prepared-statement fixture files), and the explicit RegressTests
// override (plpgsql_check's per-major-version REGRESS list).
func TestListRegressTests(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sql"), 0o755))
	for _, f := range []string{"alpha.sql", "beta.sql", "gamma.sql"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "sql", f), []byte("-- t"), 0o644))
	}

	t.Run("derived wildcard, sorted", func(t *testing.T) {
		got := listRegressTests(ExternalExtension{}, dir)
		assert.Equal(t, []string{"alpha", "beta", "gamma"}, got)
	})

	t.Run("exclude globs filter the wildcard", func(t *testing.T) {
		got := listRegressTests(ExternalExtension{
			ExcludeGlobs: []string{"sql/beta.sql"},
		}, dir)
		assert.Equal(t, []string{"alpha", "gamma"}, got)
	})

	t.Run("explicit RegressTests wins, in REGRESS order", func(t *testing.T) {
		got := listRegressTests(ExternalExtension{
			RegressTests: []string{"gamma", "alpha"},
			ExcludeGlobs: []string{"sql/gamma.sql"}, // ignored for explicit lists
		}, dir)
		assert.Equal(t, []string{"gamma", "alpha"}, got)
	})
}

// TestPgtapSpec_ExcludesPreparedStatementFixtures guards pgtap's exclusion of
// the files whose subject is SQL-level prepared statements, which multigateway
// owns by design (see the pgtap spec in extensions.go).
func TestPgtapSpec_ExcludesPreparedStatementFixtures(t *testing.T) {
	spec, ok := externalSpecs["pgtap"]
	require.True(t, ok, "pgtap must have a build spec")
	assert.Equal(t, "test", spec.TestSubdir)
	assert.Equal(t, []ExtensionInstall{{Name: "pgtap"}}, spec.PreCreateExtensions)
	assert.Equal(t, []string{"citext", "isn", "ltree"}, spec.ContribDeps)
	assert.Equal(t, []string{
		"sql/performs_ok.sql",
		"sql/performs_within.sql",
		"sql/resultset.sql",
		"sql/valueset.sql",
	}, spec.ExcludeGlobs)
}
