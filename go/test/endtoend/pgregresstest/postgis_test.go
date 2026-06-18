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

	"github.com/stretchr/testify/require"
)

func TestParsePostGISResults(t *testing.T) {
	out := `
 PostgreSQL 17.6

Running tests

 regress/core/affine .. ok in 12 ms
 topology/test/regress/addedge . failed (actual output differs: /tmp/test_2_diff)
 raster/test/regress/check_gdal . skipped (can't read any raster/test/regress/check_gdal.{sql,dbf,tif,dmp})

Run tests: 3
Failed: 1
`

	res, err := parsePostGISResults(out)
	require.NoError(t, err)
	require.Equal(t, 3, res.TotalTests)
	require.Equal(t, 1, res.PassedTests)
	require.Equal(t, 1, res.FailedTests)
	require.Equal(t, 1, res.SkippedTests)
	require.Equal(t, "postgis/regress/core/affine", res.Tests[0].Name)
	require.Equal(t, "pass", res.Tests[0].Status)
	require.Equal(t, "12ms", res.Tests[0].Duration)
	require.Equal(t, "postgis_topology/topology/test/regress/addedge", res.Tests[1].Name)
	require.Equal(t, "fail", res.Tests[1].Status)
	require.Equal(t, "postgis_raster/raster/test/regress/check_gdal", res.Tests[2].Name)
	require.Equal(t, "skip", res.Tests[2].Status)
}

func TestParsePostGISMakeDryRunTests(t *testing.T) {
	dir := t.TempDir()
	writeExpected := func(name string) {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+"_expected"), []byte("ok\n"), 0o644))
	}
	writeExpected("regress/core/affine")
	writeExpected("regress/core/binary")
	writeExpected("regress/core/clean")

	out := `make -C regress -n check
perl ./run_test.pl --nocreate --before-upgrade-script regress/hooks/hook-before-upgrade.sql core/affine \
  "core/binary" --after-create-script regress/hooks/hook-after-create.sql core/clean
`

	tests := parsePostGISMakeDryRunTests(dir, "regress", out)
	require.Equal(t, []string{
		"regress/core/affine",
		"regress/core/binary",
		"regress/core/clean",
	}, tests)
}
