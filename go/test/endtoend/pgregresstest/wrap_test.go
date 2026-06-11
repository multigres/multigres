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
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWrapLines_Simple pins the basic shape: ON_ERROR_ROLLBACK + BEGIN at the
// top, COMMIT at the end, everything else untouched.
func TestWrapLines_Simple(t *testing.T) {
	got := wrapLines([]string{
		"CREATE TABLE t (a int);",
		"SELECT 1;",
	}, nil, nil, false)
	assert.Equal(t, []string{
		`\set ON_ERROR_ROLLBACK on`,
		"BEGIN;",
		"CREATE TABLE t (a int);",
		"SELECT 1;",
		"COMMIT;",
	}, got)
}

// TestWrapLines_VacuumBreak: VACUUM cannot run inside a transaction block, so
// the wrap commits, lets it run autocommit, and reopens — injecting the reopen
// setup (hypopg's reset) whose expected output appears only on the expected
// side.
func TestWrapLines_VacuumBreak(t *testing.T) {
	setup := []WrapStatement{{SQL: "SELECT hypopg_reset();", Output: " hypopg_reset \n--------------\n \n(1 row)\n\n"}}
	lines := []string{
		"SELECT 1;",
		"VACUUM ANALYZE hypo;",
		"SELECT 2;",
	}

	gotSQL := wrapLines(lines, setup, nil, false)
	assert.Equal(t, []string{
		`\set ON_ERROR_ROLLBACK on`,
		"BEGIN;",
		"SELECT 1;",
		"COMMIT;",
		"VACUUM ANALYZE hypo;",
		"BEGIN;",
		"SELECT hypopg_reset();",
		"SELECT 2;",
		"COMMIT;",
	}, gotSQL)

	gotExpected := wrapLines(lines, setup, nil, true)
	assert.Equal(t, []string{
		`\set ON_ERROR_ROLLBACK on`,
		"BEGIN;",
		"SELECT 1;",
		"COMMIT;",
		"VACUUM ANALYZE hypo;",
		"BEGIN;",
		"SELECT hypopg_reset();",
		" hypopg_reset ",
		"--------------",
		" ",
		"(1 row)",
		"",
		"SELECT 2;",
		"COMMIT;",
	}, gotExpected)
}

// TestWrapLines_ConnectBreak: \connect starts a new session, so the wrap
// closes before it and reopens after it.
func TestWrapLines_ConnectBreak(t *testing.T) {
	got := wrapLines([]string{
		"SELECT 1;",
		`\connect - regress_user1`,
		"SELECT 2;",
	}, nil, nil, false)
	assert.Equal(t, []string{
		`\set ON_ERROR_ROLLBACK on`,
		"BEGIN;",
		"SELECT 1;",
		"COMMIT;",
		`\connect - regress_user1`,
		"BEGIN;",
		"SELECT 2;",
		"COMMIT;",
	}, got)
}

// TestWrapLines_UserTransactionPassthrough: the file's own BEGIN/COMMIT block
// is passed through untouched — the wrap closes before it and reopens after,
// so transactions never nest.
func TestWrapLines_UserTransactionPassthrough(t *testing.T) {
	got := wrapLines([]string{
		"SELECT 1;",
		"BEGIN;",
		"INSERT INTO t VALUES (1);",
		"COMMIT;",
		"SELECT 2;",
	}, nil, nil, false)
	assert.Equal(t, []string{
		`\set ON_ERROR_ROLLBACK on`,
		"BEGIN;",
		"SELECT 1;",
		"COMMIT;",
		"BEGIN;",
		"INSERT INTO t VALUES (1);",
		"COMMIT;",
		"BEGIN;",
		"SELECT 2;",
		"COMMIT;",
	}, got)
}

// TestWrapLines_DollarQuoteSuppression: BEGIN/COMMIT/VACUUM keywords inside
// dollar-quoted bodies (plpgsql, DO blocks) must not register as boundaries.
func TestWrapLines_DollarQuoteSuppression(t *testing.T) {
	got := wrapLines([]string{
		"DO $$",
		"BEGIN;",
		"VACUUM t;",
		"COMMIT;",
		"$$;",
	}, nil, nil, false)
	assert.Equal(t, []string{
		`\set ON_ERROR_ROLLBACK on`,
		"BEGIN;",
		"DO $$",
		"BEGIN;",
		"VACUUM t;",
		"COMMIT;",
		"$$;",
		"COMMIT;",
	}, got)
}

// TestWrapLines_UnterminatedUserTransaction: a file whose own transaction is
// still open at EOF (pgaudit relies on session teardown) must not get a
// trailing COMMIT from the wrap.
func TestWrapLines_UnterminatedUserTransaction(t *testing.T) {
	got := wrapLines([]string{
		"BEGIN;",
		"SELECT 1;",
	}, nil, nil, false)
	assert.Equal(t, []string{
		`\set ON_ERROR_ROLLBACK on`,
		"BEGIN;",
		"COMMIT;",
		"BEGIN;",
		"SELECT 1;",
	}, got)
}

// TestWrapLines_StopPattern: from the first stop-pattern match the wrap closes
// permanently and the rest of the file runs autocommit (http's
// statement_timeout cancellation tail).
func TestWrapLines_StopPattern(t *testing.T) {
	stop := []*regexp.Regexp{regexp.MustCompile(`^SET statement_timeout`)}
	got := wrapLines([]string{
		"SELECT 1;",
		"SET statement_timeout = 200;",
		"CREATE TEMPORARY TABLE timer AS SELECT now() AS start;",
		"SELECT 2;",
	}, nil, stop, false)
	assert.Equal(t, []string{
		`\set ON_ERROR_ROLLBACK on`,
		"BEGIN;",
		"SELECT 1;",
		"COMMIT;",
		"SET statement_timeout = 200;",
		"CREATE TEMPORARY TABLE timer AS SELECT now() AS start;",
		"SELECT 2;",
	}, got)
}

// TestTransformSuiteFile_TextRewrites: rewrites apply to the materialized copy
// (both sides use the same function, so sql and expected change identically).
func TestTransformSuiteFile_TextRewrites(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.sql")
	require.NoError(t, os.WriteFile(src, []byte("SELECT http_get('https://postgis.net');\n"), 0o644))

	ext := ExternalExtension{
		TextRewrites: []TextRewrite{{Old: "https://postgis.net", New: "https://127.0.0.1:9443"}},
	}
	dst := filepath.Join(dir, "out.sql")
	require.NoError(t, transformSuiteFile(ext, src, dst, false))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, "SELECT http_get('https://127.0.0.1:9443');\n", string(got))
}

// TestMaterializeTransformedSuite wires the pieces: sql/ and every expected
// variant are transformed into the destination layout pg_regress consumes.
func TestMaterializeTransformedSuite(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(src, "sql"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(src, "expected"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sql", "a.sql"), []byte("SELECT 1;\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "expected", "a.out"), []byte("SELECT 1;\n 1\n(1 row)\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "expected", "a_1.out"), []byte("SELECT 1;\n 1\n(1 row)\n"), 0o644))

	dest := filepath.Join(t.TempDir(), "wrapped")
	got, err := materializeTransformedSuite(ExternalExtension{WrapTransactions: true}, src, src, []string{"a"}, dest)
	require.NoError(t, err)
	assert.Equal(t, dest, got)

	sql, err := os.ReadFile(filepath.Join(dest, "sql", "a.sql"))
	require.NoError(t, err)
	assert.Equal(t, "\\set ON_ERROR_ROLLBACK on\nBEGIN;\nSELECT 1;\nCOMMIT;\n", string(sql))

	for _, name := range []string{"a.out", "a_1.out"} {
		out, err := os.ReadFile(filepath.Join(dest, "expected", name))
		require.NoError(t, err)
		assert.Equal(t, "\\set ON_ERROR_ROLLBACK on\nBEGIN;\nSELECT 1;\n 1\n(1 row)\nCOMMIT;\n", string(out), name)
	}
}

// TestWritePgPassFile pins the .pgpass shape and the 0600 mode libpq requires.
func TestWritePgPassFile(t *testing.T) {
	path, err := writePgPassFile("adminpw", []PgPassUser{
		{Name: "regress_user1", Password: "password"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(path)) })

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "localhost:*:*:postgres:adminpw\nlocalhost:*:*:regress_user1:password\n", string(data))
}
