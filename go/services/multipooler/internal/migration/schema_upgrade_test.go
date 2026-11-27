// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Compare each independently appendable list in execution order. The rewrite
// path is deliberately below the prefix check: it may record new entries only.
func TestMigrationSchemaGolden(t *testing.T) {
	const path = "testdata/migration_schema.golden"
	const boundary = "\n-- catalog shard only\n"
	const sep = ";\n\n"
	golden, err := os.ReadFile(path)
	require.NoError(t, err)
	sql := SchemaSQL()
	split := len(sql) - len(schemaConnectionsSQL)
	groups := strings.Split(strings.TrimSuffix(string(golden), ";\n"), boundary)
	require.Len(t, groups, 2)
	const advice = "Leave existing statements unchanged and append an idempotent upgrade (e.g. ALTER TABLE ... ADD COLUMN IF NOT EXISTS)."
	for i, stmts := range [][]string{sql[:split], sql[split:]} {
		for j, prior := range strings.Split(groups[i], sep) {
			if j >= len(stmts) || prior != stmts[j] {
				t.Fatalf("schema statement %d:%d edited or removed. %s\n%s", i, j, advice, prior)
			}
		}
	}
	// An independent append-only list of fingerprints pins every entry, even if
	// the SQL golden is hand-edited. Check both prefixes before writing either file.
	const pinsPath = "testdata/migration_schema.sha256"
	data, err := os.ReadFile(pinsPath)
	require.NoError(t, err)
	pinned := strings.Split(strings.TrimSuffix(string(data), "\n"), boundary)
	require.Len(t, pinned, 2)
	var next [2]string
	for i, stmts := range [][]string{sql[:split], sql[split:]} {
		old := strings.Split(pinned[i], "\n")
		require.GreaterOrEqual(t, len(stmts), len(old), advice)
		var hashes []string
		for j, stmt := range stmts {
			h := fmt.Sprintf("%x", sha256.Sum256([]byte(stmt)))
			if j < len(old) {
				require.Equal(t, old[j], h, "historical schema statement %d:%d changed. %s", i, j, advice)
			}
			hashes = append(hashes, h)
		}
		next[i] = strings.Join(hashes, "\n")
	}
	want := strings.Join(sql[:split], sep) + boundary + strings.Join(sql[split:], sep) + ";\n"
	wantPins := next[0] + boundary + next[1] + "\n"
	if os.Getenv("MIGRATION_SCHEMA_REWRITE") == "1" {
		require.NoError(t, os.WriteFile(path, []byte(want), 0o644))
		require.NoError(t, os.WriteFile(pinsPath, []byte(wantPins), 0o644))
		return
	}
	require.Equal(t, string(golden), want, "Record appended statements with MIGRATION_SCHEMA_REWRITE=1.")
	require.Equal(t, string(data), wantPins, "Pin appended statements with MIGRATION_SCHEMA_REWRITE=1.")
}
