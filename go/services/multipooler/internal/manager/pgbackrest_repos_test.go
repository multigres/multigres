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

package manager

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestInsertInitialPgBackRestRepo(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(m *mock.QueryService)
		errorContains string
	}{
		{
			name: "seeds the generation-1 row",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil))
			},
		},
		{
			name: "insert failure is surfaced",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.pgbackrest_repos", errors.New("insert failed"))
			},
			errorContains: "failed to seed pgbackrest_repos",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
			tt.setupMock(mockQueryService)

			err := pm.insertInitialPgBackRestRepo(context.Background())

			if tt.errorContains != "" {
				require.ErrorContains(t, err, tt.errorContains)
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// TestPgBackRestRepos_NoWriterSkipsTheBump scans the package source: every SQL
// expression that writes multigres.pgbackrest_repos must also carry the
// revision bump, so a future writer cannot skip it unnoticed. The real-SQL
// behaviour is covered by TestPgBackRestRepos_RealSQL.
func TestPgBackRestRepos_NoWriterSkipsTheBump(t *testing.T) {
	write := regexp.MustCompile(`(?is)(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+multigres\.pgbackrest_repos\b`)
	fset := token.NewFileSet()
	names, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var files []*ast.File
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		files = append(files, f)
	}

	found := 0
	{
		for _, f := range files {
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				for _, arg := range call.Args {
					var lits []string
					var idents []string
					ast.Inspect(arg, func(m ast.Node) bool {
						switch v := m.(type) {
						case *ast.BasicLit:
							if v.Kind == token.STRING {
								if s, err := strconv.Unquote(v.Value); err == nil {
									lits = append(lits, s)
								}
							}
						case *ast.Ident:
							idents = append(idents, v.Name)
						}
						return true
					})
					joined := strings.Join(lits, "")
					if !write.MatchString(joined) {
						continue
					}
					found++
					bumped := slices.Contains(idents, "bumpPgBackRestReposRevisionSQL") ||
						strings.Contains(joined, "UPDATE multigres.pgbackrest_repos_meta SET revision = revision + 1")
					assert.True(t, bumped, "%s writes pgbackrest_repos without bumping the revision", fset.Position(call.Pos()))
				}
				return true
			})
		}
	}
	assert.Positive(t, found, "the scan found no writer; the pattern is stale")
}

// TestPgBackRestRepos_EveryWriterBumpsRevision covers every function that
// writes multigres.pgbackrest_repos (found with grep -n pgbackrest_repos over
// go/services/multipooler): insertInitialPgBackRestRepo is the only one. The
// bump rides in the same statement, hence the same transaction; the mock
// cannot model the table, so this is string-level; the real-SQL counterpart is
// TestPgBackRestRepos_RealSQL, which needs initdb and skips without it.
func TestPgBackRestRepos_EveryWriterBumpsRevision(t *testing.T) {
	writers := map[string]func(pm *MultipoolerManager) error{
		"insertInitialPgBackRestRepo": func(pm *MultipoolerManager) error {
			return pm.insertInitialPgBackRestRepo(context.Background())
		},
	}
	for name, write := range writers {
		t.Run(name, func(t *testing.T) {
			pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
			var stmts []string
			qs.AddQueryPatternWithCallback("pgbackrest_repos", mock.MakeQueryResult(nil, nil), func(q string) { stmts = append(stmts, q) })

			require.NoError(t, write(pm))

			require.Len(t, stmts, 1, "the write and its bump must be one statement")
			assert.Equal(t, 1, strings.Count(stmts[0], "UPDATE multigres.pgbackrest_repos_meta SET revision = revision + 1 WHERE id = 1"))
			assert.Contains(t, stmts[0], "INSERT INTO multigres.pgbackrest_repos ")
		})
	}
}

func TestPgBackRestRepos_CreateSeedsMeta(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	var stmt string
	qs.AddQueryPatternWithCallback("CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos", mock.MakeQueryResult(nil, nil), func(q string) { stmt = q })

	require.NoError(t, pm.createPgBackRestReposTable(context.Background()))

	assert.Contains(t, stmt, "CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos_meta")
	assert.Contains(t, stmt, "INSERT INTO multigres.pgbackrest_repos_meta (id, revision) VALUES (1, 1) ON CONFLICT (id) DO NOTHING")
}

func TestRequireInitialRepoEncryptionError(t *testing.T) {
	required := &clustermetadatapb.BackupLocation{RequireInitialRepoEncryption: true}
	optional := &clustermetadatapb.BackupLocation{}

	assert.NoError(t, requireInitialRepoEncryptionError(optional, nil))
	assert.NoError(t, requireInitialRepoEncryptionError(required, backup.CipherKeys{1: "pass"}))
	assert.ErrorContains(t, requireInitialRepoEncryptionError(required, nil), "no cipher key for generation 1")
	assert.ErrorContains(t, requireInitialRepoEncryptionError(required, backup.CipherKeys{1: ""}), "declared unencrypted")
}

func TestPgBackRestRepos_EnsureSidecarSchemasCreatesMeta(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, "commerce", "0-80")
	var stmts []string
	record := func(q string) { stmts = append(stmts, q) }
	addShardRefEnsure(qs, record)
	qs.AddQueryPatternWithCallback(".*", mock.MakeQueryResult(nil, nil), record)

	require.NoError(t, pm.ensureSidecarSchemas(context.Background()))

	i := slices.IndexFunc(stmts, func(q string) bool { return strings.Contains(q, "multigres.pgbackrest_repos_meta") })
	require.GreaterOrEqual(t, i, 0, "the version-skew catch-up must provision the revision table")
	assert.Contains(t, stmts[i], "IF NOT EXISTS")
	assert.Contains(t, stmts[i], "ON CONFLICT (id) DO NOTHING")
}
