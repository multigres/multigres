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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// pgxQueryService runs the admin-pool statements against a real postgres, so a
// test can execute the SQL the manager issues instead of matching its text.
// Everything else comes from the embedded mock.
type pgxQueryService struct {
	*mock.QueryService
	conn *pgx.Conn
}

func (q *pgxQueryService) run(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	rows, err := q.conn.Query(ctx, sql, append([]any{pgx.QueryExecModeSimpleProtocol}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := &sqltypes.Result{}
	for rows.Next() {
		raw := rows.RawValues()
		row := &sqltypes.Row{Values: make([]sqltypes.Value, len(raw))}
		for i, v := range raw {
			if v != nil {
				row.Values[i] = append(sqltypes.Value{}, v...)
			}
		}
		res.Rows = append(res.Rows, row)
	}
	return res, rows.Err()
}

func (q *pgxQueryService) QueryAdmin(ctx context.Context, sql string) (*sqltypes.Result, error) {
	return q.run(ctx, sql)
}

func (q *pgxQueryService) QueryAdminArgs(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	return q.run(ctx, sql, args...)
}

func (q *pgxQueryService) QueryAdminMultiStatement(ctx context.Context, sql string) error {
	_, err := q.conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	return err
}

// startTempPostgres boots a throwaway cluster on a unix socket. There is no
// shared postgres fixture for in-package manager tests (they use the mock), so
// this skips when initdb is not installed.
func startTempPostgres(t *testing.T) *pgx.Conn {
	t.Helper()
	if testing.Short() {
		t.Skip("real postgres test")
	}
	initdb, err := exec.LookPath("initdb")
	if err != nil {
		t.Skip("initdb not on PATH")
	}
	pgCtl, err := exec.LookPath("pg_ctl")
	if err != nil {
		t.Skip("pg_ctl not on PATH")
	}
	// Unix socket paths are length-limited, so t.TempDir() can be too long.
	sock, err := os.MkdirTemp("/tmp", "pgrepos")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(sock) })
	data := filepath.Join(sock, "data")

	out, err := exec.Command(initdb, "-D", data, "-U", "postgres", "-A", "trust").CombinedOutput()
	require.NoError(t, err, string(out))
	out, err = exec.Command(pgCtl, "-D", data, "-w", "-l", filepath.Join(sock, "pg.log"), "-o", "-c listen_addresses= -c fsync=off -k "+sock, "start").CombinedOutput()
	require.NoError(t, err, string(out))
	t.Cleanup(func() { _ = exec.Command(pgCtl, "-D", data, "-m", "immediate", "stop").Run() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, "host="+sock+" user=postgres dbname=postgres")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// realQueryController routes the manager's internal queries to postgres.
type realQueryController struct {
	*mockPoolerController
	svc executor.InternalQueryService
}

func (c *realQueryController) InternalQueryService() executor.InternalQueryService { return c.svc }

func setupRealSQL(t *testing.T) (*MultipoolerManager, *pgx.Conn) {
	t.Helper()
	conn := startTempPostgres(t)
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	pm.qsc = &realQueryController{mockPoolerController: &mockPoolerController{queryService: qs}, svc: &pgxQueryService{QueryService: qs, conn: conn}}
	pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{
		ShardRef:  &clustermetadatapb.ShardRef{TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard},
		PoolerDir: t.TempDir(),
	})
	_, err := conn.Exec(context.Background(), "CREATE SCHEMA multigres")
	require.NoError(t, err)
	return pm, conn
}

func revisionOf(t *testing.T, conn *pgx.Conn) int64 {
	t.Helper()
	var rev int64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT revision FROM multigres.pgbackrest_repos_meta WHERE id = 1").Scan(&rev))
	return rev
}

func TestPgBackRestRepos_RealSQL(t *testing.T) {
	ctx := context.Background()
	pm, conn := setupRealSQL(t)

	require.NoError(t, pm.createPgBackRestReposTable(ctx))
	require.NoError(t, pm.createPgBackRestReposTable(ctx), "idempotent, and must not reset the revision")
	assert.Equal(t, int64(1), revisionOf(t, conn))

	t.Run("seed bumps by exactly one", func(t *testing.T) {
		require.NoError(t, pm.insertInitialPgBackRestRepo(ctx))
		assert.Equal(t, int64(2), revisionOf(t, conn))
		rev, repos, err := pm.readGenerations(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), rev)
		assert.Equal(t, []RepoGeneration{{Generation: 1, RepoNumber: 1}}, repos)
	})

	t.Run("a failed write does not bump", func(t *testing.T) {
		require.Error(t, pm.insertInitialPgBackRestRepo(ctx), "duplicate generation")
		assert.Equal(t, int64(2), revisionOf(t, conn))
	})

	t.Run("catch-up creates the meta table on a shard that predates it", func(t *testing.T) {
		_, err := conn.Exec(ctx, "DROP TABLE multigres.pgbackrest_repos_meta")
		require.NoError(t, err)
		_, _, err = pm.readGenerations(ctx)
		require.Error(t, err, "readGenerations fails without the meta table")

		require.NoError(t, pm.ensurePgBackRestReposMeta(ctx))
		require.NoError(t, pm.ensurePgBackRestReposMeta(ctx), "idempotent")
		assert.Equal(t, int64(1), revisionOf(t, conn))
		_, repos, err := pm.readGenerations(ctx)
		require.NoError(t, err)
		assert.Len(t, repos, 1, "existing repo rows are untouched")
	})

	t.Run("reconcile", func(t *testing.T) {
		setRev := func(rev int64) {
			_, err := conn.Exec(ctx, "UPDATE multigres.pgbackrest_repos_meta SET revision = "+strconv.FormatInt(rev, 10))
			require.NoError(t, err)
		}
		repoList := func() []RepoGeneration {
			_, repos, err := pm.readGenerations(ctx)
			require.NoError(t, err)
			return repos
		}
		wantRepos := repoList()

		tests := []struct {
			name              string
			table, copy, held int64
			want              int64
		}{
			{name: "table behind copy", table: 3, copy: 10, held: 0, want: 11},
			{name: "table behind sidecar floor", table: 3, copy: 4, held: 20, want: 21},
			{name: "equal", table: 7, copy: 7, held: 7, want: 8},
			{name: "table ahead is never lowered", table: 50, copy: 4, held: 9, want: 51},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				setRev(tt.table)
				require.NoError(t, os.RemoveAll(filepath.Join(pm.record.PoolerDir(), generationsCopyFile)))
				require.NoError(t, pm.writeGenerationsCopy(tt.copy, wantRepos))

				got, err := pm.reconcileRevisionFloor(ctx, tt.held)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
				assert.Equal(t, tt.want, revisionOf(t, conn))
				assert.Equal(t, wantRepos, repoList(), "the table's repo list stays authoritative")
			})
		}
	})
}
