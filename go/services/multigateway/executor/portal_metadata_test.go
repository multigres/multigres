// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	querypb "github.com/multigres/multigres/go/pb/query"
)

func TestPortalMetadataPreservesDatabaseAndEpoch(t *testing.T) {
	exec := newTestExecutor(&mockExec{})
	defer exec.planCache.Close()
	ctx := context.Background()
	portal := makePortalInfo(t, "select id from users where id = $1")
	originalAST := portal.AstStmt().SqlString()
	firstDB, secondDB := testConnWithDB("first"), testConnWithDB("second")

	firstPlan, hit, sql, fp, err := exec.resolvePortalPlan(ctx, portal, firstDB, nil)
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, originalAST, sql)
	require.Equal(t, ast.FingerprintSQL(sql), fp)
	require.Eventually(t, func() bool {
		_, ok := exec.planCache.Get(ctx, buildCacheKey("first", sql))
		return ok
	}, time.Second, time.Millisecond)
	cached, hit, sql2, fp2, err := exec.resolvePortalPlan(ctx, portal, firstDB, nil)
	require.NoError(t, err)
	require.True(t, hit)
	require.Same(t, firstPlan, cached)
	require.Equal(t, sql, sql2)
	require.Equal(t, fp, fp2)

	// The same shared metadata must not bring a plan from another database.
	otherPlan, hit, sql2, fp2, err := exec.resolvePortalPlan(ctx, portal, secondDB, nil)
	require.NoError(t, err)
	require.False(t, hit)
	require.NotSame(t, firstPlan, otherPlan)
	require.Equal(t, sql, sql2)
	require.Equal(t, fp, fp2)
	require.Eventually(t, func() bool {
		_, ok := exec.planCache.Get(ctx, buildCacheKey("second", sql))
		return ok
	}, time.Second, time.Millisecond)

	exec.planCache.Invalidate()
	for _, conn := range []*server.Conn{firstDB, secondDB} {
		fresh, hit, freshSQL, freshFP, err := exec.resolvePortalPlan(ctx, portal, conn, nil)
		require.NoError(t, err)
		require.False(t, hit, "cached metadata must not bypass epoch invalidation")
		require.NotSame(t, firstPlan, fresh)
		require.NotSame(t, otherPlan, fresh)
		require.Equal(t, sql, freshSQL)
		require.Equal(t, fp, freshFP)
	}
	require.Equal(t, originalAST, portal.AstStmt().SqlString(), "planning must leave the shared AST immutable")
}

func TestPortalMetadataConcurrentExecution(t *testing.T) {
	mock := &mockExec{}
	exec := newTestExecutor(mock)
	defer exec.planCache.Close()
	c := preparedstatement.NewConsolidator()
	const query = "select id from users where id = $1"
	psi, err := c.AddPreparedStatement(1, "one", query, []uint32{23})
	require.NoError(t, err)
	shared, err := c.AddPreparedStatement(2, "two", query, []uint32{23})
	require.NoError(t, err)
	require.Same(t, psi, shared)

	// Race concurrent first metadata initialization through the production
	// executor and route. Connections/portals stay local to each client.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			conn := testConnWithDB("shared")
			portal := preparedstatement.NewPortalInfo(shared, &querypb.Portal{})
			<-start
			for range 10 {
				res, err := exec.PortalStreamExecute(context.Background(), conn, nil, portal, 0, false, noopCallback)
				if err != nil {
					t.Errorf("execute: %v", err)
					return
				}
				if res.NormalizedSQL != "SELECT id FROM users WHERE id = $1" {
					t.Errorf("unexpected canonical SQL: %q", res.NormalizedSQL)
				}
			}
		})
	}
	close(start)
	wg.Wait()
	require.Equal(t, int32(160), mock.portalStreamExecuteCalls.Load())
	require.Equal(t, query, mock.lastPortalStreamExecuteQS.Load(), "canonical route must preserve original backend SQL")
}

func TestPortalMetadataKeepsUncachedPaths(t *testing.T) {
	exec := newTestExecutor(&mockExec{})
	defer exec.planCache.Close()
	for _, query := range []string{"BEGIN", "SHOW application_name"} {
		portal := makePortalInfo(t, query)
		for range 2 {
			_, hit, sql, fp, err := exec.resolvePortalPlan(context.Background(), portal, testConn(), nil)
			require.NoError(t, err)
			require.False(t, hit)
			require.Empty(t, sql)
			require.Empty(t, fp)
		}
	}
	// Even precomputed metadata must not bypass unsafe-connection isolation.
	portal := makePortalInfo(t, "SELECT pg_read_file('/etc/passwd')")
	portal.CanonicalSQLAndFingerprint()
	unsafe := server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection()).Conn
	_, hit, sql, fp, err := exec.resolvePortalPlan(context.Background(), portal, unsafe, nil)
	require.NoError(t, err)
	require.False(t, hit)
	require.Empty(t, sql)
	require.Empty(t, fp)
	_, _, _, _, err = exec.resolvePortalPlan(context.Background(), portal, testConn(), nil)
	require.ErrorContains(t, err, "pg_read_file is not supported")
}
