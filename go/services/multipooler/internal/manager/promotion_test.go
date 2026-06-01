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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

// TestWaitForPromotionComplete_KeepsPollingUntilPostgresReady verifies that
// waitForPromotionComplete keeps polling pg_isready even after
// pg_is_in_recovery() returns false, covering the WAL replay window.
//
// This is the regression test for the cascade re-election bug: the previous
// code had a 30 s inner timeout that caused promotionInProgress to be cleared
// before postgres was ready, triggering spurious LeaderIsDead elections.
func TestWaitForPromotionComplete_KeepsPollingUntilPostgresReady(t *testing.T) {
	const readyAfterNCalls = 4 // pg_isready returns false for the first 3 calls

	var statusCallCount atomic.Int32
	pgctldSvc := &testutil.MockPgCtldService{
		StatusFunc: func(_ *pgctldpb.StatusRequest) (*pgctldpb.StatusResponse, error) {
			n := int(statusCallCount.Add(1))
			return &pgctldpb.StatusResponse{
				Status: pgctldpb.ServerStatus_RUNNING,
				Ready:  n >= readyAfterNCalls,
			}, nil
		},
	}
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, pgctldSvc)
	t.Cleanup(cleanupPgctld)

	qs := mock.NewQueryService()
	// pg_is_in_recovery returns false — WAL-level promotion already done
	qs.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	pm, _ := setupManagerWithMockDB(t, qs, &fakeRuleStore{})
	overridePgctldClient(t, pm, pgctldAddr)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := pm.waitForPromotionComplete(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, int(statusCallCount.Load()), readyAfterNCalls,
		"should have polled pg_isready at least %d times before succeeding", readyAfterNCalls)
}

// TestWaitForPromotionComplete_ContextCancellationReturnsError verifies that
// a cancelled context causes waitForPromotionComplete to return an error so
// the caller clears promotionInProgress and multiorch can take corrective
// action rather than waiting forever.
func TestWaitForPromotionComplete_ContextCancellationReturnsError(t *testing.T) {
	pgctldSvc := &testutil.MockPgCtldService{
		StatusFunc: func(_ *pgctldpb.StatusRequest) (*pgctldpb.StatusResponse, error) {
			return &pgctldpb.StatusResponse{
				Status: pgctldpb.ServerStatus_RUNNING,
				Ready:  false, // never becomes ready
			}, nil
		},
	}
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, pgctldSvc)
	t.Cleanup(cleanupPgctld)

	qs := mock.NewQueryService()
	qs.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	pm, _ := setupManagerWithMockDB(t, qs, &fakeRuleStore{})
	overridePgctldClient(t, pm, pgctldAddr)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	err := pm.waitForPromotionComplete(ctx)
	require.Error(t, err, "should return error when context times out")
}

// overridePgctldClient replaces the manager's pgctld gRPC client with one
// pointing at the provided address. This lets tests inject a controllable mock
// pgctld after the manager has already been created with a default one.
func overridePgctldClient(t *testing.T, pm *MultiPoolerManager, pgctldAddr string) {
	t.Helper()
	conn, err := grpccommon.NewClient(pgctldAddr,
		grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	pm.pgctldClient = NewProtectedPgctldClient(pgctldpb.NewPgCtldClient(conn))
}
