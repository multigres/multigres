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

package connpoolmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	pgserver "github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/pools/reserved"
)

// fakeReplicationCredentialProvider returns IsReplicationRole=true for every
// role so fakepgserver accepts replication-mode startup parameters.
type fakeReplicationCredentialProvider struct{}

func (fakeReplicationCredentialProvider) GetCredentials(_ context.Context, _, _ string) (*pgserver.Credentials, error) {
	return &pgserver.Credentials{
		Hash:              &scram.ScramHash{},
		IsReplicationRole: true,
	}, nil
}

func TestManagerNewLogicalReplicationConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	mgr := newTestManager(t, server)
	defer mgr.Close()

	const user = "test_user"
	conn, err := mgr.NewLogicalReplicationConn(context.Background(), user, nil, nil)
	require.NoError(t, err)
	defer conn.Release(reserved.ReleaseError)

	assert.True(t, protoutil.HasLogicalReplicationReason(conn.RemainingReasons()),
		"replication conn must be tagged with ReasonLogicalReplication")
	assert.Equal(t, pgserver.ReplicationLogical, server.LastReplicationMode(),
		"replication=database must have been sent in the startup message")

	// The factory must create a per-user pool, not place the replication conn
	// on the shared admin pool.
	assert.True(t, mgr.HasUserPool(user),
		"replication conn must be checked out from the user's pool, not the admin pool")
}

// TestManager_LogicalReplicationSharesReservedCap proves at the Manager API
// that replication conns and transactional reserved conns share a single
// per-user reserved-pool capacity. Filling the cap via NewReservedConn must
// block subsequent NewLogicalReplicationConn calls, and releasing one reserved
// conn must unblock replication. This is the higher-layer regression that
// guards the wiring fixed in Task 1.
func TestManager_LogicalReplicationSharesReservedCap(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	mgr := newTestManager(t, server)
	defer mgr.Close()

	const user = "repl_user"
	ctx := context.Background()

	// Force the per-user pool into existence and pin its reserved cap to 2
	// so the test is deterministic regardless of the default reservedRatio.
	pool, err := mgr.getOrCreateUserPool(user, nil, nil)
	require.NoError(t, err)
	// Pin reserved cap to 2. Regular cap is irrelevant for this test —
	// reserved conns and replication conns share the reserved pool only.
	require.NoError(t, pool.SetCapacity(ctx, 4, 2))

	// Fill the reserved cap with transactional reserved conns.
	r1, err := mgr.NewReservedConn(ctx, nil, user, nil, nil)
	require.NoError(t, err)
	r2, err := mgr.NewReservedConn(ctx, nil, user, nil, nil)
	require.NoError(t, err)

	// Sanity-check that the rebalancer hasn't moved the reserved cap out
	// from under us between SetCapacity and the blocking call below.
	stats := pool.Stats()
	require.Equal(t, int64(2), stats.Reserved.RegularPool.Capacity,
		"reserved cap drifted from pinned 2 — rebalancer may have run")

	// A replication request must now block on the cap. Use a short ctx so
	// the test fails fast rather than hanging if cap-sharing is broken.
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	blocked, err := mgr.NewLogicalReplicationConn(shortCtx, user, nil, nil)
	require.Error(t, err, "replication conn must not be created while the user's reserved cap is full")
	// Pin the failure mode to the cap-acquire timeout path. The underlying
	// connpool returns ErrTimeout when waitForConn() exits due to ctx
	// expiration, so a regression that fails earlier (e.g. dial error,
	// auth error) would no longer satisfy this assertion.
	require.ErrorIs(t, err, connpool.ErrTimeout,
		"blocking must be due to cap-acquire timing out, not some unrelated error")
	assert.Nil(t, blocked)

	// Releasing one reserved conn must free a slot and unblock replication.
	r1.Release(reserved.ReleaseCommit)

	rl, err := mgr.NewLogicalReplicationConn(ctx, user, nil, nil)
	require.NoError(t, err, "releasing a reserved conn must let a replication conn through")
	require.NotNil(t, rl)
	defer rl.Release(reserved.ReleaseError)
	defer r2.Release(reserved.ReleaseCommit)
}
