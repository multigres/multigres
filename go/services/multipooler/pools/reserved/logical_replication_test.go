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

package reserved

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	pgserver "github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/pools/regular"
)

// fakeReplicationCredentialProvider is a test CredentialProvider that
// reports every role as having the REPLICATION attribute. Used to let
// fakepgserver accept replication-mode startup parameters.
type fakeReplicationCredentialProvider struct{}

func (fakeReplicationCredentialProvider) GetCredentials(_ context.Context, _, _ string) (*pgserver.Credentials, error) {
	return &pgserver.Credentials{
		Hash:              &scram.ScramHash{},
		IsReplicationRole: true,
	}, nil
}

func TestLogicalReplicationConfigAddsStartupParameter(t *testing.T) {
	base := client.Config{
		Parameters: map[string]string{"application_name": "multipooler"},
	}

	cfg := logicalReplicationClientConfig(base)

	assert.Equal(t, "database", cfg.Parameters["replication"])
	assert.Equal(t, "multipooler", cfg.Parameters["application_name"],
		"existing parameters must be preserved")

	_, baseHas := base.Parameters["replication"]
	assert.False(t, baseHas, "must not mutate caller's config")
}

func TestLogicalReplicationConfigHandlesNilParameters(t *testing.T) {
	base := client.Config{}

	cfg := logicalReplicationClientConfig(base)

	assert.Equal(t, "database", cfg.Parameters["replication"])
	assert.Nil(t, base.Parameters, "must not mutate caller's config")
}

func TestNewLogicalReplicationConnTagsReasonAndStartupParam(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	pool := newTestPool(t, server)
	defer pool.Close()

	conn, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)
	defer conn.Release(ReleaseError) // Pooled wrapper has pool=nil → closes the underlying socket.

	assert.True(t, protoutil.HasLogicalReplicationReason(conn.RemainingReasons()),
		"replication conn must be tagged with ReasonLogicalReplication")
	assert.Equal(t, pgserver.ReplicationLogical, server.LastReplicationMode(),
		"replication=database must have been parsed by the server")
}

func TestNewLogicalReplicationConnIsExemptFromIdleKiller(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	pool := newTestPool(t, server)
	defer pool.Close()

	conn, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)
	defer conn.Release(ReleaseError)

	assert.Zero(t, conn.InactivityTimeout(),
		"replication conn must carry inactivityTimeout=0 so the idle killer skips it; "+
			"idle teardown is Postgres' wal_sender_timeout's job")
	assert.False(t, conn.IsTimedOut())
}

func TestNewLogicalReplicationConnBlocksWhenCapFull(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{Capacity: 2, MaxIdleCount: 2},
		},
	})
	defer pool.Close()

	c1, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)
	c2, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)

	shortCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c3, err := pool.NewLogicalReplicationConn(shortCtx)
	require.Error(t, err, "third replication conn must not be created while cap=2 is full")
	assert.Nil(t, c3)

	c1.Release(ReleaseError)
	c4, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err, "releasing a slot must let a new replication conn through")
	defer c4.Release(ReleaseError)
	defer c2.Release(ReleaseError)
}

// TestNewLogicalReplicationConnDialFailureFreesSlot exercises the
// dial-failure cleanup branch in Pool.NewLogicalReplicationConn. With
// cap=1, we succeed once (and release), then arm the fake server to
// reject the next replication startup; the failed acquire must surface
// an error and must NOT leak the slot. The discriminator is the
// short-context third acquire: if the slot leaked, Get would block on
// the empty pool until the short context fires; if it was freed, the
// fresh dial succeeds well within the deadline.
func TestNewLogicalReplicationConnDialFailureFreesSlot(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})

	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{Capacity: 1, MaxIdleCount: 1},
		},
	})
	defer pool.Close()

	// Baseline: first open succeeds and releases the slot back to the pool.
	ok, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)
	ok.Release(ReleaseError)

	// Arm the one-shot: the next replication-mode startup will be
	// rejected at the rolreplication gate. The toggle auto-resets so
	// the third acquire below is unaffected.
	server.SetRejectNextReplicationStartup(true)

	bad, err := pool.NewLogicalReplicationConn(context.Background())
	require.Error(t, err, "dial failure must surface as an error")
	assert.Nil(t, bad)

	// If the slot leaked, the next acquire would block on Get until the
	// short context fires. Use a tight deadline so a regression here
	// fails fast and visibly rather than waiting on the default timeout.
	shortCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	recovered, err := pool.NewLogicalReplicationConn(shortCtx)
	require.NoError(t, err, "slot from failed dial must be freed")
	recovered.Release(ReleaseError)
}

func TestPoolStatsLogicalReplicationActive(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetCredentialProvider(fakeReplicationCredentialProvider{})
	server.SetNeverFail(true) // needed for the plain NewConn path

	pool := newTestPool(t, server)
	defer pool.Close()

	assert.Equal(t, 0, pool.Stats().LogicalReplicationActive)

	c1, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)
	c2, err := pool.NewLogicalReplicationConn(context.Background())
	require.NoError(t, err)
	plain, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	s := pool.Stats()
	assert.Equal(t, 2, s.LogicalReplicationActive, "only replication conns should be counted")
	assert.Equal(t, 3, s.Active, "Active still totals all reserved conns")

	c1.Release(ReleaseError)
	s = pool.Stats()
	assert.Equal(t, 1, s.LogicalReplicationActive)

	c2.Release(ReleaseError)
	plain.Release(ReleaseCommit)
}
