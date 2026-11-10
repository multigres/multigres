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

package multipooler

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestTimedOutWritesDoNotLeakBackends is a regression test for the heartbeat writer
// exhausting the primary's connection slots while no standby can ack.
//
// With the standbys down, each heartbeat write blocks in its commit waiting for a
// synchronous standby, and times out after one interval. Closing the socket does not
// end that wait, so each timed-out write used to leave its server backend behind until
// a standby acked. Only the first sits in SyncRep: the rest queue behind its row lock,
// since every heartbeat updates the same row. The pooler now asks the server to cancel
// the query before closing the socket, so the number of active heartbeat backends stays
// bounded.
func TestTimedOutWritesDoNotLeakBackends(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-postgres e2e in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("real postgres binaries not available")
	}

	// No multiorch runs, so nothing fails the primary over while the standbys are down.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")

	db, err := sql.Open("postgres", shardsetup.GetPostgresDSN("localhost", primary.Pgctld.PgPort, "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer db.Close()
	// Heartbeat statements still running on the server, whether blocked in the commit
	// wait or queued behind the row lock. The pooled connection that is idle between
	// writes is not counted.
	activeHeartbeatBackends := func() int {
		var n int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_stat_activity
			WHERE state = 'active' AND query LIKE '%multigres.heartbeat%' AND pid <> pg_backend_pid()`).Scan(&n))
		return n
	}

	for _, standby := range setup.GetStandbys() {
		resume := setup.StopPostgres(t, standby.Name, "fast")
		defer resume()
	}

	// Heartbeat writes now block on a standby that never acks. Watch long enough for
	// many of them to time out; a leak adds roughly one stuck backend per second.
	const observe = 20 * time.Second
	var peak int
	sawActive := false
	deadline := time.Now().Add(utils.ScaleTimeout(observe))
	for time.Now().Before(deadline) {
		n := activeHeartbeatBackends()
		sawActive = sawActive || n > 0
		peak = max(peak, n)
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("peak active heartbeat backends: %d", peak)
	require.True(t, sawActive, "heartbeat writes should have been blocked on the server; the scenario did not occur")
	assert.LessOrEqual(t, peak, 3, "timed-out writes must not leave heartbeat backends running on the server")
}
