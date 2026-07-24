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

package pgbouncertests

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMUL470LogicalReplicationSlotSurvivesPoolContention proves that a
// TEMPORARY logical replication slot created via
// pg_create_logical_replication_slot(...) on one client session's connection
// stays reachable from later statements on that SAME connection, even under
// heavy contention for a deliberately tiny connection pool. This is the
// regression test for MUL-470: absent the reservation added by this plan, a
// temporary slot's creating backend can be reassigned to a different client
// mid-session, and later reads fail with "replication slot ... is active for
// PID ..." or "does not exist" — see this plan's Background section for the
// original 93/200 failure rate observed pre-fix.
func TestMUL470LogicalReplicationSlotSurvivesPoolContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(connpoolGlobalCapacity, connpoolReservedRatio, connpoolRebalanceFast),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 120*time.Second)

	gatewayDB := openGatewayDB(t, setup)
	defer gatewayDB.Close()

	settleConnpool(t, ctx, gatewayDB)

	// One dedicated client connection, exactly like Realtime's long-lived
	// ReplicationPoller connection: every statement below rides the SAME
	// physical client->gateway TCP connection.
	slotConn, err := gatewayDB.Conn(ctx)
	require.NoError(t, err)
	defer slotConn.Close()

	slotName := "mul470_repro_slot"
	_, err = slotConn.ExecContext(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'test_decoding', true)", slotName)
	require.NoError(t, err, "slot creation must succeed")
	t.Logf("created temporary logical replication slot %q", slotName)

	// Noisy neighbors: hammer the SAME tiny shared pool concurrently so the
	// gateway is forced to reassign backend connections for non-reserved
	// statements (per executor.go: every unreserved statement does
	// GetRegularConnWithSettings + recycle, from a shared per-role LIFO stack).
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for !stop.Load() {
				_, _ = gatewayDB.ExecContext(ctx, "SELECT 1")
			}
		})
	}
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()

	// Repeatedly read from the slot on the SAME slotConn, exactly like
	// ReplicationPoller's list_changes poll loop. Without a reservation, this
	// should eventually land on a different backend than the one that created
	// the slot and fail with "replication slot ... does not exist".
	var lastErr error
	attempts := 200
	failures := 0
	for i := range attempts {
		_, err := slotConn.ExecContext(ctx,
			"SELECT * FROM pg_logical_slot_get_changes($1, NULL, NULL)", slotName)
		if err != nil {
			failures++
			lastErr = err
			t.Logf("iteration %d: read from slot failed: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if failures > 0 {
		t.Logf("REPRODUCED: %d/%d slot reads failed under pool contention without a reservation; last error: %v",
			failures, attempts, lastErr)
	}
	require.Zero(t, failures,
		"every read from the temporary slot on the same client connection must succeed; "+
			"a failure here means the backend was reassigned mid-session (MUL-470)")
}
