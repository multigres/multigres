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

package multiorch

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestSlotSyncPropagatesFailoverSlot verifies the slot-based physical-replication
// machinery end to end against real PostgreSQL: with
// --enable-slot-based-replication on, a failover logical slot created on the
// primary is copied to the standby by PostgreSQL's native slot-sync worker and
// becomes failover-ready there (synced, not temporary, not invalidated).
//
// This is the foundation the durable slot-creation barrier (multipooler) and
// slot-aware leader appointment (multiorch) build on: both assume that with the
// flag on, a failover slot actually propagates and persists on standbys. It
// exercises the full flag-gated path — the primary's per-follower physical slot,
// the standby's primary_slot_name + hot_standby_feedback + sync_replication_slots
// — without the gateway or the barrier in the way, so a failure here localizes to
// the machinery rather than the barrier logic layered on top.
func TestSlotSyncPropagatesFailoverSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end slot-sync test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end slot-sync test (no postgres binaries)")
	}

	// Primary + one standby, synchronous durability, slot-based replication on so
	// the primary creates the follower's physical slot and the standby enables
	// primary_slot_name + hot_standby_feedback + sync_replication_slots.
	setup, cleanup := shardsetup.NewIsolated(
		t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithoutInitialization(),
		shardsetup.WithDurabilityPolicy("AT_LEAST_2"),
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
	defer cleanup()

	// Start multiorch to bootstrap: elect a primary, recruit the standby, and
	// configure sync replication and (with the flag on) the slot machinery.
	watchTargets := []string{"postgres/default/0-inf"}
	config := &shardsetup.SetupConfig{CellName: setup.CellName}
	mo, moCleanup := setup.CreateMultiorchInstance(t, "test-multiorch", watchTargets, config)
	require.NoError(t, mo.Start(t.Context(), t), "should start multiorch")
	t.Cleanup(moCleanup)

	primaryName := waitForShardReady(t, setup, 1, 60*time.Second)
	require.NotEmpty(t, primaryName, "multiorch should bootstrap the shard")

	primary := setup.Multipoolers[primaryName]
	var standby *shardsetup.MultipoolerInstance
	var standbyName string
	for name, inst := range setup.Multipoolers {
		if name != primaryName {
			standby, standbyName = inst, name
			break
		}
	}
	require.NotNil(t, standby, "expected a standby distinct from the primary")

	// Create a failover logical slot on the primary. A failover subscription uses
	// pgoutput; here we use test_decoding so the test can act as a self-contained
	// consumer via pg_logical_slot_get_changes. Slot-sync is plugin-agnostic, so
	// this exercises the same machinery a pgoutput subscription relies on. A small
	// table gives the consumer real changes to decode.
	primaryDB := connectToPostgres(t, filepath.Join(primary.Pgctld.PoolerDir, "pg_sockets"), primary.Pgctld.PgPort)
	defer primaryDB.Close()

	const slot = "mg_e2e_failover"
	execCtx := utils.WithTimeout(t, 10*time.Second)
	_, err := primaryDB.ExecContext(execCtx,
		"SELECT pg_create_logical_replication_slot($1, 'test_decoding', false, false, true)", slot)
	require.NoError(t, err, "create failover logical slot on primary %s", primaryName)
	_, err = primaryDB.ExecContext(execCtx, "CREATE TABLE IF NOT EXISTS mg_e2e_probe (id bigint)")
	require.NoError(t, err, "create probe table on primary %s", primaryName)

	// The native slot-sync worker on the standby copies the slot and, once the
	// standby has caught up, persists it — at which point it is failover-ready.
	//
	// A never-consumed slot keeps its catalog_xmin frozen at creation while the
	// standby's catalog horizon creeps forward, and slot-sync refuses to persist a
	// slot whose catalog_xmin is behind the standby. So the test behaves like a
	// normal consumer: each iteration it writes a row and drains the slot with
	// pg_logical_slot_get_changes, which advances the slot's catalog_xmin over the
	// changes it actually received — the way a real subscriber does, and unlike
	// pg_replication_slot_advance, which would skip past undelivered changes. That
	// naturally overtakes the standby's horizon, and slot-sync then persists the
	// slot. Once synced, the standby's own slot holds catalog_xmin and it stays
	// failover-ready.
	standbyDB := connectToPostgres(t, filepath.Join(standby.Pgctld.PoolerDir, "pg_sockets"), standby.Pgctld.PgPort)
	defer standbyDB.Close()

	require.Eventually(t, func() bool {
		pctx := utils.WithTimeout(t, 5*time.Second)
		if _, err := primaryDB.ExecContext(pctx, "INSERT INTO mg_e2e_probe VALUES (1)"); err != nil {
			return false
		}
		if _, err := primaryDB.ExecContext(pctx,
			"SELECT count(*) FROM pg_logical_slot_get_changes($1, NULL, NULL)", slot); err != nil {
			return false
		}

		var ready bool
		qctx := utils.WithTimeout(t, 5*time.Second)
		if err := standbyDB.QueryRowContext(qctx,
			`SELECT synced AND NOT temporary AND invalidation_reason IS NULL
			   FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&ready); err != nil {
			// sql.ErrNoRows until the slot has synced across; keep polling.
			return false
		}
		return ready
	}, 30*time.Second, 1*time.Second,
		"failover slot %q should become failover-ready on standby %s via native slot-sync", slot, standbyName)
}
