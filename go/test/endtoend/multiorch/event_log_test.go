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
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestEventLog_Bootstrap verifies that lifecycle events are emitted
// to service logs during cluster bootstrap.
//
// Events checked:
//   - primary.init started/success  (multipooler log of primary)
//   - backup.attempt started/success (multipooler log of primary)
//   - term.begin started/success     (multipooler log of primary)
//   - restore.attempt started/success (multipooler log of each standby)
//   - primary.promotion started/success (multiorch log)
func TestEventLog_Bootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end event log test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end event log test (no postgres binaries)")
	}

	// Bootstrap a fresh 3-node cluster via multiorch
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithoutInitialization(),
		shardsetup.WithDurabilityPolicy("ANY_2"),
	)
	t.Cleanup(cleanup)

	// Start multiorch to trigger bootstrap
	watchTargets := []string{"postgres/default/0-inf"}
	config := &shardsetup.SetupConfig{CellName: setup.CellName}
	mo, moCleanup := setup.CreateMultiOrchInstance(t, "event-test-multiorch", watchTargets, config)
	require.NoError(t, mo.Start(t.Context(), t), "should start multiorch")
	t.Cleanup(moCleanup)

	// Wait for full bootstrap: primary elected and all standbys initialized
	primaryName := waitForShardPrimary(t, setup, 60*time.Second)
	require.NotEmpty(t, primaryName, "Expected multiorch to bootstrap shard")
	setup.PrimaryName = primaryName
	waitForStandbysInitialized(t, setup, 2, 60*time.Second)

	primary := setup.GetMultipoolerInstance(setup.PrimaryName)
	require.NotNil(t, primary)

	t.Run("primary.init, backup.attempt, and term.begin events in primary multipooler log", func(t *testing.T) {
		data, err := os.ReadFile(primary.Multipooler.LogFile)
		require.NoError(t, err, "should read primary multipooler log")

		events := shardsetup.ParseEvents(t, bytes.NewReader(data))
		t.Logf("Found %d events in primary multipooler log", len(events))

		assert.True(t, shardsetup.HasEvent(events, "primary.init", "started"),
			"expected primary.init started in primary multipooler log; got events: %v", events)
		assert.True(t, shardsetup.HasEvent(events, "primary.init", "success"),
			"expected primary.init success in primary multipooler log; got events: %v", events)

		assert.True(t, shardsetup.HasEvent(events, "backup.attempt", "started"),
			"expected backup.attempt started in primary multipooler log; got events: %v", events)
		assert.True(t, shardsetup.HasEvent(events, "backup.attempt", "success"),
			"expected backup.attempt success in primary multipooler log; got events: %v", events)

		assert.True(t, shardsetup.HasEvent(events, "term.begin", "started"),
			"expected term.begin started in primary multipooler log; got events: %v", events)
		assert.True(t, shardsetup.HasEvent(events, "term.begin", "success"),
			"expected term.begin success in primary multipooler log; got events: %v", events)
	})

	t.Run("restore.attempt events in standby multipooler logs", func(t *testing.T) {
		for name, inst := range setup.Multipoolers {
			if name == setup.PrimaryName {
				continue
			}
			// Wait for restore.attempt success — the restore may still be running even
			// after waitForStandbysInitialized returns, because isInitialized() can
			// return true (via the marker file extracted from the backup) before
			// restoreFromBackupLocked completes and emits its success event.
			events := shardsetup.WaitForEvent(t, inst.Multipooler.LogFile,
				"restore.attempt", "success", 30*time.Second)
			t.Logf("Found %d events in standby %s multipooler log", len(events), name)

			assert.True(t, shardsetup.HasEvent(events, "restore.attempt", "started"),
				"expected restore.attempt started in standby %s log; got events: %v", name, events)
		}
	})

	t.Run("primary.promotion event in multiorch log", func(t *testing.T) {
		data, err := os.ReadFile(mo.LogFile)
		require.NoError(t, err, "should read multiorch log")

		events := shardsetup.ParseEvents(t, bytes.NewReader(data))
		t.Logf("Found %d events in multiorch log", len(events))

		assert.True(t, shardsetup.HasEvent(events, "primary.promotion", "started"),
			"expected primary.promotion started in multiorch log; got events: %v", events)
		assert.True(t, shardsetup.HasEvent(events, "primary.promotion", "success"),
			"expected primary.promotion success in multiorch log; got events: %v", events)
	})
}
