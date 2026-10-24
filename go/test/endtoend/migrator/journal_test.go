// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// eventName shortens a MigrationEvent to its bare name (e.g. "CREATE"),
// dropping the MIGRATION_EVENT_ prefix, for terser test assertions.
func eventName(e migratorpb.MigrationEvent) string {
	return strings.TrimPrefix(e.String(), "MIGRATION_EVENT_")
}

// journalEvents fetches a migration's journal and returns its event sequence.
func journalEvents(t *testing.T, ctx context.Context, mt migratorpb.MigratorClient, id int64) ([]string, []*migratorpb.MigrationJournalEntry) {
	t.Helper()
	resp, err := mt.GetMigrationJournal(ctx, &migratorpb.GetMigrationJournalRequest{Id: id})
	require.NoError(t, err)
	entries := resp.GetEntries()
	events := make([]string, len(entries))
	for i, e := range entries {
		events[i] = eventName(e.GetEvent())
	}
	return events, entries
}

// findEntry returns the first journal entry with the given event, or nil.
func findEntry(entries []*migratorpb.MigrationJournalEntry, event string) *migratorpb.MigrationJournalEntry {
	for _, e := range entries {
		if eventName(e.GetEvent()) == event {
			return e
		}
	}
	return nil
}

// containsInOrder reports whether want appears as an ordered subsequence of got.
func containsInOrder(got, want []string) bool {
	i := 0
	for _, g := range got {
		if i < len(want) && g == want[i] {
			i++
		}
	}
	return i == len(want)
}

// TestMigrationJournalLifecycle drives a migration through create -> start ->
// activate -> deactivate -> drop and asserts the durable journal accumulates the
// expected lifecycle events, that the direction switches record their handoff
// LSNs, and that the journal is retained (still readable) after the DROP removes
// the migration row.
func TestMigrationJournalLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator journal e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(
		t,
		shardsetup.WithMultipoolerCount(2),
		// EXPORT makes the Multigres target the logical publisher, which needs
		// wal_level=logical (and non-temporary-slot admission) that slot-based
		// replication turns on.
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		Migration: &migratorpb.MigrationRecord{
			Target:         &clustermetadatapb.ShardKey{Database: targetDB},
			Name:           "ledger",
			ConnectionName: createTestConnection(t, ctx, mt, sourceDSN(srcPort)),
			Objects:        objs("public.orders"),
		},
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()

	// After create, the journal already holds a CREATE entry.
	events, _ := journalEvents(t, ctx, mt, id)
	require.Equal(t, []string{"CREATE"}, events, "create records exactly one journal entry")

	_, err = mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := mt.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: idRef(id)})
		return err == nil && resp.GetStatus().GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "IMPORT must catch up")

	// After start + catch-up, START and the phase advances (including COPYING ->
	// IMPORTING) are recorded.
	events, _ = journalEvents(t, ctx, mt, id)
	require.True(t, containsInOrder(events, []string{"CREATE", "START", "PHASE"}),
		"start records START then phase advances, got %v", events)

	// Activate: IMPORT -> EXPORT. The handoff entry records both LSNs.
	_, err = mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT})
	require.NoError(t, err)
	events, entries := journalEvents(t, ctx, mt, id)
	require.Contains(t, events, "ACTIVATE")
	activate := findEntry(entries, "ACTIVATE")
	require.NotNil(t, activate)
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, activate.GetDirection())
	require.NotEmpty(t, activate.GetFromLsn(), "ACTIVATE records the drained-to (quiesce) LSN")
	require.NotEmpty(t, activate.GetToLsn(), "ACTIVATE records the new-writer start LSN")
	require.Equal(t, "ledger", activate.GetMigrationName(), "the migration name is denormalized into the entry")

	// Deactivate: EXPORT -> IMPORT.
	_, err = mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := mt.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: idRef(id)})
		return err == nil && resp.GetStatus().GetCaughtUp()
	}, 30*time.Second, 500*time.Millisecond, "IMPORT must catch up again after deactivate")
	events, entries = journalEvents(t, ctx, mt, id)
	require.Contains(t, events, "DEACTIVATE")
	deactivate := findEntry(entries, "DEACTIVATE")
	require.NotNil(t, deactivate)
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, deactivate.GetDirection())
	require.NotEmpty(t, deactivate.GetFromLsn(), "DEACTIVATE records the drained-to (quiesce) LSN")

	// Drop (graceful): tears the migration down and removes its row.
	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Ref: idRef(id), Wait: true, WaitTimeoutSeconds: 30})
	require.NoError(t, err)

	// The migration row is gone from the live projection...
	getResp, err := mt.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: idRef(id)})
	require.Error(t, err, "the dropped migration must no longer be found in the live projection: %v", getResp)

	// ...but its journal is retained and addressable by id, ending with DROP.
	events, _ = journalEvents(t, ctx, mt, id)
	require.True(t, containsInOrder(events, []string{"CREATE", "START", "ACTIVATE", "DEACTIVATE", "DROP"}),
		"the journal survives the drop with the full lifecycle in order, got %v", events)
	require.Equal(t, "DROP", events[len(events)-1], "DROP is the final entry")
}
