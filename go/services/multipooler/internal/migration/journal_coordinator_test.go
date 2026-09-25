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

package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// journalEntriesFor returns the coordinator's appended journal entries for one
// migration, in order.
func (tc *testCoord) journalEntriesFor(id int64) []*JournalEntry {
	var out []*JournalEntry
	for _, e := range tc.store.journal {
		if e.MigrationID == id {
			out = append(out, e)
		}
	}
	return out
}

// TestJournal_LifecycleSequence drives create -> start -> activate -> deactivate
// -> drop through the coordinator's public API and asserts the journal
// accumulates exactly the expected event sequence, that the direction switches
// carry the handoff LSNs, and that the entries are retained after the drop.
func TestJournal_LifecycleSequence(t *testing.T) {
	tc := newTestCoord(t)
	tc.c.now = time.Now // the activation readiness gate needs an advancing clock
	tc.src.resolved = []string{"public.orders"}
	tc.src.currentLSN = "0/5000" // source LSN: drained-to on ACTIVATE, new-writer on DEACTIVATE
	tc.tgt.currentLSN = "0/9000" // target LSN: new-writer on ACTIVATE, drained-to on DEACTIVATE
	// Both subscriptions read as live so each switch's drain barrier actually runs
	// (populating the handoff from_lsn).
	tc.tgt.subExists = true
	tc.src.subExists = true
	// Catch-up status so StartMigration advances COPYING -> IMPORTING and the
	// activation readiness gate is satisfied immediately.
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1}
	tc.src.lagPresent = true
	tc.src.lag = 0

	ctx := context.Background()

	created, err := tc.c.CreateMigration(ctx, CreateParams{
		SourceDSN: "host=src dbname=app", TargetDatabase: "app",
		Tables: []string{"public.orders"}, CopyData: true,
	})
	require.NoError(t, err)
	id := created.ID

	_, err = tc.c.StartMigration(ctx, Ref{ID: id})
	require.NoError(t, err)
	_, err = tc.c.Activate(ctx, Ref{ID: id}, ActivateOptions{MaxLagBytes: 1024, WaitTimeout: time.Second})
	require.NoError(t, err)
	_, err = tc.c.Deactivate(ctx, Ref{ID: id})
	require.NoError(t, err)
	_, err = tc.c.DropMigration(ctx, Ref{ID: id}, DropOptions{})
	require.NoError(t, err)

	require.Equal(t, []JournalEvent{
		JournalEventCreate,
		JournalEventStart,
		JournalEventPhase, // CREATED -> VALIDATING
		JournalEventPhase, // VALIDATING -> SCHEMA_COPY
		JournalEventPhase, // SCHEMA_COPY -> CREATE_PUBLICATION
		JournalEventPhase, // CREATE_PUBLICATION -> COPYING
		JournalEventPhase, // COPYING -> IMPORTING
		JournalEventActivate,
		JournalEventDeactivate,
		JournalEventDrop,
	}, tc.store.journalEvents(id))

	entries := tc.journalEntriesFor(id)

	// The setup phase advances carry a "from->to" detail.
	require.Equal(t, string(PhaseCreated)+"->"+string(PhaseValidating), entries[2].Detail)
	require.Equal(t, string(PhaseCopying)+"->"+string(PhaseImporting), entries[6].Detail)

	// ACTIVATE (IMPORT->EXPORT): drained-to is the source LSN, new-writer is the target.
	activate := entries[7]
	require.Equal(t, JournalEventActivate, activate.Event)
	require.Equal(t, DirectionExport, activate.Direction)
	require.Equal(t, PhaseExporting, activate.Phase)
	require.Equal(t, "0/5000", activate.FromLSN, "ACTIVATE from_lsn is the drained source LSN")
	require.Equal(t, "0/9000", activate.ToLSN, "ACTIVATE to_lsn is the new-writer (target) LSN")

	// DEACTIVATE (EXPORT->IMPORT): drained-to is the target LSN, new-writer is the source.
	deactivate := entries[8]
	require.Equal(t, DirectionImport, deactivate.Direction)
	require.Equal(t, "0/9000", deactivate.FromLSN, "DEACTIVATE from_lsn is the drained target LSN")
	require.Equal(t, "0/5000", deactivate.ToLSN, "DEACTIVATE to_lsn is the new-writer (source) LSN")

	// Every entry denormalizes the migration id/name (name empty here) — never a DSN.
	for _, e := range entries {
		require.Equal(t, id, e.MigrationID)
	}

	// Retention: the migration row is gone, but its journal is still readable.
	require.Empty(t, tc.store.migs, "the migration row is deleted by the drop")
	retained, err := tc.c.GetMigrationJournal(ctx, Ref{ID: id})
	require.NoError(t, err)
	require.Len(t, retained, len(entries), "journal survives the drop and is addressable by id")
	require.Equal(t, JournalEventDrop, retained[len(retained)-1].Event)
}

// TestJournal_FailAppendsEntry proves a runSetup failure appends a FAILED entry
// carrying the error, after the CREATE/START/PHASE entries recorded up to the
// failure point.
func TestJournal_FailAppendsEntry(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseCreated})
	tc.tgt.createSubErr = errors.New("subscription blew up")

	_, err := tc.c.StartMigration(context.Background(), Ref{ID: m.ID})
	require.Error(t, err)

	events := tc.store.journalEvents(m.ID)
	require.Equal(t, JournalEventStart, events[0])
	require.Equal(t, JournalEventFailed, events[len(events)-1], "a runSetup failure appends FAILED last")

	entries := tc.journalEntriesFor(m.ID)
	failed := entries[len(entries)-1]
	require.Equal(t, PhaseFailed, failed.Phase)
	require.Contains(t, failed.LastError, "subscription blew up")
}

// TestJournal_HandoffAppendIsFatal proves the handoff journal append is durable:
// if it fails, the switch call errors and the migration is left in its SWITCHING_*
// intent phase (so the reconcile poller re-runs and heals it) rather than
// committing the switch without an auditable handoff record.
func TestJournal_HandoffAppendIsFatal(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseExporting})
	tc.src.subExists = true // export link live -> drain runs
	tc.store.insertJournalErr = errors.New("journal write failed")

	_, err := tc.c.Deactivate(context.Background(), Ref{ID: m.ID})
	require.ErrorContains(t, err, "handoff journal entry")
	require.Equal(t, PhaseSwitchingToImport, tc.store.migs[m.ID].Phase,
		"a failed handoff append leaves the SWITCHING_* intent for the reconcile poller to roll forward")
}

// TestJournal_DropForceRecordsEntry proves a forced (undrained) drop still
// appends a DROP entry, with an empty from_lsn (no drain LSN).
func TestJournal_DropForceRecordsEntry(t *testing.T) {
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseImporting})
	_, err := tc.c.DropMigration(context.Background(), Ref{ID: m.ID}, DropOptions{Force: true})
	require.NoError(t, err)

	entries := tc.journalEntriesFor(m.ID)
	require.Len(t, entries, 1)
	require.Equal(t, JournalEventDrop, entries[0].Event)
	require.Empty(t, entries[0].FromLSN, "a forced drop skips the drain, so from_lsn is empty")
}
