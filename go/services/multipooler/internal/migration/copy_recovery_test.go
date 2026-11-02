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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// Defaults keep unrelated orchestration tests independent of table recovery.
func (*fakeTarget) CopySnapshot(context.Context, string) (*CopySnapshot, error)           { return nil, nil }
func (*fakeTarget) WithCopyLock(context.Context, string, func(*CopySnapshot) error) error { return nil }
func (*fakeTarget) SetSubscriptionPublications(context.Context, string, string, *CopyRecovery) error {
	return nil
}
func (*fakeTarget) TruncateCopyTables(context.Context, string, *CopyRecovery) error { return nil }
func (*fakeSource) ReadCopySlots(string, []string) (map[string]bool, error)         { return nil, nil }
func (*fakeSource) ValidateCopyPublication(string, []string) error                  { return nil }
func (*fakeSource) EnsureCopyPublication(string, []string) error                    { return nil }

type recoveryTarget struct {
	*fakeTarget
	snapshot    *CopySnapshot
	locked      *CopySnapshot
	lockErr     error
	setErr      error
	truncateErr error
	actions     []string
	onAction    func(string)
}

func (t *recoveryTarget) action(s string) {
	t.actions = append(t.actions, s)
	if t.onAction != nil {
		t.onAction(s)
	}
}

func (t *recoveryTarget) CopySnapshot(context.Context, string) (*CopySnapshot, error) {
	return t.snapshot, nil
}

func (t *recoveryTarget) WithCopyLock(_ context.Context, _ string, fn func(*CopySnapshot) error) error {
	t.action("lock")
	if t.lockErr != nil {
		return t.lockErr
	}
	if t.locked != nil {
		return fn(t.locked)
	}
	return fn(t.snapshot)
}

func (t *recoveryTarget) SetSubscriptionPublications(_ context.Context, _, pub string, r *CopyRecovery) error {
	t.action("publication:" + pub)
	if t.setErr != nil {
		return t.setErr
	}
	t.snapshot.Publications = []string{pub}
	if r.Stage == CopyRecoveryDetach {
		t.snapshot.Relations = slices.DeleteFunc(t.snapshot.Relations, func(rel CopyRelation) bool {
			return slices.ContainsFunc(r.Tables, func(table CopyRecoveryTable) bool { return table.Name == rel.Name })
		})
	} else {
		for _, table := range r.Tables {
			if !slices.ContainsFunc(t.snapshot.Relations, func(rel CopyRelation) bool { return rel.Name == table.Name }) {
				t.snapshot.Relations = append(t.snapshot.Relations, CopyRelation{Name: table.Name, OID: table.OID, State: "i", Ordinary: true})
			}
		}
	}
	return nil
}

func (t *recoveryTarget) TruncateCopyTables(context.Context, string, *CopyRecovery) error {
	t.action("truncate")
	return t.truncateErr
}

type recoverySource struct {
	*fakeSource
	present  map[string]bool
	slotsErr error
	pubErr   error
	actions  []string
}

func (s *recoverySource) ReadCopySlots(_ string, names []string) (map[string]bool, error) {
	s.actions = append(s.actions, fmt.Sprintf("slots:%v", names))
	return s.present, s.slotsErr
}
func (s *recoverySource) ValidateCopyPublication(string, []string) error { return s.pubErr }
func (s *recoverySource) EnsureCopyPublication(name string, _ []string) error {
	s.actions = append(s.actions, "ensure:"+name)
	return s.pubErr
}

func (s *recoverySource) DropPublication(name string) error {
	s.actions = append(s.actions, "drop:"+name)
	return s.dropPubErr
}

func newRecoveryCoord(t *testing.T) (*testCoord, *Migration, *recoveryTarget, *recoverySource) {
	t.Helper()
	tc := newTestCoord(t)
	m := tc.seed(&Migration{Phase: PhaseCopying, CopyData: true, Tables: []string{"public.healthy", "public.stuck"}})
	tgt := &recoveryTarget{fakeTarget: tc.tgt, snapshot: &CopySnapshot{
		SubscriptionOID: 123, SystemID: "18446744073709551600", SlotName: m.SubscriptionName(), Enabled: true,
		Publications: []string{m.PublicationName()},
		Relations: []CopyRelation{
			{Name: "public.healthy", OID: 10, State: "r", Ordinary: true},
			{Name: "public.stuck", OID: 20, State: "f", Ordinary: true},
		},
	}}
	tgt.status = &SubscriptionStatus{TotalRelations: 2, ReadyRelations: 1}
	src := &recoverySource{fakeSource: tc.src, present: map[string]bool{}}
	tc.c.target = tgt
	tc.c.newSource = func(context.Context, string) (migrationSource, error) { return src, nil }
	return tc, m, tgt, src
}

func TestCopyRecoveryPreservesHealthyTable(t *testing.T) {
	tc, m, tgt, src := newRecoveryCoord(t)
	ctx := context.Background()
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.Equal(t, CopyRecoveryDetach, m.CopyRecovery.Stage)
	require.Equal(t, []CopyRecoveryTable{{Name: "public.stuck", OID: 20}}, m.CopyRecovery.Tables)
	require.Contains(t, src.actions, "slots:[pg_123_sync_20_18446744073709551600]")
	tgt.onAction = func(s string) { require.NotNil(t, tc.store.migs[m.ID].CopyRecovery, s) }
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.Equal(t, CopyRecoveryAttach, m.CopyRecovery.Stage)
	require.Len(t, tgt.snapshot.Relations, 1)
	require.Equal(t, "public.healthy", tgt.snapshot.Relations[0].Name)
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.Equal(t, CopyRecoveryWait, m.CopyRecovery.Stage)
	require.Equal(t, PhaseCopying, m.Phase)
	require.NoError(t, tc.c.reconcileLocked(ctx, m)) // the new copy is still INIT
	require.NotNil(t, m.CopyRecovery)
	tgt.snapshot.Relations[1].State = "r"
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.Nil(t, m.CopyRecovery)
	require.Contains(t, src.actions, "drop:"+copyRecoveryPublication(m))
	require.Equal(t, []string{"lock", "publication:" + copyRecoveryPublication(m), "truncate", "publication:" + m.PublicationName()}, tgt.actions)
	tgt.status.ReadyRelations = 2
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.Equal(t, PhaseImporting, m.Phase)
}

func TestCopyRecoveryDiagnosis(t *testing.T) {
	for _, scenario := range []string{"live_handoff", "state_changed_after_lock", "slot_exists", "copy_data_false", "another_copy", "source_error", "preexisting_schema", "publication_changed", "partition", "missing_table", "persist_error", "multiple_broken"} {
		t.Run(scenario, func(t *testing.T) {
			tc, m, tgt, src := newRecoveryCoord(t)
			var wantErr bool
			switch scenario {
			case "live_handoff":
				tgt.lockErr = ErrCopyLockBusy
			case "state_changed_after_lock":
				cp := *tgt.snapshot
				cp.Relations = append([]CopyRelation(nil), cp.Relations...)
				cp.Relations[1].State = "s"
				tgt.locked = &cp
			case "slot_exists":
				src.present[copySyncSlot(tgt.snapshot, tgt.snapshot.Relations[1])] = true
			case "copy_data_false":
				m.CopyData = false
			case "another_copy":
				tgt.snapshot.Relations[0].State = "d"
			case "preexisting_schema":
				m.SkipSchemaCopy = true
				wantErr = true
			case "source_error":
				src.slotsErr = errors.New("unreachable")
				wantErr = true
			case "publication_changed":
				tgt.snapshot.Publications = []string{"different"}
				wantErr = true
			case "partition":
				tgt.snapshot.Relations[1].Ordinary = false
				wantErr = true
			case "missing_table":
				tgt.snapshot.Relations = tgt.snapshot.Relations[1:]
				wantErr = true
			case "persist_error":
				tc.store.updateErr = errors.New("disk full")
				wantErr = true
			case "multiple_broken":
				tgt.snapshot.Relations[0].State = "f"
			}
			err := tc.c.startCopyRecovery(context.Background(), m)
			if wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if scenario == "multiple_broken" {
				require.Len(t, m.CopyRecovery.Tables, 2)
			} else {
				require.Nil(t, m.CopyRecovery)
			}
			for _, action := range tgt.actions {
				require.Equal(t, "lock", action, "diagnosis must not change table data")
			}
		})
	}
}

func TestCopyRecoveryResumeAfterFailure(t *testing.T) {
	for _, failure := range []string{"detach", "truncate", "save_attach", "attach", "save_wait", "cleanup", "save_clear"} {
		t.Run(failure, func(t *testing.T) {
			tc, m, tgt, src := newRecoveryCoord(t)
			ctx := context.Background()
			require.NoError(t, tc.c.startCopyRecovery(ctx, m))
			if failure == "detach" {
				tgt.setErr = errors.New("connection lost")
			}
			if failure == "truncate" {
				tgt.truncateErr = errors.New("foreign key")
			}
			if failure == "save_attach" {
				tc.store.updateErr = errors.New("commit lost")
			}
			if failure == "detach" || failure == "truncate" || failure == "save_attach" {
				require.Error(t, tc.c.reconcileLocked(ctx, m))
				require.Equal(t, CopyRecoveryDetach, m.CopyRecovery.Stage)
				tgt.setErr, tgt.truncateErr, tc.store.updateErr = nil, nil, nil
			}
			require.NoError(t, tc.c.reconcileLocked(ctx, m))
			require.Equal(t, CopyRecoveryAttach, m.CopyRecovery.Stage)
			truncates := slices.Clone(tgt.actions)
			if failure == "attach" {
				tgt.setErr = errors.New("refresh failed")
			}
			if failure == "save_wait" {
				tc.store.updateErr = errors.New("commit lost")
			}
			if failure == "attach" || failure == "save_wait" {
				require.Error(t, tc.c.reconcileLocked(ctx, m))
				require.Equal(t, CopyRecoveryAttach, m.CopyRecovery.Stage)
				tgt.setErr, tc.store.updateErr = nil, nil
			}
			require.NoError(t, tc.c.reconcileLocked(ctx, m))
			require.Equal(t, CopyRecoveryWait, m.CopyRecovery.Stage)
			for i := range tgt.snapshot.Relations {
				tgt.snapshot.Relations[i].State = "r"
			}
			if failure == "cleanup" {
				src.dropPubErr = errors.New("unreachable")
			}
			if failure == "save_clear" {
				tc.store.updateErr = errors.New("commit lost")
			}
			if failure == "cleanup" || failure == "save_clear" {
				require.Error(t, tc.c.reconcileLocked(ctx, m))
				require.Equal(t, CopyRecoveryWait, m.CopyRecovery.Stage)
				src.dropPubErr, tc.store.updateErr = nil, nil
			}
			require.NoError(t, tc.c.reconcileLocked(ctx, m))
			require.Nil(t, m.CopyRecovery)
			count := func(actions []string) int {
				n := 0
				for _, a := range actions {
					if a == "truncate" {
						n++
					}
				}
				return n
			}
			require.Equal(t, count(truncates), count(tgt.actions), "ATTACH and WAIT must never truncate")
		})
	}
}

func TestCopyRecoveryRefusesIdentityDrift(t *testing.T) {
	for _, drift := range []string{"subscription", "system", "relation", "other_table", "publication"} {
		t.Run(drift, func(t *testing.T) {
			tc, m, tgt, _ := newRecoveryCoord(t)
			require.NoError(t, tc.c.startCopyRecovery(context.Background(), m))
			switch drift {
			case "subscription":
				tgt.snapshot.SubscriptionOID++
			case "system":
				tgt.snapshot.SystemID = "different"
			case "relation":
				tgt.snapshot.Relations[1].OID++
			case "other_table":
				tgt.snapshot.Relations[0].State = "d"
			case "publication":
				tgt.snapshot.Publications = []string{"other"}
			}
			require.Error(t, tc.c.reconcileLocked(context.Background(), m))
			require.Equal(t, []string{"lock"}, tgt.actions)
		})
	}
}

func TestCopyRecoveryCannotAppearCaughtUp(t *testing.T) {
	tc, m, tgt, _ := newRecoveryCoord(t)
	require.NoError(t, tc.c.startCopyRecovery(context.Background(), m))
	// Detaching a broken table leaves only READY entries in pg_subscription_rel.
	st := &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1, CaughtUp: true}
	p := tc.c.project(context.Background(), m, st)
	require.False(t, p.CaughtUp)
	tgt.status = st
	require.NoError(t, tc.c.reconcileLocked(context.Background(), m))
	require.Equal(t, PhaseCopying, m.Phase)
}

func TestCopyRecoveryCanRetryAfterSecondFailover(t *testing.T) {
	tc, m, tgt, src := newRecoveryCoord(t)
	ctx := context.Background()
	require.NoError(t, tc.c.startCopyRecovery(ctx, m))
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.Equal(t, CopyRecoveryWait, m.CopyRecovery.Stage)
	// A fresh tablesync loses SYNCDONE after another crash, just like the first.
	tgt.snapshot.Relations[1].State = "f"
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.Equal(t, CopyRecoveryDetach, m.CopyRecovery.Stage)
	require.Contains(t, src.actions, "drop:"+copyRecoveryPublication(m))
	require.NoError(t, tc.c.reconcileLocked(ctx, m))
	require.Equal(t, CopyRecoveryAttach, m.CopyRecovery.Stage)
}
