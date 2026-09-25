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
	"log/slog"
	"testing"
	"time"
)

// These fakes back the coordinator orchestration unit tests. They let a test drive
// the coordinator's error and crash-recovery branches deterministically — without a
// live Postgres — by injecting per-method errors and canned results into the three
// ports (store/target/source). Every side-effecting call is recorded in the shared
// call log so a test can assert which steps ran (e.g. that a teardown took the
// correct direction).

// fakeStore is an in-memory migrationStore.
type fakeStore struct {
	migs map[string]*Migration

	ensureErr error
	insertErr error
	updateErr error
	deleteErr error
	getErr    error
	listErr   error

	updates int
	deletes []string
}

func newFakeStore() *fakeStore { return &fakeStore{migs: map[string]*Migration{}} }

func (f *fakeStore) EnsureSchema(context.Context) error { return f.ensureErr }

func (f *fakeStore) Insert(_ context.Context, m *Migration) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	f.migs[m.ID] = m
	return nil
}

func (f *fakeStore) Update(_ context.Context, m *Migration) error {
	f.updates++
	if f.updateErr != nil {
		return f.updateErr
	}
	f.migs[m.ID] = m
	return nil
}

func (f *fakeStore) Delete(_ context.Context, id string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.migs, id)
	f.deletes = append(f.deletes, id)
	return nil
}

func (f *fakeStore) GetByRef(_ context.Context, ref string) (*Migration, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if m, ok := f.migs[ref]; ok {
		return m, nil
	}
	for _, m := range f.migs {
		if m.Name != "" && m.Name == ref {
			return m, nil
		}
	}
	return nil, ErrNotFound
}

func (f *fakeStore) List(context.Context) ([]*Migration, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]*Migration, 0, len(f.migs))
	for _, m := range f.migs {
		out = append(out, m)
	}
	return out, nil
}

func (f *fakeStore) put(m *Migration) { f.migs[m.ID] = m }

// fakeDDL is a no-op ddlConn (DDL replication is exercised by ddlrepl_test.go); it
// lets the coordinator's switch/teardown paths run without a real connection.
type fakeDDL struct {
	execErr  error
	countErr error
	count    int64
}

func (d fakeDDL) exec(context.Context, string) error             { return d.execErr }
func (d fakeDDL) execArgs(context.Context, string, ...any) error { return d.execErr }
func (d fakeDDL) queryCount(context.Context, string, ...any) (int64, error) {
	return d.count, d.countErr
}

// fakeTarget is an in-memory migrationTarget.
type fakeTarget struct {
	log *[]string

	applySchemaErr error
	dropTablesErr  error
	createPubErr   error
	dropPubErr     error
	createSubErr   error
	dropSubErr     error
	alterConnErr   error

	status    *SubscriptionStatus
	statusErr error

	subExists     bool
	subExistsErr  error
	pubExists     bool
	pubExistsErr  error
	slotExists    bool
	slotExistsErr error

	currentLSN    string
	currentLSNErr error

	createSlotErr  error
	advanceSlotErr error
	dropSlotErr    error
	waitSlotErr    error
	advanceSeqErr  error

	lag        uint64
	lagSeconds float64
	lagPresent bool
	lagErr     error
}

func (t *fakeTarget) record(name string) {
	if t.log != nil {
		*t.log = append(*t.log, "target."+name)
	}
}

func (t *fakeTarget) ApplySchema(context.Context, string) error {
	t.record("ApplySchema")
	return t.applySchemaErr
}

func (t *fakeTarget) DropTables(context.Context, []string) error {
	t.record("DropTables")
	return t.dropTablesErr
}

func (t *fakeTarget) CreatePublication(context.Context, string, []string, string) error {
	t.record("CreatePublication")
	return t.createPubErr
}

func (t *fakeTarget) DropPublication(context.Context, string) error {
	t.record("DropPublication")
	return t.dropPubErr
}

func (t *fakeTarget) CreateSubscription(context.Context, string, string, string, bool) error {
	t.record("CreateSubscription")
	return t.createSubErr
}

func (t *fakeTarget) DropSubscription(context.Context, string) error {
	t.record("DropSubscription")
	return t.dropSubErr
}

func (t *fakeTarget) AlterSubscriptionConnection(context.Context, string, string) error {
	t.record("AlterSubscriptionConnection")
	return t.alterConnErr
}

func (t *fakeTarget) SubscriptionStatus(context.Context, string) (*SubscriptionStatus, error) {
	if t.statusErr != nil {
		return nil, t.statusErr
	}
	st := t.status
	if st == nil {
		st = &SubscriptionStatus{}
	}
	// Mirror the real target's derivation so tests only set the relation counts.
	st.CaughtUp = st.TotalRelations > 0 && st.ReadyRelations == st.TotalRelations
	return st, nil
}

func (t *fakeTarget) SubscriptionExists(context.Context, string) (bool, error) {
	return t.subExists, t.subExistsErr
}

func (t *fakeTarget) PublicationExists(context.Context, string) (bool, error) {
	return t.pubExists, t.pubExistsErr
}

func (t *fakeTarget) CurrentLSN(context.Context) (string, error) {
	if t.currentLSN == "" {
		return "0/0", t.currentLSNErr
	}
	return t.currentLSN, t.currentLSNErr
}

func (t *fakeTarget) SlotExists(context.Context, string) (bool, error) {
	return t.slotExists, t.slotExistsErr
}

func (t *fakeTarget) CreateLogicalSlot(context.Context, string) error {
	t.record("CreateLogicalSlot")
	return t.createSlotErr
}
func (t *fakeTarget) AdvanceSlot(context.Context, string, string) error { return t.advanceSlotErr }
func (t *fakeTarget) DropLogicalSlot(context.Context, string) error {
	t.record("DropLogicalSlot")
	return t.dropSlotErr
}

func (t *fakeTarget) WaitSlotConfirmed(context.Context, string, string) error { return t.waitSlotErr }

func (t *fakeTarget) ReplicationLag(context.Context, string) (uint64, float64, bool, error) {
	return t.lag, t.lagSeconds, t.lagPresent, t.lagErr
}

func (t *fakeTarget) AdvanceSequences(context.Context, []string, int64) error {
	return t.advanceSeqErr
}
func (t *fakeTarget) ddlConn() ddlConn { return fakeDDL{} }

// fakeSource is an in-memory migrationSource.
type fakeSource struct {
	log *[]string

	info         *SourceInfo
	infoErr      error
	validateInfo *SourceInfo
	resolved     []string
	warnings     []string
	validateErr  error

	dumpSchema string
	dumpErr    error

	createPubErr  error
	dropPubErr    error
	createSubErr  error
	dropSubErr    error
	setROErr      error
	currentLSNErr error
	currentLSN    string

	lag        uint64
	lagSeconds float64
	lagPresent bool
	lagErr     error

	subExists    bool
	subExistsErr error
	pubExists    bool
	pubExistsErr error

	waitSlotErr error
	advSeqErr   error

	closed int
}

func (s *fakeSource) record(name string) {
	if s.log != nil {
		*s.log = append(*s.log, "source."+name)
	}
}

func (s *fakeSource) Validate([]string) (*SourceInfo, []string, []string, error) {
	if s.validateErr != nil {
		return nil, nil, nil, s.validateErr
	}
	info := s.validateInfo
	if info == nil {
		info = &SourceInfo{ServerVersionNum: 170000, CanCreateSubscription: true}
	}
	res := s.resolved
	if res == nil {
		res = []string{"public.orders"}
	}
	return info, res, s.warnings, nil
}

func (s *fakeSource) Info() (*SourceInfo, error) {
	if s.infoErr != nil {
		return nil, s.infoErr
	}
	if s.info != nil {
		return s.info, nil
	}
	return &SourceInfo{ServerVersionNum: 170000, CanCreateSubscription: true}, nil
}
func (s *fakeSource) DumpSchema([]string) (string, error) { return s.dumpSchema, s.dumpErr }
func (s *fakeSource) CreatePublication(string, []string, string) error {
	s.record("CreatePublication")
	return s.createPubErr
}

func (s *fakeSource) DropPublication(string) error {
	s.record("DropPublication")
	return s.dropPubErr
}

func (s *fakeSource) CreateSubscription(string, string, string, bool, string) error {
	s.record("CreateSubscription")
	return s.createSubErr
}

func (s *fakeSource) DropSubscription(string) error {
	s.record("DropSubscription")
	return s.dropSubErr
}
func (s *fakeSource) SetReadOnly(bool) error { return s.setROErr }
func (s *fakeSource) CurrentLSN() (string, error) {
	if s.currentLSN == "" {
		return "0/0", s.currentLSNErr
	}
	return s.currentLSN, s.currentLSNErr
}

func (s *fakeSource) ReplicationLag(string) (uint64, float64, bool, error) {
	return s.lag, s.lagSeconds, s.lagPresent, s.lagErr
}

func (s *fakeSource) SubscriptionExists(string) (bool, error) {
	return s.subExists, s.subExistsErr
}

func (s *fakeSource) PublicationExists(string) (bool, error) {
	return s.pubExists, s.pubExistsErr
}
func (s *fakeSource) WaitSlotConfirmed(string, string) error { return s.waitSlotErr }
func (s *fakeSource) AdvanceSequences([]string, int64) error { return s.advSeqErr }
func (s *fakeSource) ddlConn() ddlConn                       { return fakeDDL{} }
func (s *fakeSource) close()                                 { s.closed++ }

// testCoord bundles a Coordinator wired to fakes plus handles to those fakes.
type testCoord struct {
	c     *Coordinator
	store *fakeStore
	tgt   *fakeTarget
	src   *fakeSource
	log   []string

	// srcErr, when set, makes the source factory fail (source unreachable).
	srcErr error
}

// newTestCoord builds a Coordinator backed by fakes. targetConnInfo returns a
// usable conninfo so the EXPORT precondition passes by default; individual tests
// override fake fields to steer specific branches.
func newTestCoord(t *testing.T) *testCoord {
	t.Helper()
	tc := &testCoord{store: newFakeStore()}
	tc.tgt = &fakeTarget{log: &tc.log}
	tc.src = &fakeSource{log: &tc.log}
	c := &Coordinator{
		store:            tc.store,
		target:           tc.tgt,
		logger:           slog.New(slog.DiscardHandler),
		targetConnInfo:   func(string) (string, error) { return "host=target dbname=app", nil },
		drainForImport:   func(context.Context) error { return nil },
		releaseForExport: func(context.Context) error { return nil },
		now:              func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
	c.newSource = func(context.Context, string) (migrationSource, error) {
		if tc.srcErr != nil {
			return nil, tc.srcErr
		}
		return tc.src, nil
	}
	tc.c = c
	return tc
}

// seed inserts a migration directly into the fake store (bypassing CreateMigration)
// so a test can start from any phase — including the transient COMPLETING /
// SWITCHING_TO_* phases a crash would leave behind.
func (tc *testCoord) seed(m *Migration) *Migration {
	if m.ID == "" {
		m.ID = "m1"
	}
	if m.SourceDSN == "" {
		m.SourceDSN = "host=src dbname=app"
	}
	if len(m.Tables) == 0 {
		m.Tables = []string{"public.orders"}
	}
	tc.store.put(m)
	return m
}
