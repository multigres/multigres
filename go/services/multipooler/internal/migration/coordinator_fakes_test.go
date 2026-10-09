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

// testMigID is the default numeric id assigned to seeded migrations, and the id
// tests address them by (Ref{ID: testMigID}).
const testMigID int64 = 1

// fakeStore is an in-memory migrationStore.
type fakeStore struct {
	migs  map[int64]*Migration
	conns map[int64]*Connection

	ensureErr        error
	insertErr        error
	updateErr        error
	deleteErr        error
	getErr           error
	listErr          error
	insertJournalErr error

	updates int
	deletes []int64
	// journal accumulates every appended entry across all migrations, in append
	// order, so tests can assert the lifecycle sequence and retention after a drop.
	journal []*JournalEntry
}

func newFakeStore() *fakeStore {
	return &fakeStore{migs: map[int64]*Migration{}, conns: map[int64]*Connection{}}
}

func (f *fakeStore) InsertConnection(_ context.Context, c *Connection) error {
	f.conns[c.ID] = c
	return nil
}

func (f *fakeStore) DeleteConnection(_ context.Context, id int64) error {
	delete(f.conns, id)
	return nil
}

func (f *fakeStore) GetConnectionByRef(_ context.Context, ref Ref) (*Connection, error) {
	if ref.ID != 0 {
		if c, ok := f.conns[ref.ID]; ok {
			return c, nil
		}
	} else if ref.Name != "" {
		for _, c := range f.conns {
			if c.Name == ref.Name {
				return c, nil
			}
		}
	}
	return nil, ErrConnectionNotFound
}

func (f *fakeStore) ListConnections(context.Context) ([]*Connection, error) {
	out := make([]*Connection, 0, len(f.conns))
	for _, c := range f.conns {
		out = append(out, c)
	}
	return out, nil
}

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

func (f *fakeStore) Delete(_ context.Context, id int64) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.migs, id)
	f.deletes = append(f.deletes, id)
	return nil
}

func (f *fakeStore) GetByRef(_ context.Context, ref Ref) (*Migration, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if ref.ID != 0 {
		if m, ok := f.migs[ref.ID]; ok {
			return m, nil
		}
	} else if ref.Name != "" {
		for _, m := range f.migs {
			if m.Name == ref.Name {
				return m, nil
			}
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

func (f *fakeStore) InsertJournal(_ context.Context, e *JournalEntry) error {
	if f.insertJournalErr != nil {
		return f.insertJournalErr
	}
	cp := *e
	cp.Seq = int64(len(f.journal) + 1)
	f.journal = append(f.journal, &cp)
	return nil
}

func (f *fakeStore) ListJournal(_ context.Context, migrationID int64) ([]*JournalEntry, error) {
	out := make([]*JournalEntry, 0)
	for _, e := range f.journal {
		if e.MigrationID == migrationID {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out, nil
}

// journalEvents returns the event sequence appended for one migration, in order.
func (f *fakeStore) journalEvents(migrationID int64) []JournalEvent {
	var out []JournalEvent
	for _, e := range f.journal {
		if e.MigrationID == migrationID {
			out = append(out, e.Event)
		}
	}
	return out
}

func (f *fakeStore) put(m *Migration) { f.migs[m.ID] = m }

// fakeTarget is an in-memory migrationTarget.
type fakeTarget struct {
	log *[]string

	applySchemaErr          error
	disableTriggersErr      error
	dropCheckConstraintsErr error
	disableRulesErr         error
	checkDropPrivilegeErr   error
	lastCallerRole          string
	dropTablesErr           error
	createPubErr            error
	dropPubErr              error
	createSubErr            error
	dropSubErr              error

	status    *SubscriptionStatus
	statusErr error

	subExists     bool
	subExistsErr  error
	pubExists     bool
	pubExistsErr  error
	slotExists    bool
	slotExistsErr error
	slotReady     bool
	slotReadyErr  error

	currentLSN    string
	currentLSNErr error

	createSlotErr  error
	advanceSlotErr error
	dropSlotErr    error
	waitSlotErr    error
	// waitSlotBlocks makes WaitSlotConfirmed block until ctx is done and return
	// ctx.Err(), simulating a slot that never confirms (e.g. lost in a failover) —
	// the only thing that stops a real drain in that case is the caller's deadline.
	waitSlotBlocks bool
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

func (t *fakeTarget) DisableUserTriggers(context.Context, []string) error {
	t.record("DisableUserTriggers")
	return t.disableTriggersErr
}

func (t *fakeTarget) DropUserCheckConstraints(context.Context, []string) error {
	t.record("DropUserCheckConstraints")
	return t.dropCheckConstraintsErr
}

func (t *fakeTarget) DisableUserRewriteRules(context.Context, []string) error {
	t.record("DisableUserRewriteRules")
	return t.disableRulesErr
}

func (t *fakeTarget) CheckDropPrivilege(_ context.Context, _ []string, callerRole string) error {
	t.record("CheckDropPrivilege")
	t.lastCallerRole = callerRole
	return t.checkDropPrivilegeErr
}

func (t *fakeTarget) DropTables(context.Context, []string) error {
	t.record("DropTables")
	return t.dropTablesErr
}

func (t *fakeTarget) CreatePublication(context.Context, string, []string) error {
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

func (t *fakeTarget) SlotReady(context.Context, string) (bool, error) {
	return t.slotReady, t.slotReadyErr
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

func (t *fakeTarget) WaitSlotConfirmed(ctx context.Context, _, _ string) error {
	if t.waitSlotBlocks {
		<-ctx.Done()
		return ctx.Err()
	}
	return t.waitSlotErr
}

func (t *fakeTarget) ReplicationLag(context.Context, string) (uint64, float64, bool, error) {
	return t.lag, t.lagSeconds, t.lagPresent, t.lagErr
}

func (t *fakeTarget) AdvanceSequences(context.Context, []string, int64) error {
	return t.advanceSeqErr
}

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

	createPubErr    error
	dropPubErr      error
	createSubErr    error
	dropSubErr      error
	setROErr        error
	setSessionROErr error
	currentLSNErr   error
	currentLSN      string

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

	terminateErr error

	revokeConnectErr error

	grantConnectErr error
	// grantConnectRestorePublic captures the restorePublic argument of the last
	// GrantConnect call, so tests can assert the coordinator threaded
	// Migration.PublicHadConnect through correctly.
	grantConnectRestorePublic bool
	// grantConnectLimits captures the connLimits argument of the last
	// GrantConnect call, so tests can assert the coordinator threaded
	// Migration.QuiesceRoleConnLimits through correctly.
	grantConnectLimits map[string]int32

	publicHasConnect    bool
	publicHasConnectErr error
	roleConnLimits      map[string]int32
	roleConnLimitsErr   error
	checkRolesErr       error

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
func (s *fakeSource) CreatePublication(string, []string) error {
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

func (s *fakeSource) SetReadOnly(ro bool) error {
	if ro {
		s.record("SetReadOnly(true)")
	} else {
		s.record("SetReadOnly(false)")
	}
	return s.setROErr
}

func (s *fakeSource) setSessionReadOnly(ro bool) error {
	if ro {
		s.record("setSessionReadOnly(true)")
	} else {
		s.record("setSessionReadOnly(false)")
	}
	return s.setSessionROErr
}

func (s *fakeSource) TerminateClientBackends() error {
	s.record("TerminateClientBackends")
	return s.terminateErr
}

func (s *fakeSource) RevokeConnect(roles []string) error {
	s.record("RevokeConnect")
	return s.revokeConnectErr
}

func (s *fakeSource) GrantConnect(roles []string, restorePublic bool, connLimits map[string]int32) error {
	if len(roles) > 0 || restorePublic {
		s.record("GrantConnect")
	}
	s.grantConnectRestorePublic = restorePublic
	s.grantConnectLimits = connLimits
	return s.grantConnectErr
}

func (s *fakeSource) RoleConnLimits(roles []string) (map[string]int32, error) {
	s.record("RoleConnLimits")
	return s.roleConnLimits, s.roleConnLimitsErr
}

func (s *fakeSource) PublicHasConnect() (bool, error) {
	s.record("PublicHasConnect")
	return s.publicHasConnect, s.publicHasConnectErr
}
func (s *fakeSource) checkQuiesceRoles([]string) error { return s.checkRolesErr }
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
func (s *fakeSource) AdvanceSequences([]string, int64) error {
	s.record("AdvanceSequences")
	return s.advSeqErr
}
func (s *fakeSource) close() { s.closed++ }

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

// defaultConnID is the id of the connection newTestCoord seeds by default
// (name "src", DSN "host=src dbname=app") — what seed() points an unset
// Migration.ConnectionID at, and what CreateParams{ConnectionName: "src"}
// resolves to in tests that don't need a distinct source.
const defaultConnID int64 = 1

// newTestCoord builds a Coordinator backed by fakes. targetConnInfo returns a
// usable conninfo so the EXPORT precondition passes by default; individual tests
// override fake fields to steer specific branches.
func newTestCoord(t *testing.T) *testCoord {
	t.Helper()
	tc := &testCoord{store: newFakeStore()}
	// slotReady defaults to healthy: most tests exercise applySwitch/Activate/
	// Deactivate paths that incidentally go through ensureReverseExportLink, not
	// the reverse-link repair itself (which has its own dedicated tests below
	// that explicitly set slotReady/subExists to exercise the degraded path).
	tc.tgt = &fakeTarget{log: &tc.log, slotReady: true}
	tc.src = &fakeSource{log: &tc.log}
	tc.store.conns[defaultConnID] = &Connection{ID: defaultConnID, Name: "src", DSN: "host=src dbname=app"}
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
	if m.ID == 0 {
		m.ID = testMigID
	}
	if m.ConnectionID == 0 {
		m.ConnectionID = defaultConnID
	}
	if len(m.Tables) == 0 {
		m.Tables = []string{"public.orders"}
	}
	tc.store.put(m)
	return m
}
