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

package manager

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// managerWithAdvertiseConfig builds a minimal manager for exercising
// targetConnInfo: a record whose topo Hostname is a cluster-internal pod FQDN
// (the address the EXPORT reverse subscription must NOT advertise) plus the
// migration advertise config and slot-feature getter under test. connPoolMgr is
// left nil, so PgUser falls back to the default superuser and the password is
// empty — enough to assert the host/port/user the conninfo advertises.
func managerWithAdvertiseConfig(advertiseHost string, advertisePort int, slotEnabled bool) *MultipoolerManager {
	record := newRecordFromProto(&clustermetadatapb.Multipooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-1"},
		Hostname: "multipooler-zone1-0.multipooler-zone1.default.svc.cluster.local",
		PortMap:  map[string]int32{"postgres": 6432},
	})
	return &MultipoolerManager{
		logger: newTestLogger(),
		record: record,
		config: &Config{
			MigrationTargetAdvertiseHost: advertiseHost,
			MigrationTargetAdvertisePort: advertisePort,
			SlotBasedReplicationEnabled:  func() bool { return slotEnabled },
		},
	}
}

func TestTargetConnInfo_AdvertisesGatewayAddress(t *testing.T) {
	pm := managerWithAdvertiseConfig("gateway.example.com", 5433, true)

	conninfo, err := pm.targetConnInfo("appdb")
	require.NoError(t, err)

	// Advertises the configured gateway address, not the pooler's topo Hostname.
	// Values are quoted (host/user/password/dbname): see
	// TestTargetConnInfo_QuotesValuesAgainstInjection for why.
	assert.Contains(t, conninfo, "host='gateway.example.com'")
	assert.Contains(t, conninfo, "port=5433")
	assert.Contains(t, conninfo, "user='postgres'")
	assert.Contains(t, conninfo, "dbname='appdb'")
	assert.NotContains(t, conninfo, pm.record.Hostname())
	assert.NotContains(t, conninfo, "6432") // never the pooler's own postgres port
}

func TestTargetConnInfo_ZeroPortUsesGatewayDefault(t *testing.T) {
	pm := managerWithAdvertiseConfig("gateway.example.com", 0, true)

	conninfo, err := pm.targetConnInfo("appdb")
	require.NoError(t, err)
	assert.Contains(t, conninfo, fmt.Sprintf("port=%d", defaultGatewayPostgresPort))
}

// TestTargetConnInfo_QuotesValuesAgainstInjection is the regression test for
// the finding this guards against: TargetDatabase comes from the migration
// row (Migration.TargetDatabase), which an unauthenticated multiadmin caller
// controls (see CreateMigrationRequest). Interpolated unquoted, a database
// name containing a space could inject an extra keyword=value pair — e.g.
// redirecting the whole conninfo (which also carries the real target
// superuser password) to an attacker-controlled host.
func TestTargetConnInfo_QuotesValuesAgainstInjection(t *testing.T) {
	pm := managerWithAdvertiseConfig("gateway.example.com", 5433, true)

	conninfo, err := pm.targetConnInfo("appdb host=attacker.org sslmode=disable")
	require.NoError(t, err)

	// Parse with a real libpq-conninfo parser (not substring matching, which
	// can't distinguish "appears in the text" from "parsed as its own
	// keyword=value pair"): the injected host=attacker.org must land inside
	// dbname's value, never override the real, trusted host.
	cfg, err := pgx.ParseConfig(conninfo)
	require.NoError(t, err)
	assert.Equal(t, "gateway.example.com", cfg.Host, "the real gateway host must win, not the one injected via dbname")
	assert.Equal(t, "appdb host=attacker.org sslmode=disable", cfg.Database, "the injection attempt must parse as part of dbname's literal value")
}

func TestTargetConnInfo_UnsetHostFailsFast(t *testing.T) {
	pm := managerWithAdvertiseConfig("", 0, true)

	_, err := pm.targetConnInfo("appdb")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--migration-target-advertise-host")
}

func TestTargetConnInfo_SlotFeatureOffFailsFast(t *testing.T) {
	pm := managerWithAdvertiseConfig("gateway.example.com", 5432, false)

	_, err := pm.targetConnInfo("appdb")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--enable-slot-based-replication")
}

// TestRefreshMigrationHold_UndefinedTableClearsHold covers the common case for
// most poolers: the migrator has never been used on this shard, so
// multigres.migration doesn't exist. That must resolve to not-held, not hold
// it hostage forever.
func TestRefreshMigrationHold_UndefinedTableClearsHold(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, "default", "0")
	pm.migrationImportHold.Store(int32(migrationHoldHeld)) // simulate a stale held value

	qs.AddQueryPatternWithError("SELECT count\\(\\*\\) FROM multigres.migration",
		&mterrors.PgDiagnostic{Code: mterrors.PgSSUndefinedTable, Message: `relation "multigres.migration" does not exist`})

	pm.refreshMigrationHold(t.Context())

	assert.Equal(t, migrationHoldNotHeld, migrationHoldState(pm.migrationImportHold.Load()), "undefined_table must resolve to not held")
}

// TestRefreshMigrationHold_OtherErrorFailsClosed covers the finding this test
// guards against: a transient or unrelated read failure (not "table doesn't
// exist") must NOT resolve the hold either way — it leaves whatever state was
// there (including migrationHoldUnknown), since the real migration state is
// unknown without a successful read. refreshMigrationHold runs on every tick
// for every pooler, so treating every error as "release the hold" would let a
// DRAINING pooler reconcile back to SERVING on a mere blip during an active or
// failed migration; treating every error as "always hold" (the naively
// "safe-looking" fix) would instead permanently wedge every non-migrating
// pooler, since for them the table genuinely never exists and every tick would
// see an error.
func TestRefreshMigrationHold_OtherErrorFailsClosed(t *testing.T) {
	otherErr := errors.New("connection reset by peer")

	for _, state := range []migrationHoldState{migrationHoldUnknown, migrationHoldNotHeld, migrationHoldHeld} {
		t.Run(state.String(), func(t *testing.T) {
			pm, qs := newTestManagerWithMock(t, "default", "0")
			pm.migrationImportHold.Store(int32(state))
			qs.AddQueryPatternWithError("SELECT count\\(\\*\\) FROM multigres.migration", otherErr)

			pm.refreshMigrationHold(t.Context())

			assert.Equal(t, state, migrationHoldState(pm.migrationImportHold.Load()), "an unrelated read failure must not change the hold state")
		})
	}
}

// TestRefreshMigrationHold_NilQueryServiceFailsClosed covers the finding this
// guards against: connection pools not yet open (startup) or otherwise
// unavailable is the same kind of ambiguous read failure
// TestRefreshMigrationHold_OtherErrorFailsClosed covers for a failed query —
// there is no successful read to tell a genuinely-held migration from an
// unreadable table, so the hold must stay at its last known state, not resolve.
func TestRefreshMigrationHold_NilQueryServiceFailsClosed(t *testing.T) {
	for _, state := range []migrationHoldState{migrationHoldUnknown, migrationHoldNotHeld, migrationHoldHeld} {
		t.Run(state.String(), func(t *testing.T) {
			pm, _ := newTestManagerWithMock(t, "default", "0")
			pm.qsc = nil
			pm.migrationImportHold.Store(int32(state))

			pm.refreshMigrationHold(t.Context())

			assert.Equal(t, state, migrationHoldState(pm.migrationImportHold.Load()), "a nil query service must not change the hold state")
		})
	}
}

// TestRefreshMigrationHold_UndecodableResultFailsClosed covers the CRITICAL
// finding this guards against: a successful QueryAdmin that still returns a
// result ScanSingleRow cannot decode (e.g. zero rows) is just as ambiguous as
// the QueryAdmin-error case TestRefreshMigrationHold_OtherErrorFailsClosed
// covers above, and per this function's own docstring must be treated the
// same way — leave the hold at its last known state, not resolve it. Resolving
// it to not-held here would let a DRAINING pooler reconcile back to SERVING
// while an IMPORT or FAILED migration still holds incomplete data.
func TestRefreshMigrationHold_UndecodableResultFailsClosed(t *testing.T) {
	for _, state := range []migrationHoldState{migrationHoldUnknown, migrationHoldNotHeld, migrationHoldHeld} {
		t.Run(state.String(), func(t *testing.T) {
			pm, qs := newTestManagerWithMock(t, "default", "0")
			pm.migrationImportHold.Store(int32(state))
			qs.AddQueryPattern("SELECT count\\(\\*\\) FROM multigres.migration", mock.MakeQueryResult(nil, nil))

			pm.refreshMigrationHold(t.Context())

			assert.Equal(t, state, migrationHoldState(pm.migrationImportHold.Load()), "an undecodable result must not change the hold state")
		})
	}
}

// TestResolveMigrationHoldBeforeServing_RecoversFromTransientFailure covers the
// scenario resolveMigrationHoldBeforeServing exists for: a restart that lands
// mid-IMPORT, whose very first refreshMigrationHold read fails transiently.
// The bounded retry must give the read a second chance and correctly resolve
// to held, rather than leaving the gate unresolved (and so, via
// migrationServingHold, silently open) for the rest of the interval until the
// next postgres-monitor tick.
func TestResolveMigrationHoldBeforeServing_RecoversFromTransientFailure(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, "default", "0")
	require.Equal(t, migrationHoldUnknown, migrationHoldState(pm.migrationImportHold.Load()), "a fresh manager must start unknown, not not-held")

	qs.AddQueryPatternOnceWithError("SELECT count\\(\\*\\) FROM multigres.migration", errors.New("connection reset by peer"))
	qs.AddQueryPattern("SELECT count\\(\\*\\) FROM multigres.migration", mock.MakeQueryResult([]string{"count"}, [][]any{{int64(1)}}))

	pm.resolveMigrationHoldBeforeServing(t.Context())

	assert.Equal(t, migrationHoldHeld, migrationHoldState(pm.migrationImportHold.Load()), "a retry succeeding with an active migration must resolve to held")
	assert.True(t, pm.migrationServingHold(), "a resolved held state must withhold serving")
}

// TestResolveMigrationHoldBeforeServing_FallsThroughWhenNeverResolves covers
// the bound: if every attempt within the (tiny) timeout fails, this must give
// up and leave the gate unresolved, not block bootstrap. An unresolved gate is
// read as not-held by migrationServingHold, same as it would be without this
// function existing at all — no worse than before. The bound itself is small
// enough that even this worst case costs single-digit milliseconds, not
// seconds, so it doesn't tax every reopen in the unit test suite.
func TestResolveMigrationHoldBeforeServing_FallsThroughWhenNeverResolves(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, "default", "0")
	qs.AddQueryPatternWithError("SELECT count\\(\\*\\) FROM multigres.migration", errors.New("connection refused"))

	start := time.Now()
	pm.resolveMigrationHoldBeforeServing(t.Context())
	elapsed := time.Since(start)

	assert.Equal(t, migrationHoldUnknown, migrationHoldState(pm.migrationImportHold.Load()), "exhausting every attempt must leave the gate unresolved, not force a guess")
	assert.False(t, pm.migrationServingHold(), "an unresolved gate must read as not-held, matching pre-existing behavior")
	assert.LessOrEqual(t, elapsed, migrationHoldResolveTimeout+50*time.Millisecond, "must give up within its tiny bound, not hang bootstrap")
}

// TestResolveMigrationHoldBeforeServing_OrdinaryBootstrapResolvesImmediately
// covers the common case the earlier plain-bool-default fix regressed: a
// pooler that has never run a migration resolves on the very first attempt
// (undefined table), well before the retry bound, so ordinary bootstrap is not
// slowed down waiting on a gate that was never going to be held.
func TestResolveMigrationHoldBeforeServing_OrdinaryBootstrapResolvesImmediately(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, "default", "0")
	qs.AddQueryPatternWithError("SELECT count\\(\\*\\) FROM multigres.migration",
		&mterrors.PgDiagnostic{Code: mterrors.PgSSUndefinedTable, Message: `relation "multigres.migration" does not exist`})

	start := time.Now()
	pm.resolveMigrationHoldBeforeServing(t.Context())
	elapsed := time.Since(start)

	assert.Equal(t, migrationHoldNotHeld, migrationHoldState(pm.migrationImportHold.Load()))
	assert.False(t, pm.migrationServingHold())
	assert.Less(t, elapsed, migrationHoldResolveBaseWait, "the first attempt must resolve it; no retry wait should be needed")
}

// TestResolveMigrationHoldBeforeServing_NoQueryServiceReturnsImmediately covers
// the fast path: no query service means connections are not open yet (or, in
// a test harness, never configured at all), which is not a transient query
// failure worth retrying — retrying here would just busy-wait for the whole
// bound. This must return immediately, leaving the gate unresolved for the
// next postgres-monitor tick to pick up once connections do exist.
func TestResolveMigrationHoldBeforeServing_NoQueryServiceReturnsImmediately(t *testing.T) {
	pm, _ := newTestManagerWithMock(t, "default", "0")
	pm.qsc = nil

	start := time.Now()
	pm.resolveMigrationHoldBeforeServing(t.Context())
	elapsed := time.Since(start)

	assert.Equal(t, migrationHoldUnknown, migrationHoldState(pm.migrationImportHold.Load()))
	assert.Less(t, elapsed, migrationHoldResolveBaseWait, "a nil query service must not trigger any retry wait")
}
