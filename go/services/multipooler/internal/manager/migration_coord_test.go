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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
	assert.Contains(t, conninfo, "host=gateway.example.com")
	assert.Contains(t, conninfo, "port=5433")
	assert.Contains(t, conninfo, "user=postgres")
	assert.Contains(t, conninfo, "dbname=appdb")
	assert.NotContains(t, conninfo, pm.record.Hostname())
	assert.NotContains(t, conninfo, "6432") // never the pooler's own postgres port
}

func TestTargetConnInfo_ZeroPortUsesGatewayDefault(t *testing.T) {
	pm := managerWithAdvertiseConfig("gateway.example.com", 0, true)

	conninfo, err := pm.targetConnInfo("appdb")
	require.NoError(t, err)
	assert.Contains(t, conninfo, fmt.Sprintf("port=%d", defaultGatewayPostgresPort))
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
// multigres.migration doesn't exist. That must clear the hold, not hold it
// hostage forever.
func TestRefreshMigrationHold_UndefinedTableClearsHold(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, "default", "0")
	pm.migrationImportHold.Store(true) // simulate a stale "held" value

	qs.AddQueryPatternWithError("SELECT count\\(\\*\\) FROM multigres.migration",
		&mterrors.PgDiagnostic{Code: mterrors.PgSSUndefinedTable, Message: `relation "multigres.migration" does not exist`})

	pm.refreshMigrationHold(t.Context())

	assert.False(t, pm.migrationImportHold.Load(), "undefined_table must clear the hold")
}

// TestRefreshMigrationHold_OtherErrorFailsClosed covers the finding this test
// guards against: a transient or unrelated read failure (not "table doesn't
// exist") must NOT clear the hold, in either direction — it leaves whatever
// was there, since the real migration state is unknown without a successful
// read. refreshMigrationHold runs on every tick for every pooler, so treating
// every error as "release the hold" would let a DRAINING pooler reconcile back
// to SERVING on a mere blip during an active or failed migration; treating
// every error as "always hold" (the naively "safe-looking" fix) would instead
// permanently wedge every non-migrating pooler, since for them the table
// genuinely never exists and every tick would see an error.
func TestRefreshMigrationHold_OtherErrorFailsClosed(t *testing.T) {
	otherErr := errors.New("connection reset by peer")

	t.Run("previously held, stays held", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, "default", "0")
		pm.migrationImportHold.Store(true)
		qs.AddQueryPatternWithError("SELECT count\\(\\*\\) FROM multigres.migration", otherErr)

		pm.refreshMigrationHold(t.Context())

		assert.True(t, pm.migrationImportHold.Load(), "an unrelated read failure must not release a real hold")
	})

	t.Run("previously clear, stays clear", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, "default", "0")
		pm.migrationImportHold.Store(false)
		qs.AddQueryPatternWithError("SELECT count\\(\\*\\) FROM multigres.migration", otherErr)

		pm.refreshMigrationHold(t.Context())

		assert.False(t, pm.migrationImportHold.Load(), "a non-migrating pooler must not be wedged by a transient read failure")
	})
}
