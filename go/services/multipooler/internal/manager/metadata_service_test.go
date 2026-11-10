// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

func requireMetadataCode(t *testing.T, want mtrpcpb.Code, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, want, mterrors.Code(err), "%v", err)
}

type fakeDirectory struct {
	conn  *multipoolerservicepb.BackingConnection
	found bool
	err   error
	asked int64
}

func (d *fakeDirectory) TableGroupBinding(_ context.Context, tablegroupID int64) (*multipoolerservicepb.BackingConnection, bool, error) {
	d.asked = tablegroupID
	return d.conn, d.found, d.err
}

func newMetadataManager(t *testing.T, poolerType clustermetadatapb.PoolerType, tablegroup string) *MultipoolerManager {
	t.Helper()
	initial := newTestMultipooler(poolerType, clustermetadatapb.PoolerServingStatus_SERVING)
	initial.ShardRef = &clustermetadatapb.ShardRef{Database: "db", TableGroup: tablegroup, Shard: constants.DefaultShard}
	cfg := &Config{}
	if tablegroup != constants.DefaultTableGroup {
		// A pooler of another tablegroup is an unmanaged one.
		initial.LeadershipMode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED
		cfg.ExternalBackend = &ExternalBackend{Host: "db.example.com", Port: 5432, Database: "db"}
	}
	pm, err := NewMultipoolerManager(newTestLogger(), initial, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		pm.cancel()
		pm.shutdownCancel()
	})
	return pm
}

// TestTableGroupBindingComesFromTheDirectory: what the default primary answers comes from the
// directory, by tablegroup id: the connection of an external tablegroup, or that it is managed.
func TestTableGroupBindingComesFromTheDirectory(t *testing.T) {
	pm := newMetadataManager(t, clustermetadatapb.PoolerType_REPLICA, constants.DefaultTableGroup)

	dir := &fakeDirectory{conn: &multipoolerservicepb.BackingConnection{Name: "src", Hosts: []string{"h"}, Ports: []int32{5432}, Database: "db", User: "u"}, found: true}
	pm.connectionDir = dir
	resp, err := pm.tableGroupBinding(t.Context(), 7)
	require.NoError(t, err)
	require.EqualValues(t, 7, dir.asked)
	require.Equal(t, "src", resp.GetConnection().GetName())
	require.Equal(t, []string{"h"}, resp.GetConnection().GetHosts())
	require.False(t, resp.GetManaged())

	// A managed tablegroup is said to be managed, with no connection.
	dir.conn = nil
	resp, err = pm.tableGroupBinding(t.Context(), 7)
	require.NoError(t, err)
	require.True(t, resp.GetManaged())
	require.Nil(t, resp.GetConnection())

	dir.found = false
	_, err = pm.tableGroupBinding(t.Context(), 7)
	requireMetadataCode(t, mtrpcpb.Code_NOT_FOUND, err)

	dir.err = errors.New("directory down")
	_, err = pm.tableGroupBinding(t.Context(), 7)
	require.ErrorContains(t, err, "directory down")

	// A refusal by the directory keeps its code through the manager.
	dir.err = errUnusableConnection("it names more than one server")
	_, err = pm.tableGroupBinding(t.Context(), 7)
	requireMetadataCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)

	for _, id := range []int64{0, -1} {
		_, err = pm.tableGroupBinding(t.Context(), id)
		requireMetadataCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	}
}

// TestMetadataIsServedOnlyByTheDefaultPrimary: a replica, and a pooler of another tablegroup,
// answer UNAVAILABLE, which a caller retries after rediscovering the default primary; a
// request for another database is INVALID_ARGUMENT.
func TestMetadataIsServedOnlyByTheDefaultPrimary(t *testing.T) {
	req := &multipoolerservicepb.GetBackingConnectionRequest{Database: "db", TablegroupId: 7}
	_, err := newMetadataManager(t, clustermetadatapb.PoolerType_REPLICA, constants.DefaultTableGroup).GetBackingConnection(t.Context(), req)
	requireMetadataCode(t, mtrpcpb.Code_UNAVAILABLE, err)
	_, err = newMetadataManager(t, clustermetadatapb.PoolerType_REPLICA, "migrateTG").GetBackingConnection(t.Context(), req)
	requireMetadataCode(t, mtrpcpb.Code_UNAVAILABLE, err)
	_, err = newMetadataManager(t, clustermetadatapb.PoolerType_REPLICA, constants.DefaultTableGroup).GetBackingConnection(t.Context(), &multipoolerservicepb.GetBackingConnectionRequest{Database: "other", TablegroupId: 7})
	requireMetadataCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
}
