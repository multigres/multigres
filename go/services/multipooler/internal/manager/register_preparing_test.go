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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// preparingRecord is the SERVING record of an unmanaged pooler.
func preparingRecord() *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "ext"},
		Hostname:       "localhost",
		PortMap:        map[string]int32{"grpc": 1},
		ShardRef:       &clustermetadatapb.ShardRef{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"},
		LeadershipMode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED,
		ServingStatus:  clustermetadatapb.PoolerServingStatus_SERVING,
	}
}

func TestRegisterPreparingReplacesItsOwnStaleEntry(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()

	// A previous process of the same pooler left a SERVING entry.
	require.NoError(t, ts.CreateMultipooler(t.Context(), preparingRecord()))
	require.NoError(t, RegisterPreparing(t.Context(), ts, preparingRecord()))

	info, err := ts.GetMultipooler(t.Context(), preparingRecord().Id)
	require.NoError(t, err)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, info.GetServingStatus(), "a restart must not leave a stale SERVING entry")
}
