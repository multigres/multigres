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

package topoclient_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

type sharedTopologyFactory struct {
	*memorytopo.Factory
}

func (f sharedTopologyFactory) Create(_ string, root string, serverAddrs []string) (topoclient.Conn, error) {
	return f.Factory.Create(topoclient.GlobalCell, root, serverAddrs)
}

func TestGetComponentsByCell_SharedTopology(t *testing.T) {
	ctx := t.Context()
	backingStore, factory := memorytopo.NewServerAndFactory(ctx)
	defer backingStore.Close()

	const root = "/multigres/global"
	ts := topoclient.NewWithFactory(sharedTopologyFactory{factory}, root, []string{"global-topo:2379"}, topoclient.NewDefaultTopoConfig())
	defer ts.Close()

	cells := []string{"zone-a", "zone-b"}
	for _, cell := range cells {
		require.NoError(t, ts.CreateCell(ctx, cell, &clustermetadatapb.Cell{
			Name:            cell,
			ServerAddresses: []string{"global-topo:2379"},
			Root:            root,
		}))
		require.NoError(t, ts.CreateMultipooler(ctx, topoclient.NewMultipooler("pooler", cell, "host")))
		require.NoError(t, ts.CreateMultigateway(ctx, topoclient.NewMultigateway("gateway", cell, "host")))
		require.NoError(t, ts.CreateMultiorch(ctx, topoclient.NewMultiorch("orch", cell, "host")))
	}

	for _, cell := range cells {
		poolers, err := ts.GetMultipoolersByCell(ctx, cell, nil)
		require.NoError(t, err)
		require.Len(t, poolers, 1)
		require.Equal(t, cell, poolers[0].GetId().GetCell())
		poolerIDs, err := ts.GetMultipoolerIDsByCell(ctx, cell)
		require.NoError(t, err)
		require.Len(t, poolerIDs, 1)
		require.Equal(t, cell, poolerIDs[0].GetCell())

		gateways, err := ts.GetMultigatewaysByCell(ctx, cell)
		require.NoError(t, err)
		require.Len(t, gateways, 1)
		require.Equal(t, cell, gateways[0].GetId().GetCell())
		gatewayIDs, err := ts.GetMultigatewayIDsByCell(ctx, cell)
		require.NoError(t, err)
		require.Len(t, gatewayIDs, 1)
		require.Equal(t, cell, gatewayIDs[0].GetCell())

		orchs, err := ts.GetMultiorchsByCell(ctx, cell)
		require.NoError(t, err)
		require.Len(t, orchs, 1)
		require.Equal(t, cell, orchs[0].GetId().GetCell())
		orchIDs, err := ts.GetMultiorchIDsByCell(ctx, cell)
		require.NoError(t, err)
		require.Len(t, orchIDs, 1)
		require.Equal(t, cell, orchIDs[0].GetCell())
	}
}
