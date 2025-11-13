// Copyright 2025 Supabase, Inc.
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

package coordinator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestCreateNode(t *testing.T) {
	ctx := context.Background()

	t.Run("success - creates node with valid pooler info", func(t *testing.T) {
		poolerInfo := &topo.MultiPoolerInfo{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "mp1",
				},
				Hostname: "localhost",
				PortMap: map[string]int32{
					"grpc": 9000,
				},
				Shard: "shard0",
			},
		}

		node, err := CreateNode(ctx, poolerInfo)
		require.NoError(t, err)
		require.NotNil(t, node)
		require.Equal(t, "mp1", node.ID.Name)
		require.Equal(t, "zone1", node.ID.Cell)
		require.Equal(t, "localhost", node.Hostname)
		require.Equal(t, int32(9000), node.Port)
		require.Equal(t, "shard0", node.ShardID)
		require.NotNil(t, node.ManagerClient)
		require.NotNil(t, node.ConsensusClient)
	})

	t.Run("error - missing grpc address", func(t *testing.T) {
		poolerInfo := &topo.MultiPoolerInfo{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "mp1",
				},
				Hostname: "",
				PortMap:  map[string]int32{},
			},
		}

		node, err := CreateNode(ctx, poolerInfo)
		require.Error(t, err)
		require.Nil(t, node)
		require.Contains(t, err.Error(), "no gRPC address")
	})
}

func TestCreateNodes(t *testing.T) {
	ctx := context.Background()

	t.Run("success - creates multiple nodes", func(t *testing.T) {
		poolerInfos := []*topo.MultiPoolerInfo{
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "zone1",
						Name:      "mp1",
					},
					Hostname: "localhost",
					PortMap: map[string]int32{
						"grpc": 9000,
					},
					Shard: "shard0",
				},
			},
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "zone1",
						Name:      "mp2",
					},
					Hostname: "localhost",
					PortMap: map[string]int32{
						"grpc": 9001,
					},
					Shard: "shard0",
				},
			},
		}

		nodes, err := CreateNodes(ctx, poolerInfos)
		require.NoError(t, err)
		require.Len(t, nodes, 2)
		require.Equal(t, "mp1", nodes[0].ID.Name)
		require.Equal(t, "mp2", nodes[1].ID.Name)
	})

	t.Run("error - fails if any node creation fails", func(t *testing.T) {
		poolerInfos := []*topo.MultiPoolerInfo{
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "zone1",
						Name:      "mp1",
					},
					Hostname: "localhost",
					PortMap: map[string]int32{
						"grpc": 9000,
					},
				},
			},
			{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "zone1",
						Name:      "mp2",
					},
					Hostname: "",
					PortMap:  map[string]int32{},
				},
			},
		}

		nodes, err := CreateNodes(ctx, poolerInfos)
		require.Error(t, err)
		require.Nil(t, nodes)
	})
}
