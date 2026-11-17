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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestTableGroupProvisionClaim(t *testing.T) {
	claim := func(provisioner string, id int64) *clustermetadatapb.TableGroupProvisionClaim {
		return &clustermetadatapb.TableGroupProvisionClaim{Provisioner: provisioner, TablegroupId: id, TablegroupName: "commerce"}
	}
	noNode := func(t *testing.T, err error) {
		t.Helper()
		require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}), "want NoNode, got %v", err)
	}

	t.Run("claim, read, release", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		_, err := ts.GetTableGroupProvisionClaim(ctx, "mydb", 7)
		noNode(t, err)

		require.NoError(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-a", 7)))
		got, err := ts.GetTableGroupProvisionClaim(ctx, "mydb", 7)
		require.NoError(t, err)
		assert.Equal(t, "operator-a", got.GetProvisioner())
		assert.Equal(t, int64(7), got.GetTablegroupId())
		assert.Equal(t, "commerce", got.GetTablegroupName())

		require.NoError(t, ts.ReleaseTableGroupProvisioning(ctx, "mydb", 7))
		_, err = ts.GetTableGroupProvisionClaim(ctx, "mydb", 7)
		noNode(t, err)
	})

	t.Run("the same provisioner may repeat its claim; another may not", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		require.NoError(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-a", 7)))
		require.NoError(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-a", 7)), "a crashed provisioner resumes")
		err := ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-b", 7))
		require.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}), "want NodeExists, got %v", err)
		got, err := ts.GetTableGroupProvisionClaim(ctx, "mydb", 7)
		require.NoError(t, err)
		assert.Equal(t, "operator-a", got.GetProvisioner(), "the first claim stands")
	})

	t.Run("releasing a claim that is not held is not an error", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()
		require.NoError(t, ts.ReleaseTableGroupProvisioning(ctx, "mydb", 7))
	})

	t.Run("claims are per database and per id", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		require.NoError(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-a", 10)))
		require.NoError(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-a", 7)))
		require.NoError(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-a", 9)))
		require.NoError(t, ts.ClaimTableGroupProvisioning(ctx, "otherdb", claim("operator-a", 7)))

		claims, err := ts.ListTableGroupProvisionClaims(ctx, "mydb")
		require.NoError(t, err)
		require.Len(t, claims, 3)
		assert.Equal(t, int64(7), claims[0].GetTablegroupId())
		assert.Equal(t, int64(9), claims[1].GetTablegroupId(), "ids sort numerically, not as strings")
		assert.Equal(t, int64(10), claims[2].GetTablegroupId())
		for _, c := range claims {
			assert.NotNil(t, c.GetClaimedAt(), "claimed_at is stamped when the caller leaves it empty")
		}

		require.NoError(t, ts.ReleaseTableGroupProvisioning(ctx, "mydb", 7))
		claims, err = ts.ListTableGroupProvisionClaims(ctx, "mydb")
		require.NoError(t, err)
		require.Len(t, claims, 2)
		assert.Equal(t, int64(9), claims[0].GetTablegroupId())
		_, err = ts.GetTableGroupProvisionClaim(ctx, "otherdb", 7)
		require.NoError(t, err, "the other database's claim is untouched")

		claims, err = ts.ListTableGroupProvisionClaims(ctx, "nodb")
		require.NoError(t, err)
		assert.Empty(t, claims)
	})

	t.Run("a stray node in the claims directory is ignored", func(t *testing.T) {
		// Nothing stops a pooler from being started with the directory's name
		// as its tablegroup; multiorch would then write that shard's init
		// claim under it.
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()
		coord := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "orch-1"}
		won, _, err := ts.ClaimShardInitialization(ctx, &clustermetadatapb.ShardRef{Database: "mydb", TableGroup: topoclient.TableGroupClaimsDir, Shard: "0-inf"}, coord, nil)
		require.NoError(t, err)
		require.True(t, won)
		require.NoError(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-a", 7)))

		claims, err := ts.ListTableGroupProvisionClaims(ctx, "mydb")
		require.NoError(t, err)
		require.Len(t, claims, 1)
		assert.Equal(t, int64(7), claims[0].GetTablegroupId())
	})

	t.Run("every call needs a database and a positive id", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()
		require.Error(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("", 7)), "a provisioner is required")
		require.Error(t, ts.ClaimTableGroupProvisioning(ctx, "mydb", claim("operator-a", 0)))
		require.Error(t, ts.ClaimTableGroupProvisioning(ctx, "", claim("operator-a", 7)))
		_, err := ts.GetTableGroupProvisionClaim(ctx, "", 7)
		require.Error(t, err)
		_, err = ts.GetTableGroupProvisionClaim(ctx, "mydb", 0)
		require.Error(t, err)
		require.Error(t, ts.ReleaseTableGroupProvisioning(ctx, "", 7))
		require.Error(t, ts.ReleaseTableGroupProvisioning(ctx, "mydb", -1))
		_, err = ts.ListTableGroupProvisionClaims(ctx, "")
		require.Error(t, err)
	})
}
