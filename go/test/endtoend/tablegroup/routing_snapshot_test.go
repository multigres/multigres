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

package tablegroup

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// catalogPickupTimeout bounds how long the gateway takes to show a catalog
// change. The gateway's poll is disabled in this test, so the change can only
// arrive through the catalog primary's health stream: at once for its own
// writes, and within its periodic catalog_state reread after a failover.
var catalogPickupTimeout = utils.ScaleTimeout(30 * time.Second)

// gatewayCatalog is the part of the gateway's /debug/sharding-catalog
// response the test reads.
type gatewayCatalog struct {
	Databases []struct {
		Database       string `json:"database"`
		Installed      bool   `json:"installed"`
		RoutingVersion int64  `json:"routing_version"`
		ConsensusRule  string `json:"consensus_rule"`
		TableGroups    []struct {
			ID     int64  `json:"id"`
			Name   string `json:"name"`
			State  string `json:"state"`
			Shards []struct {
				Name  string `json:"name"`
				State string `json:"state"`
			} `json:"shards"`
		} `json:"tablegroups"`
	} `json:"databases"`
}

// readGatewayCatalog returns the gateway's installed snapshot of postgres:
// its routing_version and each tablegroup's shard names by tablegroup name.
// ok is false until a snapshot is installed.
func readGatewayCatalog(t *testing.T, setup *shardsetup.ShardSetup) (version int64, tableGroups map[string][]string, ok bool) {
	t.Helper()
	version, _, tableGroups, ok = readGatewayCatalogAt(t, setup)
	return version, tableGroups, ok
}

// readGatewayCatalogAt is readGatewayCatalog that also returns the consensus
// rule the installed snapshot was served under, as "term.subterm".
func readGatewayCatalogAt(t *testing.T, setup *shardsetup.ShardSetup) (version int64, rule string, tableGroups map[string][]string, ok bool) {
	t.Helper()
	url := fmt.Sprintf("http://localhost:%d/debug/sharding-catalog", setup.Multigateway.HttpPort)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, "", nil, false
	}
	var got gatewayCatalog
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	for _, db := range got.Databases {
		if db.Database != "postgres" || !db.Installed {
			continue
		}
		tableGroups = map[string][]string{}
		for _, tg := range db.TableGroups {
			names := []string{}
			for _, sh := range tg.Shards {
				names = append(names, sh.Name)
			}
			tableGroups[tg.Name] = names
		}
		return db.RoutingVersion, db.ConsensusRule, tableGroups, true
	}
	return 0, "", nil, false
}

func getRoutingSnapshot(t *testing.T, manager multipoolermanagerpb.MultipoolerManagerClient, since *int64, sinceRule *clustermetadatapb.RuleNumber) *multipoolermanagerdatapb.GetRoutingSnapshotResponse {
	t.Helper()
	resp, err := manager.GetRoutingSnapshot(t.Context(), &multipoolermanagerdatapb.GetRoutingSnapshotRequest{SinceVersion: since, SinceConsensusRule: sinceRule})
	require.NoError(t, err)
	return resp
}

func TestRoutingSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end routing snapshot test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithMultigatewayExtraArgs("--enable-tablegroup-ddl", "--sharding-catalog-poll-interval=0"),
	)
	defer cleanup()
	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	primary := setup.NewPrimaryClient(t)
	defer primary.Close()
	oldPrimaryName := setup.PrimaryName

	var freshVersion int64
	var firstRule *clustermetadatapb.RuleNumber
	t.Run("a fresh catalog is the default tablegroup", func(t *testing.T) {
		resp := getRoutingSnapshot(t, primary.Manager, nil, nil)
		require.False(t, resp.GetUnchanged())
		firstRule = resp.GetConsensusRule()
		require.NotNil(t, firstRule, "the snapshot is stamped with the primary's consensus rule")
		snap := resp.GetSnapshot()
		require.NotNil(t, snap)
		freshVersion = snap.GetRoutingVersion()
		assert.Equal(t, resp.GetRoutingVersion(), freshVersion)
		require.Len(t, snap.GetTablegroups(), 1)
		assert.Equal(t, "default", snap.GetTablegroups()[0].GetName())
		assert.Equal(t, shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE, snap.GetTablegroups()[0].GetState())
		require.Len(t, snap.GetShards(), 1)
		assert.Equal(t, "0-inf", snap.GetShards()[0].GetName())
		assert.Equal(t, shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE, snap.GetShards()[0].GetState())
		_, err := shardingcatalog.New(snap)
		require.NoError(t, err, "the served snapshot validates")

		again := getRoutingSnapshot(t, primary.Manager, &freshVersion, firstRule)
		assert.True(t, again.GetUnchanged())
		assert.Nil(t, again.GetSnapshot())

		replica := setup.NewClient(t, replicaName(t, setup))
		defer replica.Close()
		_, err = replica.Manager.GetRoutingSnapshot(t.Context(), &multipoolermanagerdatapb.GetRoutingSnapshotRequest{})
		requireCode(t, err, codes.FailedPrecondition)

		require.Eventually(t, func() bool {
			v, groups, ok := readGatewayCatalog(t, setup)
			return ok && v == freshVersion && len(groups) == 1
		}, catalogPickupTimeout, 200*time.Millisecond, "the gateway never installed the fresh catalog")
	})

	var createdVersion int64
	t.Run("the gateway sees CREATE TABLEGROUP without a restart", func(t *testing.T) {
		createThroughGateway(t, setup, "CREATE TABLEGROUP commerce SHARDS ('0-80', '80-inf')")

		var groups map[string][]string
		require.Eventually(t, func() bool {
			var ok bool
			createdVersion, groups, ok = readGatewayCatalog(t, setup)
			return ok && groups["commerce"] != nil
		}, catalogPickupTimeout, 200*time.Millisecond, "the gateway never saw the new tablegroup")
		assert.Equal(t, []string{"0-80", "80-inf"}, groups["commerce"])
		assert.Equal(t, []string{"0-inf"}, groups["default"])
		assert.Equal(t, freshVersion+1, createdVersion, "one creation advances routing_version once")
	})

	t.Run("a new catalog primary serves a version no lower than before", func(t *testing.T) {
		require.NotZero(t, createdVersion)
		setup.KillPostgres(t, oldPrimaryName)
		newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, utils.ScaleTimeout(60*time.Second))
		newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
		require.NotNil(t, newPrimary)
		shardsetup.WaitForManagerReady(t, newPrimary.Multipooler)
		client := setup.NewClient(t, newPrimaryName)
		defer client.Close()

		var resp *multipoolermanagerdatapb.GetRoutingSnapshotResponse
		require.Eventually(t, func() bool {
			var err error
			resp, err = client.Manager.GetRoutingSnapshot(t.Context(), &multipoolermanagerdatapb.GetRoutingSnapshotRequest{})
			return err == nil
		}, utils.ScaleTimeout(60*time.Second), 500*time.Millisecond, "the new primary never served a snapshot")
		assert.GreaterOrEqual(t, resp.GetRoutingVersion(), createdVersion)
		assert.Positive(t, commonconsensus.CompareRuleNumbers(resp.GetConsensusRule(), firstRule),
			"the new primary serves under a higher rule (%s after %s)",
			commonconsensus.FormatRuleNumber(resp.GetConsensusRule()), commonconsensus.FormatRuleNumber(firstRule))
		names := []string{}
		for _, tg := range resp.GetSnapshot().GetTablegroups() {
			names = append(names, tg.GetName())
		}
		assert.Contains(t, names, "commerce", "the creation survived the failover")

		// The gateway follows the new primary: a change written there shows up
		// through its health stream.
		createThroughGateway(t, setup, "CREATE TABLEGROUP analytics")
		require.Eventually(t, func() bool {
			v, rule, groups, ok := readGatewayCatalogAt(t, setup)
			return ok && v > resp.GetRoutingVersion() && rule != commonconsensus.FormatRuleNumber(firstRule) &&
				groups["analytics"] != nil && groups["commerce"] != nil
		}, catalogPickupTimeout, 200*time.Millisecond, "the gateway never installed a snapshot from the new primary")
	})
}

// createThroughGateway runs a CREATE TABLEGROUP through the gateway. Nothing
// provisions the shards here, so the statement waits until statement_timeout
// ends the wait; the creation is recorded anyway.
func createThroughGateway(t *testing.T, setup *shardsetup.ShardSetup, stmt string) {
	t.Helper()
	db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(t.Context(), "SET statement_timeout = '2s'")
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), stmt)
	require.Error(t, err, "the creation cannot complete without a provisioner")
}
