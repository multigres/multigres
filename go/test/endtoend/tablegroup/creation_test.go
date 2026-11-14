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
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	validator "github.com/pb33f/libopenapi-validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/test/utils/openapitest"
)

// provisionTimeout bounds waiting for provisioned shards to serve: starting
// two poolers per shard and bootstrapping the shard with multiorch.
var provisionTimeout = utils.ScaleTimeout(4 * time.Minute)

func TestTableGroupCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tablegroup test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultiadmin())
	defer cleanup()
	primary := setup.NewPrimaryClient(t)
	defer primary.Close()
	manager := primary.Manager
	prov := newProvisioner(t, setup)

	var op string
	var tg *multipoolermanagerdatapb.TableGroup

	t.Run("records the intent", func(t *testing.T) {
		resp, err := manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{
			Name:   "commerce",
			Shards: []string{"80-inf", "0-80"},
		})
		require.NoError(t, err)
		op = resp.GetOperationId()
		require.NoError(t, uuid.Validate(op), "operation id %q is not a UUID", op)
		assert.Positive(t, resp.GetTablegroupId())
		assert.False(t, resp.GetAlreadyExists())

		got := getOperation(t, manager, op)
		assert.Equal(t, multipoolermanagerdatapb.OperationKind_OPERATION_KIND_CREATE_TABLEGROUP, got.GetKind())
		assert.Equal(t, multipoolermanagerdatapb.OperationState_OPERATION_STATE_CREATING, got.GetState())
		assert.Equal(t, resp.GetTablegroupId(), got.GetTablegroupId())
		assert.Equal(t, "commerce", got.GetTablegroupName())
		assert.Equal(t, int32(2), got.GetShardsDesired())
		assert.Zero(t, got.GetShardsServing())

		tg = prov.find(t, "commerce")
		require.NotNil(t, tg, "commerce is not desired")
		assert.Equal(t, resp.GetTablegroupId(), tg.GetTablegroupId())
		assert.Equal(t, shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_CREATING, tg.GetState())
		assert.Equal(t, []string{"0-80", "80-inf"}, tg.GetShards(), "shards are listed in canonical order")
	})

	t.Run("a repeat joins the creation", func(t *testing.T) {
		require.NotEmpty(t, op)
		for _, ifNotExists := range []bool{false, true} {
			resp, err := manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{
				Name: "commerce", Shards: []string{"0-80", "80-inf"}, IfNotExists: ifNotExists,
			})
			require.NoError(t, err)
			assert.Equal(t, op, resp.GetOperationId())
		}
		_, err := manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce"})
		requireCode(t, err, codes.FailedPrecondition)
		// IF NOT EXISTS ignores the layout, as PostgreSQL ignores the definition.
		resp, err := manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce", IfNotExists: true})
		require.NoError(t, err)
		assert.Equal(t, op, resp.GetOperationId())
	})

	t.Run("concurrent creations of a new name share one operation", func(t *testing.T) {
		const callers = 4
		ops := make(chan string, callers)
		errs := make(chan error, callers)
		for range callers {
			go func() {
				resp, err := manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{
					Name: "analytics", Shards: []string{"0-inf"},
				})
				if err != nil {
					errs <- err
					return
				}
				ops <- resp.GetOperationId()
			}()
		}
		seen := map[string]bool{}
		for range callers {
			select {
			case err := <-errs:
				require.NoError(t, err)
			case id := <-ops:
				seen[id] = true
			}
		}
		assert.Len(t, seen, 1, "every caller joins the same creation")
	})

	t.Run("rejects invalid requests", func(t *testing.T) {
		for _, tc := range []struct {
			req  *multipoolermanagerdatapb.CreateTableGroupRequest
			code codes.Code
		}{
			{&multipoolermanagerdatapb.CreateTableGroupRequest{Name: "Commerce"}, codes.InvalidArgument},
			{&multipoolermanagerdatapb.CreateTableGroupRequest{Name: "gap", Shards: []string{"0-40", "80-inf"}}, codes.InvalidArgument},
			{&multipoolermanagerdatapb.CreateTableGroupRequest{Name: constants.DefaultTableGroup}, codes.AlreadyExists},
		} {
			_, err := manager.CreateTableGroup(t.Context(), tc.req)
			requireCode(t, err, tc.code)
		}
		_, err := manager.GetOperation(t.Context(), &multipoolermanagerdatapb.GetOperationRequest{OperationId: "not-a-uuid"})
		requireCode(t, err, codes.InvalidArgument)
		_, err = manager.GetOperation(t.Context(), &multipoolermanagerdatapb.GetOperationRequest{OperationId: uuid.NewString()})
		requireCode(t, err, codes.NotFound)
		assert.Len(t, prov.desired(t), 2, "rejected requests leave the catalog unchanged")
	})

	t.Run("a replica of the catalog shard refuses every operation", func(t *testing.T) {
		require.NotEmpty(t, op)
		replica := setup.NewClient(t, replicaName(t, setup))
		defer replica.Close()
		ctx := t.Context()
		_, err := replica.Manager.GetOperation(ctx, &multipoolermanagerdatapb.GetOperationRequest{OperationId: op})
		requireCode(t, err, codes.FailedPrecondition)
		_, err = replica.Manager.ListTableGroups(ctx, &multipoolermanagerdatapb.ListTableGroupsRequest{})
		requireCode(t, err, codes.FailedPrecondition)
		_, err = replica.Manager.CreateTableGroup(ctx, &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "elsewhere"})
		requireCode(t, err, codes.FailedPrecondition)
	})

	// The cluster is isolated to this test, so nothing here is cleaned up.
	t.Run("a tablegroup served by an external postgres", func(t *testing.T) {
		// The fixture goes around the pooler, to the primary postgres.
		admin, err := pgx.Connect(t.Context(), fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.PrimaryPgctld(t).PgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)
		t.Cleanup(func() { _ = admin.Close(context.Background()) })
		_, err = admin.Exec(t.Context(), `INSERT INTO multigres.connections (name, dsn, conninfo) VALUES
			('ext-src', 'not read', '{"host":["db.example.com"],"port":[5432],"dbname":"postgres","user":"app","password":"secret","sslmode":"require"}'),
			('ext-other-db', 'not read', '{"host":["db.example.com"],"port":[5432],"dbname":"elsewhere","user":"app","password":"secret","sslmode":"require"}')`)
		require.NoError(t, err)

		for _, tc := range []struct {
			name string
			req  *multipoolermanagerdatapb.CreateTableGroupRequest
			code codes.Code
		}{
			{"a connection that does not exist", &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "external", BackingConnection: "nope"}, codes.NotFound},
			{"a connection to another database", &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "external", BackingConnection: "ext-other-db"}, codes.FailedPrecondition},
			{"more than one shard", &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "external", BackingConnection: "ext-src", Shards: []string{"0-80", "80-inf"}}, codes.InvalidArgument},
			{"the default tablegroup", &multipoolermanagerdatapb.CreateTableGroupRequest{Name: constants.DefaultTableGroup, BackingConnection: "ext-src"}, codes.InvalidArgument},
		} {
			_, err := manager.CreateTableGroup(t.Context(), tc.req)
			requireCode(t, err, tc.code)
		}
		assert.Nil(t, prov.find(t, "external"), "a rejected request leaves the catalog unchanged")

		resp, err := manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "external", BackingConnection: "ext-src"})
		require.NoError(t, err)
		assert.Positive(t, resp.GetTablegroupId())

		// The provisioner learns the binding from the desired list, and the list is valid
		// against the published OpenAPI contract.
		listed := prov.find(t, "external")
		require.NotNil(t, listed)
		assert.Equal(t, "ext-src", listed.GetBackingConnection())
		assert.Equal(t, []string{"0-inf"}, listed.GetShards())
		assert.Empty(t, prov.find(t, "analytics").GetBackingConnection(), "a managed tablegroup has none")

		// A repeat has to match the binding, with or without IF NOT EXISTS, which ignores the
		// definition otherwise.
		again, err := manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "external", BackingConnection: "ext-src"})
		require.NoError(t, err)
		assert.Equal(t, resp.GetOperationId(), again.GetOperationId())
		_, err = manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "external"})
		requireCode(t, err, codes.FailedPrecondition)
		_, err = manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "external", IfNotExists: true})
		requireCode(t, err, codes.FailedPrecondition)
		again, err = manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "external", BackingConnection: "ext-src", IfNotExists: true})
		require.NoError(t, err)
		assert.Equal(t, resp.GetOperationId(), again.GetOperationId())

		// A connection a tablegroup is bound to cannot be deleted from under it.
		_, err = admin.Exec(t.Context(), `DELETE FROM multigres.connections WHERE name = 'ext-src'`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tablegroup_backing_connection_id_fkey")
	})

	t.Run("provisioned shards complete the creation", func(t *testing.T) {
		// The poolers and multiorchs provisioned here are stopped when this
		// subtest ends, so everything that needs them serving is asserted here.
		require.NotNil(t, tg)
		prov.provision(t, tg)
		claim, err := setup.TopoServer.GetTableGroupProvisionClaim(t.Context(), "postgres", tg.GetTablegroupId())
		require.NoError(t, err, "the provisioner claims the id before it provisions")
		assert.Equal(t, "e2e-provisioner", claim.GetProvisioner())
		require.Eventually(t, func() bool {
			return getOperation(t, manager, op).GetShardsServing() == 2
		}, provisionTimeout, time.Second, "the provisioned shards never served")

		// The completion loop sees every shard serving and activates the tablegroup.
		require.Eventually(t, func() bool {
			return getOperation(t, manager, op).GetState() == multipoolermanagerdatapb.OperationState_OPERATION_STATE_DONE
		}, provisionTimeout, time.Second, "the creation never completed")
		got := getOperation(t, manager, op)
		assert.Equal(t, int32(2), got.GetShardsServing())
		assert.Empty(t, got.GetLastError())
		active := prov.find(t, "commerce")
		require.NotNil(t, active)
		assert.Equal(t, shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE, active.GetState())
		assert.Equal(t, []string{"0-80", "80-inf"}, active.GetShards())

		// Repeating the creation of an active tablegroup is a conflict, or a
		// no-op with IF NOT EXISTS.
		_, err = manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce", Shards: []string{"0-80", "80-inf"}})
		requireCode(t, err, codes.AlreadyExists)
		resp, err := manager.CreateTableGroup(t.Context(), &multipoolermanagerdatapb.CreateTableGroupRequest{Name: "commerce", IfNotExists: true})
		require.NoError(t, err)
		assert.True(t, resp.GetAlreadyExists())
		assert.Equal(t, tg.GetTablegroupId(), resp.GetTablegroupId())

		// The catalog shows the shards active and the routing version advanced.
		rows, err := primary.Pooler.ExecuteQuery(t.Context(),
			"SELECT count(*) FROM multigres.shard WHERE state = 'active' AND tablegroup_id = "+strconv.FormatInt(tg.GetTablegroupId(), 10), 1)
		require.NoError(t, err)
		require.Len(t, rows.Rows, 1)
		assert.Equal(t, "2", string(rows.Rows[0].Values[0]))
		rows, err = primary.Pooler.ExecuteQuery(t.Context(), "SELECT routing_version FROM multigres.catalog_state", 1)
		require.NoError(t, err)
		require.Len(t, rows.Rows, 1)
		assert.NotEqual(t, "0", string(rows.Rows[0].Values[0]))
	})
}

func getOperation(t *testing.T, manager multipoolermanagerpb.MultipoolerManagerClient, id string) *multipoolermanagerdatapb.Operation {
	t.Helper()
	resp, err := manager.GetOperation(t.Context(), &multipoolermanagerdatapb.GetOperationRequest{OperationId: id})
	require.NoError(t, err)
	return resp.GetOperation()
}

func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, code, status.Code(err), err.Error())
}

// replicaName returns a pooler of the catalog shard other than its primary.
func replicaName(t *testing.T, setup *shardsetup.ShardSetup) string {
	t.Helper()
	for name := range setup.Multipoolers {
		if name != setup.PrimaryName {
			return name
		}
	}
	t.Fatal("the catalog shard has no replica")
	return ""
}

// provisioner stands in for the Kubernetes operator: it reads desired
// tablegroups from multiadmin's REST API and starts two poolers and a
// multiorch per shard.
type provisioner struct {
	setup    *shardsetup.ShardSetup
	contract validator.Validator
}

func newProvisioner(t *testing.T, setup *shardsetup.ShardSetup) *provisioner {
	return &provisioner{setup: setup, contract: openapitest.Validator(t)}
}

// desired reads ListDesiredTableGroups over REST, validating the response
// against the published OpenAPI contract the operator builds on.
func (p *provisioner) desired(t *testing.T) []*multipoolermanagerdatapb.TableGroup {
	t.Helper()
	url := fmt.Sprintf("http://localhost:%d/api/v1/databases/postgres/tablegroups", p.setup.MultiadminHttpPort)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	valid, failures := p.contract.ValidateHttpResponse(req, resp)
	require.True(t, valid, "ListDesiredTableGroups violates OpenAPI: %+v", failures)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	out := &multiadminpb.ListDesiredTableGroupsResponse{}
	require.NoError(t, protojson.Unmarshal(body, out), string(body))
	return out.GetTableGroups()
}

func (p *provisioner) find(t *testing.T, name string) *multipoolermanagerdatapb.TableGroup {
	t.Helper()
	for _, tg := range p.desired(t) {
		if tg.GetName() == name {
			return tg
		}
	}
	return nil
}

// provision claims the tablegroup's id in topology, as the operator must before
// it provisions anything, then starts each shard's poolers with the catalog id
// and a multiorch to bootstrap the shard.
func (p *provisioner) provision(t *testing.T, tg *multipoolermanagerdatapb.TableGroup) {
	t.Helper()
	require.NoError(t, p.setup.TopoServer.ClaimTableGroupProvisioning(t.Context(), "postgres", &clustermetadatapb.TableGroupProvisionClaim{
		Provisioner: "e2e-provisioner", TablegroupId: tg.GetTablegroupId(), TablegroupName: tg.GetName(),
	}))
	for _, shard := range tg.GetShards() {
		for i := range 2 {
			name := fmt.Sprintf("%s-%d-%s-%d", tg.GetName(), tg.GetTablegroupId(), shard, i)
			inst := p.setup.CreateMultipoolerInstance(t, name, utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t))
			inst.Multipooler.ExtraArgs = append(inst.Multipooler.ExtraArgs,
				"--table-group", tg.GetName(),
				"--shard", shard,
				"--tablegroup-id", strconv.FormatInt(tg.GetTablegroupId(), 10),
			)
			require.NoError(t, inst.Pgctld.Start(t.Context(), t), "start pgctld %s", name)
			require.NoError(t, inst.Multipooler.Start(t.Context(), t), "start multipooler %s", name)
			shardsetup.WaitForManagerReady(t, inst.Multipooler)
		}
		orch, _ := p.setup.CreateMultiorchInstance(t, fmt.Sprintf("orch-%s-%d-%s", tg.GetName(), tg.GetTablegroupId(), shard),
			[]string{fmt.Sprintf("postgres/%s/%s", tg.GetName(), shard)},
			&shardsetup.SetupConfig{CellName: p.setup.CellName})
		require.NoError(t, orch.Start(t.Context(), t), "start multiorch for %s/%s", tg.GetName(), shard)
	}
}
