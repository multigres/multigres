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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	validator "github.com/pb33f/libopenapi-validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/common/constants"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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

	t.Run("the catalog creates multigres_topo", func(t *testing.T) {
		resp, err := primary.Pooler.ExecuteQuery(t.Context(),
			"SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = 'multigres_topo'", 1)
		require.NoError(t, err)
		require.Len(t, resp.Rows, 1, "multigres_topo does not exist")
		assert.Equal(t, "f", string(resp.Rows[0].Values[0]), "multigres_topo must not log in")
	})

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
		assert.Equal(t, multipoolermanagerdatapb.TableGroupState_TABLE_GROUP_STATE_CREATING, tg.GetState())
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

	t.Run("provisioned shards count toward the creation", func(t *testing.T) {
		require.NotNil(t, tg)
		prov.provision(t, tg)
		require.Eventually(t, func() bool {
			return getOperation(t, manager, op).GetShardsServing() == 2
		}, provisionTimeout, time.Second, "the provisioned shards never served")
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

// provision starts each shard's poolers with the tablegroup's catalog id and
// a multiorch to bootstrap the shard.
func (p *provisioner) provision(t *testing.T, tg *multipoolermanagerdatapb.TableGroup) {
	t.Helper()
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
