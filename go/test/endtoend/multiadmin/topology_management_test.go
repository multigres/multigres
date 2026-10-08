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

package multiadmin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/test/utils/openapitest"
)

func TestMultiadminTopologyManagement(t *testing.T) {
	if testing.Short() || utils.ShouldSkipRealPostgres() {
		t.Skip("requires the live test cluster")
	}
	setup := getSharedSetup(t)
	base := fmt.Sprintf("http://localhost:%d/api/v1", setup.MultiadminHttpPort)
	contract := openapitest.Validator(t)
	call := func(method, route string, body proto.Message, want int, out proto.Message) {
		t.Helper()
		var payload []byte
		var err error
		if body != nil {
			payload, err = protojson.Marshal(body)
			require.NoError(t, err)
		}
		req, err := http.NewRequestWithContext(t.Context(), method, base+route, bytes.NewReader(payload))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		valid, failures := contract.ValidateHttpResponse(req, response)
		require.True(t, valid, "%s %s: %+v", method, route, failures)
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, want, response.StatusCode, string(data))
		if out != nil {
			require.NoError(t, protojson.Unmarshal(data, out))
		}
	}
	existing, err := setup.TopoServer.GetCell(t.Context(), setup.CellName)
	require.NoError(t, err)
	cell := &clustermetadatapb.Cell{Name: "management-cell", ServerAddresses: existing.ServerAddresses, Root: existing.Root + "/management"}
	database := &clustermetadatapb.Database{Name: "managementdb", Cells: []string{cell.Name}, BootstrapDurabilityPolicy: topoclient.AtLeastN(2)}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, setup.TopoServer.DeleteDatabase(ctx, database.Name, true))
		require.NoError(t, setup.TopoServer.DeleteCell(ctx, cell.Name, true))
	})
	for range 2 {
		call("POST", "/cells", cell, http.StatusOK, &multiadminpb.CreateCellResponse{})
		call("POST", "/databases", database, http.StatusOK, &multiadminpb.CreateDatabaseResponse{})
	}
	var readDatabase multiadminpb.GetDatabaseResponse
	call("GET", "/databases/"+database.Name, nil, http.StatusOK, &readDatabase)
	require.True(t, proto.Equal(database, readDatabase.Database))
	conflict := proto.Clone(database).(*clustermetadatapb.Database)
	conflict.BootstrapDurabilityPolicy = topoclient.AtLeastN(3)
	call("POST", "/databases", conflict, http.StatusConflict, nil)
	// No process is started for these fixture registrations.
	old := topoclient.NewMultipooler("retired-member", cell.Name, "localhost")
	old.ShardKey = &clustermetadatapb.ShardKey{Database: database.Name, TableGroup: "default", Shard: "0-inf"}
	require.NoError(t, setup.TopoServer.CreateMultipooler(t.Context(), old))
	registrationRoute := "/poolers/" + cell.Name + "/retired-member/registration"
	retireRoute := "/poolers/" + cell.Name + "/retired-member/retire"
	var observed multiadminpb.GetPoolerRegistrationResponse
	call("GET", registrationRoute, nil, http.StatusOK, &observed)
	request := &multiadminpb.RetirePoolerRequest{PoolerId: old.Id, ShardKey: old.ShardKey, IncarnationId: old.IncarnationId, Version: observed.Version, FencingAcknowledged: true}
	_, err = setup.TopoServer.UpdateMultipoolerFields(t.Context(), old.Id, func(mp *clustermetadatapb.Multipooler) error { mp.Hostname = "updated-host"; return nil })
	require.NoError(t, err)
	call("POST", retireRoute, request, http.StatusConflict, nil)
	call("GET", registrationRoute, nil, http.StatusOK, &observed)
	request.Version = observed.Version
	for range 2 {
		call("POST", retireRoute, request, http.StatusOK, &multiadminpb.RetirePoolerResponse{})
	}
	call("GET", registrationRoute, nil, http.StatusNotFound, nil)
	replacement := topoclient.NewMultipooler(old.Id.Name, cell.Name, old.Hostname)
	replacement.ShardKey = old.ShardKey
	require.NoError(t, setup.TopoServer.CreateMultipooler(t.Context(), replacement))
	call("POST", retireRoute, request, http.StatusConflict, nil)
	call("GET", registrationRoute, nil, http.StatusOK, &observed)
	require.Equal(t, replacement.IncarnationId, observed.Pooler.IncarnationId)
	request.IncarnationId = observed.Pooler.IncarnationId
	request.Version = observed.Version
	call("POST", retireRoute, request, http.StatusOK, &multiadminpb.RetirePoolerResponse{})
	// Running poolers publish incarnation IDs too, without involving retirement.
	var live multiadminpb.GetPoolersResponse
	call("GET", "/poolers?cells="+setup.CellName, nil, http.StatusOK, &live)
	require.NotEmpty(t, live.Poolers)
	for _, mp := range live.Poolers {
		require.NotEmpty(t, mp.IncarnationId)
	}
}
