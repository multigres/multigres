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

package minigressetup

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/utils"
)

var setupManager = clustersetup.NewSharedSetupManager(func(t *testing.T) *Setup {
	return New(t)
})

func TestMain(m *testing.M) {
	exitCode := clustersetup.RunTestMain(m)
	if exitCode != 0 {
		setupManager.DumpLogs()
	}
	setupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
}

// TestMinigres_ServesQueries checks the harness end to end: the shared Minigres
// cluster is in its clean state, and both comparison targets answer, the proxy
// target through the minigres process.
func TestMinigres_ServesQueries(t *testing.T) {
	skipIfShort(t)
	setup := setupManager.Get(t)
	setup.SetupTest(t)

	targets := setup.ComparisonTargets(t)
	require.Len(t, targets, 2)
	for _, target := range targets {
		t.Run(target.Name, func(t *testing.T) {
			db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5"))
			require.NoError(t, err)
			defer db.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var one int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
			assert.Equal(t, 1, one)

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "CREATE TEMP TABLE harness_check (x int)")
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "INSERT INTO harness_check VALUES (1), (2)")
			require.NoError(t, err)
			var sum int
			require.NoError(t, tx.QueryRowContext(ctx, "SELECT sum(x) FROM harness_check").Scan(&sum))
			assert.Equal(t, 3, sum)
			require.NoError(t, tx.Rollback())
		})
	}
	assert.Equal(t, setup.ClientPort(), targets[1].Port, "the proxy target is the minigres client port")
	assert.Equal(t, setup.GatewayLogFile(), setup.PoolerLogFile(t), "minigres writes both halves to one log")
}

// TestMinigres_QueriesStayInProcess checks that the query service is served
// only in memory: the external gRPC port answers the admin services but not
// MultipoolerService, while queries through the client port still reach the
// multipooler, which they now can only do without leaving the process.
func TestMinigres_QueriesStayInProcess(t *testing.T) {
	skipIfShort(t)
	setup := setupManager.Get(t)
	setup.SetupTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The query service is not on the external port.
	queryClient, err := clustersetup.NewMultipoolerTestClient(fmt.Sprintf("localhost:%d", setup.Minigres.GrpcPort))
	require.NoError(t, err)
	defer queryClient.Close()
	_, err = queryClient.ExecuteQuery(ctx, "SELECT 1", 1)
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err), "MultipoolerService must not be served on the external port: %v", err)
	assert.Contains(t, err.Error(), "unknown service", "the service is absent, not merely unimplemented")

	// The admin services are.
	managerClient, err := clustersetup.NewMultipoolerClient(setup.Minigres.GrpcPort)
	require.NoError(t, err)
	defer managerClient.Close()
	_, err = managerClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "the pooler manager service stays on the external port")

	// Queries still reach the multipooler, through the gateway.
	db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer db.Close()
	var one int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)
}

// TestMinigres_GatewayIgnoresOtherPoolers registers another pooler for the same
// database in the shared topology, as another Minigres process would, and
// checks that the gateway never learns of it: it follows only the pooler in
// its own process, not the topology.
func TestMinigres_GatewayIgnoresOtherPoolers(t *testing.T) {
	skipIfShort(t)
	setup := setupManager.Get(t)
	setup.SetupTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	own, err := setup.TopoServer.GetMultipooler(ctx, &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cellName,
		Name:      serviceID,
	})
	require.NoError(t, err)
	other := topoclient.NewMultipooler("other-instance-pooler", cellName, "unreachable.invalid")
	other.ShardKey = own.ShardKey
	other.PortMap["grpc"] = 1
	require.NoError(t, setup.TopoServer.CreateMultipooler(ctx, other))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = setup.TopoServer.UnregisterMultipooler(cleanupCtx, other.Id)
	})

	// A topology watch would deliver the record within milliseconds; give it
	// ample time, then read the pooler list from the gateway's status page.
	time.Sleep(3 * time.Second)
	statusURL := fmt.Sprintf("http://localhost:%d/multigateway", setup.Minigres.HttpPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	page := string(body)

	assert.Contains(t, page, "<th>"+serviceID+"</th>", "the gateway lists its own pooler")
	assert.NotContains(t, page, "other-instance-pooler", "the gateway must not learn of other processes' poolers")

	// Queries still go to the own pooler.
	db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer db.Close()
	var one int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)
}
