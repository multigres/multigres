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
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	adminserver "github.com/multigres/multigres/go/services/multiadmin"
	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/testpoll"
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
	other.ShardRef = own.ShardRef
	other.PortMap["grpc"] = 1
	require.NoError(t, setup.TopoServer.CreateMultipooler(ctx, other))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = setup.TopoServer.UnregisterMultipooler(cleanupCtx, other.Id)
	})

	// The status page lists the poolers the gateway knows. A filtered record
	// leaves no trace to wait on, so wait for the gateway to list its own
	// pooler, then check that the other one never appears while the topology
	// watch delivers it (within milliseconds).
	statusURL := fmt.Sprintf("http://localhost:%d/multigateway", setup.Minigres.HttpPort)
	statusPage := func() string {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
		if err != nil {
			return ""
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			return ""
		}
		return string(body)
	}
	require.Eventually(t, func() bool {
		return strings.Contains(statusPage(), "<th>"+serviceID+"</th>")
	}, 10*time.Second, 100*time.Millisecond, "the gateway lists its own pooler")
	testpoll.Never(t, func() bool {
		return strings.Contains(statusPage(), "other-instance-pooler")
	}, 3*time.Second, 100*time.Millisecond, "the gateway must not learn of other processes' poolers")

	// Queries still go to the own pooler.
	db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer db.Close()
	var one int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)
}

// TestMinigres_ServesWithoutEtcd checks that Minigres keeps serving when etcd
// goes down, and that it starts and serves while etcd is down: the gateway
// learns of its pooler in memory and registers in the background, and the
// static leader starts from its copy of the database record.
func TestMinigres_ServesWithoutEtcd(t *testing.T) {
	skipIfShort(t)
	setup, cleanup := NewIsolated(t)
	defer cleanup()

	setup.StopEtcd(t)
	setup.WaitForQueryServing(t)

	setup.Minigres.TerminateGracefully(t.Logf, 10*time.Second)
	require.NoError(t, setup.Minigres.Start(t.Context(), t), "minigres starts without etcd")
	setup.WaitForQueryServing(t)
}

// TestMinigres_ConfigFile starts Minigres with a config file and checks that a
// gateway setting in it, written behind the gateway's name, reaches the
// gateway.
func TestMinigres_ConfigFile(t *testing.T) {
	skipIfShort(t)
	file := filepath.Join(t.TempDir(), "minigres.yaml")
	require.NoError(t, os.WriteFile(file, []byte("multigateway.statement-timeout: 7s\n"), 0o600))
	setup, cleanup := NewIsolated(t, WithExtraArgs("--config-file="+file))
	defer cleanup()

	db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var timeout string
	require.NoError(t, db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&timeout))
	assert.Equal(t, "7s", timeout, "the gateway's statement timeout comes from the config file")
}

// TestMinigres_BackupAndRestore backs up a static leader through multiadmin
// without "force primary", lists and verifies the backup, then restores it
// into an empty data directory.
func TestMinigres_BackupAndRestore(t *testing.T) {
	skipIfShort(t)
	setup, cleanup := NewIsolated(t)
	defer cleanup()

	db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.ExecContext(t.Context(), "CREATE TABLE backup_check (v text); INSERT INTO backup_check VALUES ('kept')")
	require.NoError(t, err)

	admin := adminserver.NewMultiadminServer(setup.TopoServer, slog.Default(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer admin.Stop()
	shard := &multiadminpb.BackupRequest{Database: database, TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard, Type: "full"}

	resp, err := admin.Backup(t.Context(), shard)
	require.NoError(t, err, "a static leader is backed up without force primary")
	var backupID string
	require.Eventually(t, func() bool {
		status, err := admin.GetBackupJobStatus(t.Context(), &multiadminpb.GetBackupJobStatusRequest{JobId: resp.JobId})
		require.NoError(t, err)
		require.NotEqual(t, multiadminpb.JobStatus_JOB_STATUS_FAILED, status.Status, status.ErrorMessage)
		backupID = status.BackupId
		return status.Status == multiadminpb.JobStatus_JOB_STATUS_COMPLETED
	}, 5*time.Minute, time.Second)

	backups, err := admin.GetBackups(t.Context(), &multiadminpb.GetBackupsRequest{Database: database, TableGroup: shard.TableGroup, Shard: shard.Shard, Limit: 10})
	require.NoError(t, err)
	var ids []string
	for _, b := range backups.Backups {
		ids = append(ids, b.BackupId)
	}
	assert.Contains(t, ids, backupID)
	_, err = admin.VerifyBackups(t.Context(), &multiadminpb.VerifyBackupsRequest{Database: database, TableGroup: shard.TableGroup, Shard: shard.Shard})
	require.NoError(t, err)

	// Lose the data directory: the static leader restores the latest backup
	// and promotes itself.
	setup.Minigres.TerminateGracefully(t.Logf, 10*time.Second)
	setup.Pgctld.StopPostgres(t)
	require.NoError(t, os.RemoveAll(filepath.Join(setup.Pgctld.PoolerDir, "pg_data")))
	require.NoError(t, setup.Minigres.Start(t.Context(), t))
	setup.WaitForQueryServing(t)

	var v string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT v FROM backup_check").Scan(&v))
	assert.Equal(t, "kept", v)
}
