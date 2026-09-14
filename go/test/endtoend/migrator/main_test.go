// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package Multigres Migrator holds end-to-end tests for the Multigres Migrator table-migration
// coordinator, which is hosted inside multipooler and serves the Migrator gRPC
// service on the shard primary. The tests drive that service directly.
package migrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

const sourcePassword = "srcpass"

func TestMain(m *testing.M) {
	os.Exit(shardsetup.RunTestMain(m)) //nolint:forbidigo // TestMain is allowed to call os.Exit
}

// migrationClient dials the given pooler's Migrator gRPC service (insecure, as
// shardsetup runs). The returned close func releases the connection.
func migrationClient(t *testing.T, pooler *shardsetup.MultipoolerInstance) (migratorpb.MigratorClient, func()) {
	t.Helper()
	addr := fmt.Sprintf("localhost:%d", pooler.Multipooler.GrpcPort)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return migratorpb.NewMigratorClient(conn), func() { _ = conn.Close() }
}

// waitForPrimaryDB waits for a pooler's routing_state to reach PRIMARY in topo
// and returns the target database name.
func waitForPrimaryDB(t *testing.T, ctx context.Context, setup *shardsetup.ShardSetup) string {
	t.Helper()
	var db string
	require.Eventually(t, func() bool {
		infos, err := setup.TopoServer.GetMultipoolersByCell(ctx, setup.CellName, &topoclient.GetMultipoolersByCellOptions{})
		if err != nil {
			return false
		}
		for _, info := range infos {
			if info.Multipooler.GetRoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
				db = info.Multipooler.GetShardKey().GetDatabase()
				return true
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "primary routing_state must reach topo")
	require.NotEmpty(t, db, "target database")
	return db
}

// targetConn opens a pgprotocol connection to a pooler's postgres.
func targetConn(t *testing.T, ctx context.Context, pooler *shardsetup.MultipoolerInstance, db string) *client.Conn {
	t.Helper()
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        pooler.Pgctld.PgPort,
		User:        constants.DefaultPostgresUser,
		Password:    shardsetup.TestPostgresPassword,
		Database:    db,
		SSLMode:     client.SSLModeDisable,
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return conn
}

// countRows returns the row count of a table via a pgprotocol connection.
func countRows(t *testing.T, ctx context.Context, conn *client.Conn, table string) (int, bool) {
	t.Helper()
	res, err := conn.Query(ctx, "SELECT count(*) FROM "+table)
	if err != nil || len(res) == 0 || len(res[0].Rows) == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(string(res[0].Rows[0].Values[0]))
	if err != nil {
		return 0, false
	}
	return n, true
}

// startStandaloneSource initdb's and launches a throwaway postgres with
// wal_level=logical on a free port, returning that port. Torn down via cleanup.
func startStandaloneSource(t *testing.T) int {
	t.Helper()
	port := utils.GetFreePort(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	pwFile := filepath.Join(root, "pwfile")
	require.NoError(t, os.WriteFile(pwFile, []byte(sourcePassword), 0o600))

	initCmd := executil.Command(t.Context(), "initdb",
		"-D", dataDir, "-U", "postgres", "--pwfile="+pwFile,
		"--encoding=UTF8", "--locale=C",
		"--auth-local=trust", "--auth-host=scram-sha-256")
	initCmd.SetEnv(utils.BaseTestEnv())
	if out, err := initCmd.CombinedOutput(); err != nil {
		t.Fatalf("initdb failed: %v\n%s", err, out)
	}

	hba := "local all all trust\nhost all all 127.0.0.1/32 scram-sha-256\nhost all all ::1/128 scram-sha-256\n"
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "pg_hba.conf"), []byte(hba), 0o600))

	logFile, err := os.Create(filepath.Join(root, "postgres.log"))
	require.NoError(t, err)

	pg := executil.Command(context.Background(), "postgres",
		"-D", dataDir,
		"-p", strconv.Itoa(port),
		"-c", "listen_addresses=127.0.0.1",
		"-c", "unix_socket_directories=",
		"-c", "wal_level=logical",
		"-c", "max_wal_senders=10",
		"-c", "max_replication_slots=10",
		"-c", "fsync=off",
		"-c", "synchronous_commit=off",
		"-c", "full_page_writes=off",
	).WithProcessGroup()
	pg.SetEnv(utils.BaseTestEnv())
	if runtime.GOOS == "darwin" {
		pg.AddEnv("LC_ALL=en_US.UTF-8") // avoid "postmaster became multithreaded" on macOS
	}
	pg.SetStdout(logFile)
	pg.SetStderr(logFile)
	require.NoError(t, pg.Start())
	t.Cleanup(func() {
		_, _ = pg.Stop(context.Background())
		_ = logFile.Close()
	})

	require.Eventually(t, func() bool {
		c := executil.Command(t.Context(), "pg_isready", "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-U", "postgres", "-q")
		return c.Run() == nil
	}, 30*time.Second, 200*time.Millisecond, "source postgres must become ready")
	return port
}

// sourceDSN returns a libpq conninfo (scram) for the standalone source.
func sourceDSN(port int) string {
	return fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=postgres sslmode=disable", port, sourcePassword)
}

// dialSource opens a pgprotocol connection to the standalone source.
func dialSource(t *testing.T, ctx context.Context, port int) *client.Conn {
	t.Helper()
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "127.0.0.1",
		Port:        port,
		User:        "postgres",
		Password:    sourcePassword,
		Database:    "postgres",
		SSLMode:     client.SSLModeDisable,
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return conn
}

// seedSource creates public.orders with a BY DEFAULT identity primary key (so
// its sequence is exercised by AdvanceSequences at teardown) and three rows.
func seedSource(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	conn := dialSource(t, ctx, port)
	defer conn.Close()
	for _, stmt := range []string{
		"CREATE TABLE public.orders (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text)",
		"INSERT INTO public.orders (v) VALUES ('a'), ('b'), ('c')",
	} {
		_, err := conn.Query(ctx, stmt)
		require.NoError(t, err, "seed: %s", stmt)
	}
}
