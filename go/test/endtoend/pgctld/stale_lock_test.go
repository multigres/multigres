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

package pgctld

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// TestStartAfterUncleanKill_RecoversFromStaleLock is a regression test
// covering the case where PostgreSQL is killed uncleanly (SIGKILL, leaving a
// stale postmaster.pid): pgctld's own Start RPC must recover on its own,
// without needing an external caller to retry. It reproduces, without any
// artificial delay, the ~1-5s orphan-cleanup window during which pg_ctl
// start genuinely (but transiently) fails with `FATAL: lock file
// "postmaster.pid" already exists`.
func TestStartAfterUncleanKill_RecoversFromStaleLock(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_stale_lock_test")
	defer cleanup()

	poolerDir := filepath.Join(tempDir, "pooler")
	configFile := filepath.Join(tempDir, ".pgctld.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("log-level: info\ntimeout: 30\n"), 0o644))
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))

	srv := startPgCtldServer(t, poolerDir, configFile)
	grpcAddr := fmt.Sprintf("localhost:%d", srv.GrpcPort)

	require.NoError(t, InitAndStartPostgreSQL(t, grpcAddr))

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := pb.NewPgCtldClient(conn)

	// Kill the real postmaster uncleanly, exactly as an evicted/rescheduled
	// pod would leave it: postmaster.pid stays behind with the now-dead PID,
	// and orphaned children (checkpointer, walreceiver, etc.) briefly keep
	// the data-directory lock held after the postmaster itself is gone.
	pgDataDir := filepath.Join(poolerDir, "pg_data")
	pgPID, err := readPostmasterPID(pgDataDir)
	require.NoError(t, err)
	t.Logf("PostgreSQL PID: %d", pgPID)

	pgProcess, err := os.FindProcess(pgPID)
	require.NoError(t, err)

	killCtx, killCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer killCancel()
	_, _ = executil.KillPID(killCtx, pgPID)

	require.Eventually(t, func() bool {
		return pgProcess.Signal(syscall.Signal(0)) != nil
	}, 5*time.Second, 50*time.Millisecond, "postmaster should terminate after SIGKILL")

	// No sleep here on purpose: the whole point is that pgctld's Start RPC
	// must absorb the orphan-cleanup race on its own, with no help from an
	// external retry loop (unlike MonitorPostgres's incidental 5s polling in
	// production).
	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()

	startTime := time.Now()
	resp, err := client.Start(startCtx, &pb.StartRequest{AsPrimary: true})
	require.NoError(t, err, "Start must recover from the stale lock without external retries")
	t.Logf("recovered from stale postmaster.pid in %v (pid=%d, message=%s)",
		time.Since(startTime), resp.Pid, resp.Message)
}
