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

package connpoolmanager

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// startDrainTestPostgres starts a postgres for the test, reachable over a unix
// socket with trust authentication, and stops it when the test ends. It
// returns the socket file.
func startDrainTestPostgres(t *testing.T) string {
	t.Helper()
	// /tmp keeps the socket path under the 104-byte macOS limit.
	dir, err := os.MkdirTemp("/tmp", "drain_pg_test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	dataDir := filepath.Join(dir, "data")
	socketDir := filepath.Join(dir, "sock")
	require.NoError(t, os.MkdirAll(socketDir, 0o700))
	env := append(os.Environ(), "LC_ALL=C")

	initdb := exec.Command("initdb", "-D", dataDir, "-U", "postgres", "--no-instructions", "-A", "trust")
	initdb.Env = env
	out, err := initdb.CombinedOutput()
	require.NoError(t, err, "initdb: %s", out)

	logFile := filepath.Join(dir, "postgres.log")
	start := exec.Command("pg_ctl", "start", "-D", dataDir, "-l", logFile, "-w", "-t", "30",
		"-o", fmt.Sprintf("-c listen_addresses= -c unix_socket_directories=%s -c logging_collector=off", socketDir))
	start.Env = env
	out, err = start.CombinedOutput()
	if err != nil {
		pgLog, _ := os.ReadFile(logFile)
		require.NoError(t, err, "pg_ctl start: %s\n%s", out, pgLog)
	}
	t.Cleanup(func() {
		stop := exec.Command("pg_ctl", "stop", "-D", dataDir, "-m", "immediate")
		stop.Env = env
		_ = stop.Run()
	})
	return filepath.Join(socketDir, ".s.PGSQL.5432")
}

// newPGDrainTestManager opens a Manager on the test postgres.
func newPGDrainTestManager(t *testing.T, socket string) *Manager {
	t.Helper()
	config := NewConfig(viperutil.NewRegistry())
	resolveTestPgPassword(t, config)
	m := config.NewManager(slog.New(slog.DiscardHandler))
	m.Open(context.Background(), &ConnectionConfig{SocketFile: socket, Database: "postgres"})
	t.Cleanup(m.Close)
	return m
}

// adminScalar runs query on an admin connection and returns the first column
// of its first row.
func adminScalar(t *testing.T, m *Manager, query string) string {
	t.Helper()
	conn, err := m.GetAdminConn(t.Context())
	require.NoError(t, err)
	defer conn.Recycle()
	results, err := conn.Conn.StatelessQuery(t.Context(), query)
	require.NoError(t, err, query)
	require.NotEmpty(t, results)
	rows := results[0].StructuredRows()
	require.NotEmpty(t, rows, query)
	return string(rows[0].Values[0])
}

// adminExec runs statement on an admin connection.
func adminExec(t *testing.T, m *Manager, statement string) {
	t.Helper()
	conn, err := m.GetAdminConn(t.Context())
	require.NoError(t, err)
	defer conn.Recycle()
	_, err = conn.Conn.StatelessQuery(t.Context(), statement)
	require.NoError(t, err, statement)
}

// backendAlive reports whether pid is a live backend.
func backendAlive(t *testing.T, m *Manager, pid uint32) bool {
	t.Helper()
	return adminScalar(t, m, fmt.Sprintf("SELECT count(*) FROM pg_stat_activity WHERE pid = %d", pid)) != "0"
}

// The drain's pool operations against a real postgres: sealing refuses work
// and wakes waiters, KillLent ends what is lent and returns only once the
// backends are gone, and the exemptions hold.
func TestDrainAgainstPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a postgres")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries not on PATH")
	}
	socket := startDrainTestPostgres(t)
	const user = "postgres"

	t.Run("a sealed pool refuses requests and wakes its waiters", func(t *testing.T) {
		m := newPGDrainTestManager(t, socket)
		ctx := t.Context()

		// Saturate the user's regular pool, so the next request waits.
		var held []interface{ Recycle() }
		for range 64 {
			getCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			conn, err := m.GetRegularConn(getCtx, user, nil, nil)
			cancel()
			if err != nil {
				break
			}
			held = append(held, conn)
		}
		require.NotEmpty(t, held)
		waited := make(chan error, 1)
		go func() {
			conn, err := m.GetRegularConn(ctx, user, nil, nil)
			if err == nil {
				conn.Recycle()
			}
			waited <- err
		}()
		time.Sleep(100 * time.Millisecond) // the request is waiting for a connection

		m.SealPools(mterrors.MTF02.New())
		select {
		case err := <-waited:
			assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "the waiter gets the seal's error: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("sealing did not wake the waiting request")
		}
		_, err := m.GetRegularConn(ctx, user, nil, nil)
		assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "%v", err)
		_, err = m.NewReservedConn(ctx, nil, user, nil, nil)
		assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "%v", err)
		assert.Equal(t, "1", adminScalar(t, m, "SELECT 1"), "the admin pool is never sealed")

		for _, conn := range held {
			conn.Recycle()
		}
		drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, m.WaitForDrain(drainCtx), "returned connections leave a sealed pool drained")

		m.UnsealPools()
		conn, err := m.GetRegularConn(ctx, user, nil, nil)
		require.NoError(t, err, "an unsealed pool lends again")
		conn.Recycle()
	})

	t.Run("a reservation waiting for a connection is refused, never served", func(t *testing.T) {
		m := newPGDrainTestManager(t, socket)
		ctx := t.Context()

		// Saturate the user's reserved pool, so the next reservation (a BEGIN
		// admitted before a drain, say) waits for a connection.
		var held []*reserved.Conn
		for range 64 {
			getCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			rc, err := m.NewReservedConn(getCtx, nil, user, nil, nil)
			cancel()
			if err != nil {
				break
			}
			held = append(held, rc)
		}
		require.NotEmpty(t, held)
		waited := make(chan error, 1)
		go func() {
			rc, err := m.NewReservedConn(ctx, nil, user, nil, nil)
			if err == nil {
				rc.Release(reserved.ReleaseCommit, nil)
			}
			waited <- err
		}()
		time.Sleep(100 * time.Millisecond)

		m.SealPools(mterrors.MTF02.New())
		killed, err := m.KillLent(ctx, 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, len(held), killed, "the reservations already made are killed")
		select {
		case err := <-waited:
			assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID),
				"the waiting reservation gets the seal's error, not a freed connection: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("the waiting reservation was neither refused nor served")
		}
		for _, rc := range held {
			assert.False(t, backendAlive(t, m, rc.ProcessID()))
		}
		drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, m.WaitForDrain(drainCtx))
		m.UnsealPools()
	})

	t.Run("KillLent ends a running single query before it returns", func(t *testing.T) {
		m := newPGDrainTestManager(t, socket)
		ctx := t.Context()
		conn, err := m.GetRegularConn(ctx, user, nil, nil)
		require.NoError(t, err)
		pid := conn.Conn.ProcessID()
		queried := make(chan error, 1)
		go func() {
			_, err := conn.Conn.Query(ctx, "SELECT pg_sleep(60)")
			queried <- err
		}()
		require.Eventually(t, func() bool {
			return adminScalar(t, m, fmt.Sprintf("SELECT count(*) FROM pg_stat_activity WHERE pid = %d AND state = 'active'", pid)) == "1"
		}, 5*time.Second, 20*time.Millisecond, "the query is running")

		m.SealPools(mterrors.MTF01.New())
		killed, err := m.KillLent(ctx, 5*time.Second)
		require.NoError(t, err, "the kill is confirmed")
		assert.Equal(t, 1, killed)
		assert.False(t, backendAlive(t, m, pid), "the backend is gone by the time KillLent returns")

		select {
		case err := <-queried:
			assert.Error(t, err, "the killed query fails")
		case <-time.After(5 * time.Second):
			t.Fatal("the killed query did not return")
		}
		conn.Recycle()
		drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, m.WaitForDrain(drainCtx))
		m.UnsealPools()
	})

	t.Run("KillLent ends an open transaction, which commits nothing", func(t *testing.T) {
		m := newPGDrainTestManager(t, socket)
		ctx := t.Context()
		adminExec(t, m, "CREATE TABLE IF NOT EXISTS drain_kill_t (v int)")
		rc, err := m.NewReservedConn(ctx, nil, user, nil, nil)
		require.NoError(t, err)
		pid := rc.ProcessID()
		_, err = rc.Query(ctx, "BEGIN; INSERT INTO drain_kill_t VALUES (1)")
		require.NoError(t, err)

		m.SealPools(mterrors.MTF01.New())
		killed, err := m.KillLent(ctx, 5*time.Second)
		require.NoError(t, err)
		assert.Equal(t, 1, killed)
		assert.False(t, backendAlive(t, m, pid))
		_, ok := m.GetReservedConn(rc.ConnID(), user)
		assert.False(t, ok, "the killed reserved connection is gone from the pool")
		assert.Equal(t, "0", adminScalar(t, m, "SELECT count(*) FROM drain_kill_t"), "nothing of the transaction committed")

		drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, m.WaitForDrain(drainCtx), "the killed reservation is released")
		m.UnsealPools()
	})

	t.Run("KillLent keeps logical-replication tunnels", func(t *testing.T) {
		m := newPGDrainTestManager(t, socket)
		ctx := t.Context()
		tunnel, err := m.NewLogicalReplicationConn(ctx, user, nil, nil)
		require.NoError(t, err)
		defer tunnel.Release(reserved.ReleaseError, nil)
		pid := tunnel.ProcessID()
		require.True(t, backendAlive(t, m, pid))

		m.SealPools(mterrors.MTF01.New())
		killed, err := m.KillLent(ctx, 5*time.Second)
		require.NoError(t, err)
		assert.Zero(t, killed)
		assert.True(t, backendAlive(t, m, pid), "the tunnel's backend survives the drain")
		m.UnsealPools()
	})

	t.Run("terminating a backend that does not exist counts as gone", func(t *testing.T) {
		m := newPGDrainTestManager(t, socket)
		conn, err := m.GetAdminConn(t.Context())
		require.NoError(t, err)
		defer conn.Recycle()
		gone, err := conn.Conn.TerminateBackendAndWait(t.Context(), 1, time.Second)
		require.NoError(t, err)
		assert.True(t, gone, "pg_terminate_backend returns false for a pid that is no backend; pg_stat_activity confirms it is gone")
	})
}
