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

package migration

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// pgBinDirs are the places a PostgreSQL install is looked for when initdb is not
// on PATH.
var pgBinDirs = []string{"/opt/homebrew/opt/postgresql@18/bin", "/usr/lib/postgresql/18/bin", "/usr/lib/postgresql/17/bin"}

// startTestPostgres initializes and starts a throwaway PostgreSQL listening on a
// free TCP port, returning the port. The test is skipped under -short or when no
// PostgreSQL binaries are installed.
func startTestPostgres(t *testing.T) int {
	t.Helper()
	if testing.Short() {
		t.Skip("needs a real PostgreSQL; skipped under -short")
	}
	bin := ""
	if p, err := exec.LookPath("initdb"); err == nil {
		bin = filepath.Dir(p)
	} else {
		for _, d := range pgBinDirs {
			if _, err := os.Stat(filepath.Join(d, "initdb")); err == nil {
				bin = d
				break
			}
		}
	}
	if bin == "" {
		t.Skip("PostgreSQL binaries not found")
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	out, err := exec.Command(filepath.Join(bin, "initdb"), "-D", data, "-U", "postgres", "--auth=trust", "-E", "UTF8", "--locale=C").CombinedOutput()
	require.NoError(t, err, string(out))
	conf := fmt.Sprintf("\nport = %d\nlisten_addresses = '127.0.0.1'\nunix_socket_directories = ''\nfsync = off\n", port)
	f, err := os.OpenFile(filepath.Join(data, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString(conf)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	logFile := filepath.Join(dir, "pg.log")
	// LC_ALL must name a valid locale or the postmaster aborts on macOS.
	start := exec.Command(filepath.Join(bin, "pg_ctl"), "-D", data, "-l", logFile, "-w", "start")
	start.Env = append(os.Environ(), "LC_ALL=C")
	out, err = start.CombinedOutput()
	if err != nil {
		log, _ := os.ReadFile(logFile)
		t.Fatalf("pg_ctl start: %v\n%s\n%s", err, out, log)
	}
	t.Cleanup(func() {
		_ = exec.Command(filepath.Join(bin, "pg_ctl"), "-D", data, "-m", "immediate", "stop").Run()
	})
	return port
}

func pgDSN(port int, role, db string) string {
	return fmt.Sprintf("host=127.0.0.1 port=%d user=%s dbname=%s sslmode=disable", port, role, db)
}

// TestSourceSubscriptionStatus_RealPostgres covers SubscriptionStatus against a
// real server: the query aggregates with no GROUP BY, so even a subscription
// name that doesn't exist must come back as a single zero-valued,
// not-caught-up row rather than an error or a missing row — the EXPORT
// reverse link's tick-driven check relies on that to avoid special-casing a
// subscription that hasn't attached yet.
func TestSourceSubscriptionStatus_RealPostgres(t *testing.T) {
	port := startTestPostgres(t)
	ctx := context.Background()

	src, err := newSource(ctx, infoOf(pgDSN(port, "postgres", "postgres")))
	require.NoError(t, err)
	t.Cleanup(src.close)

	st, err := src.SubscriptionStatus("no_such_subscription")
	require.NoError(t, err)
	require.Equal(t, int64(0), st.TotalRelations)
	require.Equal(t, int64(0), st.ReadyRelations)
	require.False(t, st.CaughtUp)
}

// TestSourceFencing_RealPostgres drives the source's quiesce primitives against
// a real server, since their effect (who can connect, what survives a
// terminate) only exists in PostgreSQL itself.
func TestSourceFencing_RealPostgres(t *testing.T) {
	port := startTestPostgres(t)
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, pgDSN(port, "postgres", "postgres"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close(ctx) })
	run := func(sql string) {
		t.Helper()
		_, err := admin.Exec(ctx, sql)
		require.NoError(t, err, sql)
	}
	for _, sql := range []string{
		"CREATE DATABASE appdb",
		"CREATE ROLE app LOGIN",
		"CREATE ROLE reader LOGIN",
		"CREATE ROLE su LOGIN SUPERUSER",
		"CREATE ROLE grp NOLOGIN",
		"CREATE ROLE member LOGIN IN ROLE grp",
	} {
		run(sql)
	}
	// reset puts every fixture back to the default: PUBLIC holds CONNECT, no role
	// has its own grant, and no connection limits are set.
	reset := func() {
		run("GRANT CONNECT ON DATABASE appdb TO PUBLIC")
		for _, r := range []string{"app", "reader", "grp", "member"} {
			run("REVOKE CONNECT ON DATABASE appdb FROM " + r)
		}
		run("ALTER ROLE app CONNECTION LIMIT -1")
		run("ALTER ROLE member CONNECTION LIMIT -1")
	}
	canConnect := func(role string) bool {
		c, err := pgx.Connect(ctx, pgDSN(port, role, "appdb"))
		if err != nil {
			return false
		}
		_ = c.Close(ctx)
		return true
	}

	src, err := newSource(ctx, infoOf(pgDSN(port, "postgres", "appdb")))
	require.NoError(t, err)
	t.Cleanup(src.close)

	t.Run("reads the pre-revoke state", func(t *testing.T) {
		reset()
		has, err := src.PublicHasConnect()
		require.NoError(t, err)
		require.True(t, has)

		hadConnect, err := src.RolesHadConnect([]string{"app", "ghost"})
		require.NoError(t, err)
		require.Equal(t, map[string]bool{"app": false}, hadConnect, "PUBLIC-only access is not a personal grant, and a missing role is absent")
		run("GRANT CONNECT ON DATABASE appdb TO app")
		hadConnect, err = src.RolesHadConnect([]string{"app"})
		require.NoError(t, err)
		require.Equal(t, map[string]bool{"app": true}, hadConnect)

		run("ALTER ROLE app CONNECTION LIMIT 7")
		limits, err := src.RoleConnLimits([]string{"app", "reader"})
		require.NoError(t, err)
		require.Equal(t, map[string]int32{"app": 7, "reader": -1}, limits)
	})

	t.Run("revoke fences PUBLIC and named roles and spares the migrator", func(t *testing.T) {
		reset()
		run("GRANT CONNECT ON DATABASE appdb TO app")
		require.True(t, canConnect("app"))

		require.NoError(t, src.RevokeConnect([]string{"app"}))

		require.False(t, canConnect("app"), "a named role is fenced")
		require.False(t, canConnect("reader"), "a PUBLIC-only role is fenced")
		require.True(t, canConnect("postgres"), "the migrator's own role keeps CONNECT")
		require.True(t, canConnect("su"), "a superuser bypasses CONNECT")
		has, err := src.PublicHasConnect()
		require.NoError(t, err)
		require.False(t, has)
		limits, err := src.RoleConnLimits([]string{"app"})
		require.NoError(t, err)
		require.Equal(t, map[string]int32{"app": 0}, limits)

		require.NoError(t, src.GrantConnect([]string{"app"}, true, map[string]int32{"app": 7}))

		require.True(t, canConnect("app"))
		require.True(t, canConnect("reader"), "PUBLIC is restored when it had CONNECT")
		limits, err = src.RoleConnLimits([]string{"app"})
		require.NoError(t, err)
		require.Equal(t, map[string]int32{"app": 7}, limits, "the original limit comes back, not unlimited")
	})

	t.Run("restore does not re-open PUBLIC that was already closed", func(t *testing.T) {
		reset()
		run("REVOKE CONNECT ON DATABASE appdb FROM PUBLIC")
		run("GRANT CONNECT ON DATABASE appdb TO app")
		require.NoError(t, src.RevokeConnect([]string{"app"}))

		require.NoError(t, src.GrantConnect([]string{"app"}, false, map[string]int32{"app": -1}))

		require.True(t, canConnect("app"))
		require.False(t, canConnect("reader"), "PUBLIC stays closed")
	})

	t.Run("a role with only group-inherited CONNECT gets its limit back", func(t *testing.T) {
		reset()
		run("REVOKE CONNECT ON DATABASE appdb FROM PUBLIC")
		run("GRANT CONNECT ON DATABASE appdb TO grp")
		require.True(t, canConnect("member"), "member inherits CONNECT from grp")

		require.NoError(t, src.RevokeConnect([]string{"member"}))
		require.False(t, canConnect("member"), "CONNECTION LIMIT 0 closes the inherited-grant gap")

		// Nothing to re-grant and PUBLIC stays closed, but the limit must still be restored.
		require.NoError(t, src.GrantConnect(nil, false, map[string]int32{"member": -1}))
		require.True(t, canConnect("member"))
	})

	t.Run("validates the quiesce roles", func(t *testing.T) {
		reset()
		require.NoError(t, src.checkQuiesceRoles(nil))
		require.NoError(t, src.checkQuiesceRoles([]string{"app", "reader"}))
		require.ErrorContains(t, src.checkQuiesceRoles([]string{"postgres"}), "own role")
		require.ErrorContains(t, src.checkQuiesceRoles([]string{"ghost"}), "do not exist")
		require.ErrorContains(t, src.checkQuiesceRoles([]string{"app", "su"}), "superusers")
	})

	t.Run("terminate waits for client backends and spares the migrator", func(t *testing.T) {
		reset()
		client, err := pgx.Connect(ctx, pgDSN(port, "app", "appdb"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close(ctx) })
		var pid int
		require.NoError(t, client.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))

		require.NoError(t, src.TerminateClientBackends())

		var remaining int
		require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", pid).Scan(&remaining))
		require.Zero(t, remaining, "TerminateClientBackends must not return before the backend has exited")
		_, err = src.conn.Exec(ctx, "SELECT 1")
		require.NoError(t, err, "the migrator's own connection survives")
	})

	t.Run("read-only default is set and reset cluster-wide", func(t *testing.T) {
		reset()
		readOnly := func() string {
			c, err := pgx.Connect(ctx, pgDSN(port, "postgres", "appdb"))
			require.NoError(t, err)
			defer c.Close(ctx)
			var v string
			require.NoError(t, c.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&v))
			return v
		}

		require.NoError(t, src.SetReadOnly(true))
		require.Eventually(t, func() bool { return readOnly() == "on" }, 10*time.Second, 50*time.Millisecond)

		require.NoError(t, src.SetReadOnly(false))
		require.Eventually(t, func() bool { return readOnly() == "off" }, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("a revoke by a role that cannot revoke is reported, not swallowed", func(t *testing.T) {
		reset()
		run("CREATE ROLE limited LOGIN")
		// Neither superuser nor the database owner: Postgres only warns that no
		// privileges could be revoked, so RevokeConnect must notice by itself.
		weak, err := newSource(ctx, infoOf(pgDSN(port, "limited", "appdb")))
		require.NoError(t, err)
		t.Cleanup(weak.close)

		err = weak.RevokeConnect(nil)

		require.ErrorContains(t, err, "PUBLIC still holds CONNECT")
		require.True(t, canConnect("reader"), "nothing was fenced, and the error says so")
	})

	t.Run("every primitive reports a lost connection", func(t *testing.T) {
		dead, err := newSource(ctx, infoOf(pgDSN(port, "postgres", "appdb")))
		require.NoError(t, err)
		require.NoError(t, dead.conn.Close(ctx))

		_, err = dead.PublicHasConnect()
		require.Error(t, err)
		_, err = dead.RolesHadConnect([]string{"app"})
		require.Error(t, err)
		_, err = dead.RoleConnLimits([]string{"app"})
		require.Error(t, err)
		require.Error(t, dead.RevokeConnect([]string{"app"}))
		require.Error(t, dead.GrantConnect([]string{"app"}, true, nil))
		require.Error(t, dead.GrantConnect(nil, false, map[string]int32{"app": -1}))
		require.Error(t, dead.SetConnLimits(map[string]int32{"app": 1}))
		require.Error(t, dead.checkQuiesceRoles([]string{"app"}))
		require.Error(t, dead.TerminateClientBackends())
		require.Error(t, dead.SetReadOnly(true))
		require.Error(t, dead.setSessionReadOnly(true))
	})
}
