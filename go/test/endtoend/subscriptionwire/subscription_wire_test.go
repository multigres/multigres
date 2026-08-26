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

// Package subscriptionwire verifies, against real PostgreSQL, exactly how a
// logical-replication subscriber puts `failover = true` on the wire when it
// creates its slot. The multigateway's replication preamble guard (which admits
// a persistent slot only when it is registered for failover) depends on this:
// if the subscriber carries FAILOVER inside the CREATE_REPLICATION_SLOT command
// then the command-form guard can admit it at creation time; if instead the
// subscriber creates a plain slot and enables failover afterward via
// ALTER_REPLICATION_SLOT, the guard would reject the CREATE and needs to
// understand the create+alter pair. This test settles that empirically rather
// than by assumption, and fails loudly if a future PostgreSQL changes it.
package subscriptionwire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

const (
	// subscriptionName is also the default slot name PostgreSQL uses for the
	// subscription's main (non-temporary) slot.
	subscriptionName = "sub_orders"
	publicationName  = "orders_pub"
	pgSuperuser      = "postgres"
)

// TestCreateSubscriptionFailoverWireBehavior stands up a real publisher and
// subscriber, runs CREATE SUBSCRIPTION ... WITH (failover = true), and inspects
// the publisher's logged replication commands to confirm the subscriber sends
// FAILOVER inside CREATE_REPLICATION_SLOT (not via a later ALTER_REPLICATION_SLOT).
func TestCreateSubscriptionFailoverWireBehavior(t *testing.T) {
	skipUnlessPostgres17(t)

	ctx := utils.WithTimeout(t, 90*time.Second)

	// Publisher: wal_level=logical for logical decoding, and
	// log_replication_commands=on so every replication-protocol command it
	// receives is written verbatim to the server log — our observation point.
	publisher := startPostgres(
		t, ctx, "publisher",
		"wal_level = logical",
		"log_replication_commands = on",
		"max_wal_senders = 10",
		"max_replication_slots = 10",
	)
	// Subscriber is an ordinary primary; wal_level=logical is harmless and keeps
	// the two configs uniform.
	subscriber := startPostgres(
		t, ctx, "subscriber",
		"wal_level = logical",
	)

	// Matching table on both sides; publication on the publisher.
	pubConn := connect(t, ctx, publisher.port)
	defer pubConn.Close(ctx)
	mustExec(t, ctx, pubConn, `CREATE TABLE orders (id int PRIMARY KEY, note text)`)
	mustExec(t, ctx, pubConn, `CREATE PUBLICATION `+publicationName+` FOR TABLE orders`)

	subConn := connect(t, ctx, subscriber.port)
	defer subConn.Close(ctx)
	mustExec(t, ctx, subConn, `CREATE TABLE orders (id int PRIMARY KEY, note text)`)

	// The subject under test. create_slot defaults to true, so this command
	// itself connects to the publisher and creates the subscription's main slot
	// before returning — the CREATE_REPLICATION_SLOT we want to observe.
	conninfo := fmt.Sprintf("host=localhost port=%d user=%s dbname=postgres", publisher.port, pgSuperuser)
	mustExec(t, ctx, subConn, fmt.Sprintf(
		`CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s WITH (failover = true, copy_data = true)`,
		subscriptionName, conninfo, publicationName,
	))

	// The main slot exists on the publisher once CREATE SUBSCRIPTION returns;
	// poll to be robust against any lag, then read its recorded state.
	temporary, failover := waitForSlot(t, ctx, pubConn, subscriptionName)
	assert.False(t, temporary, "the subscription's main slot must be persistent (non-temporary)")
	assert.True(t, failover, "WITH (failover = true) must produce a failover slot on the publisher")

	// Read the publisher's log and extract the replication commands it received.
	cmds := receivedReplicationCommands(t, publisher.logPath)
	require.NotEmpty(t, cmds, "publisher should have logged replication commands (is log_replication_commands on?)")
	t.Logf("publisher received %d replication command(s):", len(cmds))
	for i, c := range cmds {
		t.Logf("  [%d] %s", i, c)
	}

	// Find the CREATE_REPLICATION_SLOT for the subscription's main slot (the
	// non-temporary one; tablesync workers create separate TEMPORARY slots).
	var mainCreate string
	for _, c := range cmds {
		u := strings.ToUpper(c)
		if strings.Contains(u, "CREATE_REPLICATION_SLOT") &&
			strings.Contains(c, subscriptionName) &&
			!strings.Contains(u, "TEMPORARY") {
			mainCreate = c
			break
		}
	}
	require.NotEmpty(t, mainCreate,
		"expected a non-temporary CREATE_REPLICATION_SLOT for %q among the received commands", subscriptionName)

	// The claim the gateway guard relies on: FAILOVER rides inside the
	// CREATE_REPLICATION_SLOT command, so it can be admitted at creation time.
	assert.Contains(t, strings.ToUpper(mainCreate), "FAILOVER",
		"CREATE_REPLICATION_SLOT for the failover subscription must carry FAILOVER; "+
			"if this fails, the subscriber sets failover some other way (e.g. ALTER_REPLICATION_SLOT) "+
			"and the preamble guard must account for that")

	// Informational: report whether an ALTER_REPLICATION_SLOT also appeared, so
	// a change in mechanism is visible in the test output even when the assert
	// above still holds.
	for _, c := range cmds {
		if strings.Contains(strings.ToUpper(c), "ALTER_REPLICATION_SLOT") {
			t.Logf("note: publisher also received an ALTER_REPLICATION_SLOT: %s", c)
		}
	}
}

// TestTemporaryFailoverSlotRejected pins PostgreSQL's own rejection of the
// contradictory temporary + failover combination: a temporary slot is dropped
// when its session ends, so it can never be a failover target. The multigateway
// preamble deliberately does not special-case this — it admits any TEMPORARY
// slot and lets PostgreSQL enforce the contradiction — so this guards the
// assumption that PostgreSQL still rejects it, asserting the exact error message
// PostgreSQL returns.
func TestTemporaryFailoverSlotRejected(t *testing.T) {
	skipUnlessPostgres17(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	pg := startPostgres(t, ctx, "pg", "wal_level = logical")
	conn := connect(t, ctx, pg.port)
	defer conn.Close(ctx)

	// temporary => true together with failover => true.
	_, err := conn.Exec(ctx,
		`SELECT pg_create_logical_replication_slot('t_tempfail', 'pgoutput', true, false, true)`)
	require.Error(t, err)

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "cannot enable failover for a temporary replication slot", pgErr.Message)
}

// TestAutoMarkedFailoverSlotSurvivesSubscriptionReconnect is the empirical
// confirmation of the auto-marking correctness claim: a slot the
// gateway made a failover slot without the subscriber asking keeps
// failover=true even though the subscription's own subfailover=false, and that
// mismatch survives a subscription reconnect.
//
// It reproduces the auto-marked end state directly against a real publisher —
// pre-creating the subscription's main slot as a failover slot (what the
// gateway's rewritten CREATE_REPLICATION_SLOT produces) and attaching a plain
// CREATE SUBSCRIPTION (failover defaults to false, create_slot = false) to it —
// so no gateway is needed to prove the PostgreSQL-side behavior the feature
// relies on. The gateway rewrite itself is covered by the multigateway handler
// unit tests.
func TestAutoMarkedFailoverSlotSurvivesSubscriptionReconnect(t *testing.T) {
	skipUnlessPostgres17(t)

	ctx := utils.WithTimeout(t, 90*time.Second)

	publisher := startPostgres(
		t, ctx, "publisher",
		"wal_level = logical",
		"max_wal_senders = 10",
		"max_replication_slots = 10",
	)
	subscriber := startPostgres(
		t, ctx, "subscriber",
		"wal_level = logical",
	)

	pubConn := connect(t, ctx, publisher.port)
	defer pubConn.Close(ctx)
	mustExec(t, ctx, pubConn, `CREATE TABLE orders (id int PRIMARY KEY, note text)`)
	mustExec(t, ctx, pubConn, `CREATE PUBLICATION `+publicationName+` FOR TABLE orders`)

	// Simulate the gateway's auto-marking: create the subscription's main slot
	// as a failover slot up front, exactly the end state the rewritten
	// CREATE_REPLICATION_SLOT ... (FAILOVER, ...) command yields.
	mustExec(t, ctx, pubConn, fmt.Sprintf(
		`SELECT pg_create_logical_replication_slot('%s', 'pgoutput', false, false, true)`, subscriptionName,
	))

	temporary, failover := waitForSlot(t, ctx, pubConn, subscriptionName)
	require.False(t, temporary, "auto-marked slot must be persistent")
	require.True(t, failover, "auto-marked slot must be a failover slot")

	subConn := connect(t, ctx, subscriber.port)
	defer subConn.Close(ctx)
	mustExec(t, ctx, subConn, `CREATE TABLE orders (id int PRIMARY KEY, note text)`)

	// A plain subscriber — no WITH (failover = true) — attached to the
	// pre-created failover slot. subfailover is therefore false while the slot
	// is failover=true: the deliberate mismatch the feature creates.
	conninfo := fmt.Sprintf("host=localhost port=%d user=%s dbname=postgres", publisher.port, pgSuperuser)
	mustExec(t, ctx, subConn, fmt.Sprintf(
		`CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s WITH (failover = false, create_slot = false, copy_data = true)`,
		subscriptionName, conninfo, publicationName,
	))

	var subFailover bool
	require.NoError(t, subConn.QueryRow(ctx,
		`SELECT subfailover FROM pg_subscription WHERE subname = $1`, subscriptionName).Scan(&subFailover))
	require.False(t, subFailover, "subscription must not have opted into failover (that is the point)")

	// Force the apply worker to tear down and reconnect. Per PostgreSQL's
	// design the only thing that resets a slot's failover is an explicit
	// ALTER SUBSCRIPTION ... SET (failover = ...); a plain reconnect must not.
	mustExec(t, ctx, subConn, fmt.Sprintf(`ALTER SUBSCRIPTION %s DISABLE`, subscriptionName))
	mustExec(t, ctx, subConn, fmt.Sprintf(`ALTER SUBSCRIPTION %s ENABLE`, subscriptionName))

	// The slot must still be a failover slot after the reconnect.
	temporary, failover = waitForSlot(t, ctx, pubConn, subscriptionName)
	assert.False(t, temporary, "slot must stay persistent across the subscription reconnect")
	assert.True(t, failover, "auto-marked failover flag must survive the subscription reconnect despite subfailover=false")

	// Clean up the subscription so its slot is released before the publisher is
	// torn down (DROP SUBSCRIPTION drops the remote slot).
	mustExec(t, ctx, subConn, `DROP SUBSCRIPTION `+subscriptionName)
}

// skipUnlessPostgres17 skips the test unless PostgreSQL 17+ binaries are on PATH.
func skipUnlessPostgres17(t *testing.T) {
	t.Helper()
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries (initdb/postgres/pg_ctl/pg_isready) not on PATH")
	}
	if major := postgresMajorVersion(t); major < 17 {
		t.Skipf("logical-slot failover needs PostgreSQL 17+, found major version %d", major)
	}
}

// pgInstance is a running standalone PostgreSQL instance under a temp datadir.
type pgInstance struct {
	dir     string
	port    int
	logPath string
}

// startPostgres initdb's a fresh cluster, applies extraConf, starts it with
// pg_ctl (server log redirected to a file), and registers cleanup that stops it.
func startPostgres(t *testing.T, ctx context.Context, name string, extraConf ...string) *pgInstance {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	dataDir := filepath.Join(dir, "data")
	logPath := filepath.Join(dir, "postgres.log")
	port := utils.GetFreePort(t)

	// Pin the locale/encoding so initdb does not depend on the caller's LANG /
	// LC_* environment (unset in some test/CI shells, which makes initdb fail
	// with "invalid locale settings").
	run(t, ctx, "initdb", "-D", dataDir, "-U", pgSuperuser, "-A", "trust",
		"--no-instructions", "--locale=C", "--encoding=UTF8")

	conf := append([]string{
		fmt.Sprintf("port = %d", port),
		"listen_addresses = 'localhost'",
		// Disable the Unix socket: everything here connects over TCP, and a
		// temp-dir socket path easily exceeds the ~103-char sun_path limit on
		// macOS, which would keep the server from starting.
		"unix_socket_directories = ''",
		"fsync = off",
		"full_page_writes = off",
	}, extraConf...)
	appendFile(t, filepath.Join(dataDir, "postgresql.conf"), strings.Join(conf, "\n")+"\n")

	startCmd := executil.Command(ctx, "pg_ctl", "-D", dataDir, "-l", logPath, "-w", "-t", "60", "start").
		AddEnv("LC_ALL=C", "LANG=C")
	if out, err := startCmd.CombinedOutput(); err != nil {
		serverLog, _ := os.ReadFile(logPath)
		t.Fatalf("pg_ctl start failed: %v\n%s\n--- server log ---\n%s", err, out, serverLog)
	}
	t.Cleanup(func() {
		// Best-effort immediate stop; the temp dir is removed by t.TempDir.
		stop := executil.Command(context.Background(), "pg_ctl", "-D", dataDir, "-m", "immediate", "-w", "-t", "30", "stop").
			AddEnv("LC_ALL=C", "LANG=C")
		_ = stop.Run()
	})

	inst := &pgInstance{dir: dir, port: port, logPath: logPath}
	// Prove it accepts connections before returning.
	conn := connect(t, ctx, port)
	_ = conn.Close(ctx)
	return inst
}

func connect(t *testing.T, ctx context.Context, port int) *pgx.Conn {
	t.Helper()
	connStr := fmt.Sprintf("postgres://%s@localhost:%d/postgres?sslmode=disable", pgSuperuser, port)
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "connect to postgres on port %d", port)
	return conn
}

func mustExec(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(ctx, sql)
	require.NoError(t, err, "exec: %s", sql)
}

// waitForSlot polls pg_replication_slots until the named slot exists, returning
// its temporary and failover flags.
func waitForSlot(t *testing.T, ctx context.Context, conn *pgx.Conn, slot string) (temporary, failover bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := conn.QueryRow(ctx,
			`SELECT temporary, failover FROM pg_replication_slots WHERE slot_name = $1`, slot).
			Scan(&temporary, &failover)
		if err == nil {
			return temporary, failover
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			require.NoError(t, err, "query pg_replication_slots")
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot %q did not appear on the publisher within the timeout", slot)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// receivedReplicationCommands extracts the command text of every
// "received replication command: <cmd>" line from a publisher log.
func receivedReplicationCommands(t *testing.T, logPath string) []string {
	t.Helper()
	data, err := os.ReadFile(logPath)
	require.NoError(t, err, "read publisher log %s", logPath)
	const marker = "received replication command: "
	var cmds []string
	for line := range strings.SplitSeq(string(data), "\n") {
		if _, after, ok := strings.Cut(line, marker); ok {
			cmds = append(cmds, strings.TrimSpace(after))
		}
	}
	return cmds
}

func postgresMajorVersion(t *testing.T) int {
	t.Helper()
	out, err := executil.Command(context.Background(), "postgres", "--version").
		AddEnv("LC_ALL=C", "LANG=C").Output()
	require.NoError(t, err, "postgres --version")
	// e.g. "postgres (PostgreSQL) 17.10 (Homebrew)"
	for f := range strings.FieldsSeq(string(out)) {
		if maj, _, ok := strings.Cut(f, "."); ok {
			if n, err := strconv.Atoi(maj); err == nil {
				return n
			}
		}
	}
	t.Fatalf("could not parse postgres version from %q", string(out))
	return 0
}

func run(t *testing.T, ctx context.Context, name string, args ...string) {
	t.Helper()
	// AddEnv pins LC_ALL/LANG so initdb and the postmaster get a valid locale
	// even when the test shell leaves them unset (macOS postgres refuses to
	// start otherwise: "postmaster became multithreaded during startup").
	cmd := executil.Command(ctx, name, args...).AddEnv("LC_ALL=C", "LANG=C")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s %s failed: %v\n%s", name, strings.Join(args, " "), err, out)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err, "open %s", path)
	defer func() { require.NoError(t, f.Close()) }()
	_, err = f.WriteString(content)
	require.NoError(t, err, "append to %s", path)
}
