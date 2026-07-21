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

package shardsetup

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/test/utils"
)

// This file is the fast-feedback oracle for the replication tunnel: it drives a
// real logical-replication client THROUGH the multigateway to postgres, exactly
// the way Realtime's Broadcast-from-Database feature does. The gateway tunnels
// a `replication=database` connection to the PRIMARY pooler, which opens a
// pinned `replication=database` backend on postgres. The pooler-direct
// equivalents live in go/test/endtoend/multipooler/logical_replication_*; here
// we prove the same sequence survives the additional gateway+pooler hop.
//
// lib/pq / database/sql cannot drive the replication sub-protocol, so the
// replication connection uses the repo's raw pgwire client
// (go/common/pgprotocol/client) with replication=database in the startup
// parameters, pointed at the GATEWAY's PG port. A separate ordinary lib/pq
// connection (openMultigatewayConn) runs the DDL / INSERT that generates WAL.

// replFixtureCounter makes table/publication/slot names unique within a run.
var replFixtureCounter atomic.Int64

// xlogDataHeaderLen is the fixed header before the WAL bytes in a 'w' frame:
// 'w' + walStart(8) + walEnd(8) + sendTime(8).
const xlogDataHeaderLen = 25

// ensureRolReplication grants rolreplication to the test user. A
// `replication=database` connection requires the role to have
// rolreplication=true: the gateway enforces this post-auth by reading
// pg_authid.rolreplication directly (admin_conn.go), NOT rolsuper, so a
// superuser without the replication attribute would be rejected. The initdb
// bootstrap superuser (DefaultTestUser="postgres") happens to be created with
// rolreplication=true, so this ALTER is idempotent today; it is kept to make
// the wire requirement explicit and to keep the test correct if the harness
// ever switches to a non-superuser test role. Run over a normal SQL connection
// before opening any replication connection.
func ensureRolReplication(t *testing.T, conn *sql.Conn) {
	t.Helper()
	ctx := utils.WithTimeout(t, 10*time.Second)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER ROLE %s REPLICATION", DefaultTestUser))
	require.NoError(t, err, "grant rolreplication to test user")
}

// setupGatewayReplicationFixture creates a fresh table + publication over a
// normal multigateway SQL connection and returns their names plus that
// connection (kept open for subsequent INSERTs / catalog queries). Each call
// uses a unique suffix so sub-tests don't collide. The connection also has
// rolreplication ensured so the replication-mode dial that follows is accepted.
func setupGatewayReplicationFixture(t *testing.T, setup *ShardSetup) (sqlConn *sql.Conn, tableName, pubName string) {
	t.Helper()
	n := replFixtureCounter.Add(1)
	tableName = fmt.Sprintf("repl_gw_%d", n)
	pubName = fmt.Sprintf("pub_gw_%d", n)

	sqlConn = openMultigatewayConn(t, setup)
	ensureRolReplication(t, sqlConn)

	ctx := utils.WithTimeout(t, 15*time.Second)
	_, err := sqlConn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id int primary key, v text)", tableName))
	require.NoError(t, err, "create fixture table")
	_, err = sqlConn.ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, tableName))
	require.NoError(t, err, "create fixture publication")

	t.Cleanup(func() {
		// t.Context() is canceled by cleanup time; use a fresh bounded context.
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = sqlConn.ExecContext(cctx, "DROP PUBLICATION IF EXISTS "+pubName)
		_, _ = sqlConn.ExecContext(cctx, "DROP TABLE IF EXISTS "+tableName)
	})

	return sqlConn, tableName, pubName
}

// dialGatewayReplicationConn opens a Postgres connection in logical-replication
// mode against the multigateway PG port. This is the path under test: the
// gateway hijacks the socket and tunnels it to the PRIMARY pooler's
// replication=database backend.
func dialGatewayReplicationConn(t *testing.T, setup *ShardSetup) *client.Conn {
	t.Helper()
	ctx := utils.WithTimeout(t, 15*time.Second)
	cfg := client.Config{
		Host:        "localhost",
		Port:        setup.MultigatewayPgPort,
		User:        DefaultTestUser,
		Password:    TestPostgresPassword,
		Database:    "postgres",
		DialTimeout: 10 * time.Second,
		Parameters:  map[string]string{"replication": "database"},
	}
	conn, err := client.Connect(ctx, ctx, &cfg)
	require.NoError(t, err, "open replication-mode connection through gateway")
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// insertGatewayRow writes one row over the supplied normal SQL connection,
// generating WAL the publication will stream.
func insertGatewayRow(t *testing.T, conn *sql.Conn, table string, id int, v string) {
	t.Helper()
	ctx := utils.WithTimeout(t, 15*time.Second)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, v) VALUES ($1, $2)", table), id, v)
	require.NoError(t, err, "insert fixture row")
}

// parseXLogData extracts walStart, walEnd from a 'w' payload
// ('w' + walStart(8) + walEnd(8) + sendTime(8) + WAL bytes).
func parseXLogData(t *testing.T, p []byte) (walStart, walEnd uint64) {
	t.Helper()
	require.GreaterOrEqual(t, len(p), xlogDataHeaderLen)
	require.Equal(t, byte('w'), p[0])
	return binary.BigEndian.Uint64(p[1:9]), binary.BigEndian.Uint64(p[9:17])
}

// parseKeepalive extracts walEnd from a 'k' payload
// ('k' + walEnd(8) + sendTime(8) + replyRequested(1)).
func parseKeepalive(t *testing.T, p []byte) (walEnd uint64, replyRequested bool) {
	t.Helper()
	require.Len(t, p, 18)
	require.Equal(t, byte('k'), p[0])
	return binary.BigEndian.Uint64(p[1:9]), p[17] != 0
}

// buildStandbyStatus builds an 'r' payload acking the given LSN as
// received/flushed/applied (the value PG compares against confirmed_flush_lsn).
func buildStandbyStatus(lsn uint64) []byte {
	b := make([]byte, 34)
	b[0] = 'r'
	binary.BigEndian.PutUint64(b[1:9], lsn)   // received
	binary.BigEndian.PutUint64(b[9:17], lsn)  // flushed
	binary.BigEndian.PutUint64(b[17:25], lsn) // applied
	// clientTime microseconds since 2000-01-01; 0 is acceptable to the server.
	b[33] = 0 // replyRequested
	return b
}

// TestGatewayReplicationStream_HappyPath is the oracle: it replays Realtime's
// Broadcast-DB sequence end-to-end through the gateway — IDENTIFY_SYSTEM,
// CREATE_REPLICATION_SLOT (temporary, logical, pgoutput), START_REPLICATION,
// then receives an inserted row as a 'w' XLogData frame and acks it with an 'r'
// Standby Status Update.
func TestGatewayReplicationStream_HappyPath(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	sqlConn, table, pub := setupGatewayReplicationFixture(t, setup)
	slot := fmt.Sprintf("slot_gw_happy_%d", replFixtureCounter.Add(1))

	conn := dialGatewayReplicationConn(t, setup)
	ctx := utils.WithTimeout(t, 60*time.Second)

	// IDENTIFY_SYSTEM — simplest replication command; proves the tunnel is live.
	rows, err := conn.Query(ctx, "IDENTIFY_SYSTEM")
	require.NoError(t, err, "IDENTIFY_SYSTEM through gateway")
	require.Len(t, rows, 1, "IDENTIFY_SYSTEM returns one result set")
	require.Len(t, rows[0].Rows, 1, "IDENTIFY_SYSTEM returns one row")
	require.GreaterOrEqual(t, len(rows[0].Rows[0].Values), 4, "IDENTIFY_SYSTEM row has >=4 columns")

	// CREATE_REPLICATION_SLOT — temporary so it auto-drops on disconnect.
	_, err = conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err, "CREATE_REPLICATION_SLOT through gateway")

	// START_REPLICATION — Realtime's exact command, including binary 'true'
	// (opaque to multigres; must flow through verbatim).
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	require.NoError(t, err, "START_REPLICATION must enter copy-both")

	// Generate WAL on the published table over the normal SQL connection.
	insertGatewayRow(t, sqlConn, table, 1, "hello")

	// Read frames until a 'w' XLogData arrives; assert a non-zero walEnd.
	var walEnd uint64
	deadline := time.Now().Add(30 * time.Second)
	var sawW bool
	for time.Now().Before(deadline) {
		rctx, cancel := context.WithDeadline(t.Context(), deadline)
		msg, err := conn.ReadReplicationMessage(rctx)
		cancel()
		require.NoError(t, err)
		if msg.Notice != nil {
			t.Logf("notice during stream: %s", msg.Notice.Message)
			continue
		}
		require.NotEmpty(t, msg.Data)
		switch msg.Data[0] {
		case 'w':
			_, walEnd = parseXLogData(t, msg.Data)
			sawW = true
		case 'k':
			ka, _ := parseKeepalive(t, msg.Data)
			if ka > walEnd {
				walEnd = ka
			}
		}
		if sawW {
			break
		}
	}
	require.True(t, sawW, "must receive at least one XLogData ('w') frame through the gateway")
	require.NotZero(t, walEnd, "XLogData walEnd must be non-zero")

	// Ack the LSN with a Standby Status Update ('r') — the write half of the
	// tunnel back through gateway → pooler → postgres.
	require.NoError(t, conn.WriteReplicationData(buildStandbyStatus(walEnd)),
		"Standby Status Update must flow back through the gateway")
}

// TestGatewayReplicationStream_LargePayload streams a row whose text value
// exceeds 1 MiB and asserts the reassembled WAL bytes round-trip across 'w'
// frames without frame-boundary corruption through the gateway + pooler
// tunnels. The walsender splits a large message across multiple CopyData
// frames; the gateway/pooler must forward them byte-for-byte.
func TestGatewayReplicationStream_LargePayload(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	sqlConn, table, pub := setupGatewayReplicationFixture(t, setup)
	slot := fmt.Sprintf("slot_gw_large_%d", replFixtureCounter.Add(1))

	conn := dialGatewayReplicationConn(t, setup)
	ctx := utils.WithTimeout(t, 90*time.Second)
	_, err := conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s')",
		slot, pub))
	require.NoError(t, err)

	const payloadSize = 2 * 1024 * 1024 // 2 MiB, comfortably over 1 MiB
	insertGatewayRow(t, sqlConn, table, 1, strings.Repeat("x", payloadSize))

	// Accumulate WAL bytes from 'w' frames until we've covered the payload.
	var walBytes int
	deadline := time.Now().Add(60 * time.Second)
	for walBytes < payloadSize && time.Now().Before(deadline) {
		rctx, cancel := context.WithDeadline(t.Context(), deadline)
		msg, err := conn.ReadReplicationMessage(rctx)
		cancel()
		require.NoError(t, err)
		if msg.Notice != nil {
			continue
		}
		require.NotEmpty(t, msg.Data)
		if msg.Data[0] == 'w' {
			require.GreaterOrEqual(t, len(msg.Data), xlogDataHeaderLen, "truncated XLogData frame")
			walBytes += len(msg.Data) - xlogDataHeaderLen
		}
	}
	require.GreaterOrEqual(t, walBytes, payloadSize,
		"must receive at least the inserted payload size across 'w' frames without corruption")
}

// TestGatewayReplicationStream_TempSlotDropped verifies that after the
// replication connection closes, the temporary slot it created is gone — proved
// by querying pg_replication_slots over a normal multigateway connection.
func TestGatewayReplicationStream_TempSlotDropped(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	sqlConn, table, pub := setupGatewayReplicationFixture(t, setup)
	slot := fmt.Sprintf("slot_gw_temp_%d", replFixtureCounter.Add(1))

	conn := dialGatewayReplicationConn(t, setup)
	ctx := utils.WithTimeout(t, 60*time.Second)
	_, err := conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s')",
		slot, pub))
	require.NoError(t, err)

	// Stream at least one frame so the slot is fully established server-side.
	insertGatewayRow(t, sqlConn, table, 1, "temp")
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		rctx, cancel := context.WithDeadline(t.Context(), deadline)
		msg, err := conn.ReadReplicationMessage(rctx)
		cancel()
		require.NoError(t, err)
		if msg.Notice != nil {
			continue
		}
		require.NotEmpty(t, msg.Data)
		if msg.Data[0] == 'w' || msg.Data[0] == 'k' {
			break
		}
	}

	// Slot exists while the connection is live.
	requireGatewaySlotPresent(t, sqlConn, slot)

	// Close the replication connection: the temporary slot must disappear.
	require.NoError(t, conn.Close())
	requireGatewaySlotGone(t, sqlConn, slot)
}

// requireGatewaySlotPresent asserts the slot currently exists, via a normal
// multigateway SQL connection.
func requireGatewaySlotPresent(t *testing.T, conn *sql.Conn, slot string) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx := utils.WithTimeout(t, 5*time.Second)
		var present bool
		err := conn.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&present)
		return err == nil && present
	}, 15*time.Second, 250*time.Millisecond, "slot %s must exist while connection is live", slot)
}

// requireGatewaySlotGone asserts the slot is (eventually) absent, via a normal
// multigateway SQL connection.
func requireGatewaySlotGone(t *testing.T, conn *sql.Conn, slot string) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx := utils.WithTimeout(t, 5*time.Second)
		var present bool
		err := conn.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&present)
		return err == nil && !present
	}, 15*time.Second, 250*time.Millisecond, "temporary slot %s must disappear after disconnect", slot)
}
