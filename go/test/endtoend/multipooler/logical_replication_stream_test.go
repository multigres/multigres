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

package multipooler

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/test/utils"
)

// xlogDataHeaderLen is the fixed header before the WAL bytes in a 'w' frame:
// 'w' + walStart(8) + walEnd(8) + sendTime(8).
const xlogDataHeaderLen = 25

// streamUntilFrame reads from a copy-both stream until it sees one XLogData
// ('w') or keepalive ('k') frame, returning the highest walEnd LSN observed.
// Notices are logged and skipped. Fatal if nothing arrives before timeout.
func streamUntilFrame(t *testing.T, conn *client.Conn, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// Per-read context bound to the outer deadline (not a fixed sub-timeout)
		// so a single read can use the full remaining budget, and cancelled
		// promptly each iteration rather than accumulating on the cleanup stack.
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
			_, walEnd := parseXLogData(t, msg.Data)
			return walEnd
		case 'k':
			walEnd, _ := parseKeepalive(t, msg.Data)
			return walEnd
		}
	}
	t.Fatal("no XLogData/keepalive frame received before timeout")
	return 0
}

// requireSlotInactive asserts the slot exists and (eventually) has active=false.
func requireSlotInactive(t *testing.T, setup *MultipoolerTestSetup, slot string) {
	t.Helper()
	conn := dialNormalConn(t, setup)
	require.Eventually(t, func() bool {
		ctx := utils.WithTimeout(t, 5*time.Second)
		rows, err := conn.Query(ctx, fmt.Sprintf(
			"SELECT active FROM pg_replication_slots WHERE slot_name = '%s'", slot))
		if err != nil || len(rows) == 0 || len(rows[0].Rows) == 0 {
			return false
		}
		return string(rows[0].Rows[0].Values[0]) == "f"
	}, 15*time.Second, 250*time.Millisecond, "slot %s must persist and become inactive", slot)
}

// requireSlotPresent asserts the slot currently exists.
func requireSlotPresent(t *testing.T, setup *MultipoolerTestSetup, slot string) {
	t.Helper()
	conn := dialNormalConn(t, setup)
	ctx := utils.WithTimeout(t, 5*time.Second)
	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'", slot))
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	require.NotEmpty(t, rows[0].Rows, "slot %s must exist", slot)
}

// requireSlotGone asserts the slot is (eventually) absent from the catalog.
func requireSlotGone(t *testing.T, setup *MultipoolerTestSetup, slot string) {
	t.Helper()
	conn := dialNormalConn(t, setup)
	require.Eventually(t, func() bool {
		ctx := utils.WithTimeout(t, 5*time.Second)
		rows, err := conn.Query(ctx, fmt.Sprintf(
			"SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'", slot))
		return err == nil && (len(rows) == 0 || len(rows[0].Rows) == 0)
	}, 15*time.Second, 250*time.Millisecond, "slot %s must disappear", slot)
}

// fixtureCounter makes table/publication/slot names unique within a run.
var fixtureCounter atomic.Int64

// normalConnConfig is the client.Config for a plain (non-replication) SQL
// connection to the shared fixture's primary.
func normalConnConfig(setup *MultipoolerTestSetup) *client.Config {
	return &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        constants.DefaultPostgresUser,
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	}
}

// dialNormalConn opens a plain SQL connection for use in a test body, used for
// catalog queries, DDL, and INSERTs that generate WAL. Replication-mode
// connections (dialReplicationConn) can't run ordinary SQL, so the stream
// tests pair the two. Bound to t.Context(); do NOT call from a t.Cleanup
// handler (t.Context() is already canceled there) — use cleanupConn instead.
func dialNormalConn(t *testing.T, setup *MultipoolerTestSetup) *client.Conn {
	t.Helper()
	ctx := utils.WithTimeout(t, 10*time.Second)
	conn, err := client.Connect(ctx, ctx, normalConnConfig(setup))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// cleanupConn dials a normal SQL connection from a t.Cleanup handler. It uses a
// background-derived context with its own timeout because t.Context() is
// canceled before cleanups run. Returns the conn plus a done func; nil conn if
// the dial failed (logged, non-fatal — cleanup is best-effort).
func cleanupConn(t *testing.T, setup *MultipoolerTestSetup, timeout time.Duration) (*client.Conn, context.Context, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	conn, err := client.Connect(ctx, ctx, normalConnConfig(setup))
	if err != nil {
		t.Logf("cleanup dial failed: %v", err)
		cancel()
		return nil, nil, func() {}
	}
	return conn, ctx, func() { _ = conn.Close(); cancel() }
}

// setupReplicationFixture creates a fresh table + publication on the primary
// over a normal connection and returns their names. Each call uses a unique
// suffix so sub-tests don't collide.
func setupReplicationFixture(t *testing.T, setup *MultipoolerTestSetup) (tableName, pubName string) {
	t.Helper()
	n := fixtureCounter.Add(1)
	tableName = fmt.Sprintf("repl_e2e_%d", n)
	pubName = fmt.Sprintf("pub_e2e_%d", n)

	conn := dialNormalConn(t, setup)
	ctx := utils.WithTimeout(t, 15*time.Second)

	_, err := conn.Query(ctx, fmt.Sprintf("CREATE TABLE %s (id int primary key, v text)", tableName))
	require.NoError(t, err, "create fixture table")
	_, err = conn.Query(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, tableName))
	require.NoError(t, err, "create fixture publication")

	t.Cleanup(func() {
		conn, ctx, done := cleanupConn(t, setup, 10*time.Second)
		if conn == nil {
			return
		}
		defer done()
		_, _ = conn.Query(ctx, "DROP PUBLICATION IF EXISTS "+pubName)
		_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS "+tableName)
	})

	return tableName, pubName
}

// parseXLogData extracts walStart, walEnd from a 'w' payload (header is
// 'w' + walStart(8) + walEnd(8) + sendTime(8) + WAL bytes).
func parseXLogData(t *testing.T, p []byte) (walStart, walEnd uint64) {
	t.Helper()
	require.GreaterOrEqual(t, len(p), 25)
	require.Equal(t, byte('w'), p[0])
	return binary.BigEndian.Uint64(p[1:9]), binary.BigEndian.Uint64(p[9:17])
}

// parseKeepalive extracts walEnd + replyRequested from a 'k' payload
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

// insertRow writes one row into the fixture table over a normal connection,
// generating WAL that the publication will stream.
func insertRow(t *testing.T, setup *MultipoolerTestSetup, table string, id int, v string) {
	t.Helper()
	conn := dialNormalConn(t, setup)
	ctx := utils.WithTimeout(t, 10*time.Second)
	_, err := conn.Query(ctx, fmt.Sprintf("INSERT INTO %s (id, v) VALUES (%d, '%s')", table, id, v))
	require.NoError(t, err, "insert fixture row")
}

// lsnToText formats a uint64 LSN as PG's canonical X/Y hex form.
func lsnToText(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

// requireConfirmedFlushAdvances polls pg_replication_slots until the slot's
// confirmed_flush_lsn has advanced to at least ackLSN (the value we acked via
// a Standby Status Update). Comparison is done server-side via pg_lsn ordering.
func requireConfirmedFlushAdvances(t *testing.T, setup *MultipoolerTestSetup, slot string, ackLSN uint64) {
	t.Helper()
	conn := dialNormalConn(t, setup)
	q := fmt.Sprintf(
		"SELECT confirmed_flush_lsn >= '%s'::pg_lsn FROM pg_replication_slots WHERE slot_name = '%s'",
		lsnToText(ackLSN), slot)
	require.Eventually(t, func() bool {
		ctx := utils.WithTimeout(t, 5*time.Second)
		rows, err := conn.Query(ctx, q)
		if err != nil || len(rows) == 0 || len(rows[0].Rows) == 0 {
			return false
		}
		return string(rows[0].Rows[0].Values[0]) == "t"
	}, 15*time.Second, 250*time.Millisecond, "confirmed_flush_lsn must advance to >= acked LSN")
}

// dropReplicationSlot best-effort drops a slot during cleanup. Register it
// BEFORE dialing the replication conn so it runs (LIFO) after that conn closes
// and the slot becomes inactive. A temporary slot is already gone by then, so
// this becomes a no-op.
func dropReplicationSlot(t *testing.T, setup *MultipoolerTestSetup, slot string) {
	conn, _, done := cleanupConn(t, setup, 30*time.Second)
	if conn == nil {
		return
	}
	defer done()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, err := conn.Query(ctx, fmt.Sprintf(
			"SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'", slot))
		if err == nil && (len(rows) == 0 || len(rows[0].Rows) == 0) {
			return true // already gone
		}
		_, derr := conn.Query(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slot))
		return derr == nil
	}, 15*time.Second, 250*time.Millisecond, "slot %s must be droppable (no leak)", slot)
}

func TestLogicalReplicationStreamHappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)

	for _, temporary := range []bool{true, false} {
		name := "permanent"
		if temporary {
			name = "temporary"
		}
		t.Run(name, func(t *testing.T) {
			table, pub := setupReplicationFixture(t, setup)
			slot := fmt.Sprintf("slot_%s_%d", name, fixtureCounter.Add(1))
			ctx := utils.WithTimeout(t, 30*time.Second)

			// Register slot cleanup before dialing so it runs after the
			// replication conn closes (temporary slots auto-drop; this is a
			// no-op for them).
			t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

			conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)

			// CREATE_REPLICATION_SLOT via the normal query path (reuses Query()).
			tempKw := ""
			if temporary {
				tempKw = "TEMPORARY "
			}
			_, err := conn.Query(ctx, fmt.Sprintf(
				"CREATE_REPLICATION_SLOT %s %sLOGICAL pgoutput NOEXPORT_SNAPSHOT", slot, tempKw))
			require.NoError(t, err)

			// START_REPLICATION — matches Realtime's exact command, including
			// binary 'true' (opaque to multigres; must flow through verbatim).
			_, err = conn.StartReplication(ctx, fmt.Sprintf(
				"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
				slot, pub))
			require.NoError(t, err)

			// Generate WAL on the published table via a separate normal conn.
			insertRow(t, setup, table, 1, "hello")

			// Read frames until we see >=1 'w' and >=1 'k'; ack the highest LSN.
			var sawW, sawK bool
			var ackLSN uint64
			deadline := time.Now().Add(20 * time.Second)
			for (!sawW || !sawK) && time.Now().Before(deadline) {
				rctx, cancel := context.WithDeadline(t.Context(), deadline)
				msg, err := conn.ReadReplicationMessage(rctx)
				cancel()
				require.NoError(t, err)
				if msg.Notice != nil {
					t.Logf("notice during stream: %s", msg.Notice.Message)
					continue
				}
				payload := msg.Data
				require.NotEmpty(t, payload)
				switch payload[0] {
				case 'w':
					_, walEnd := parseXLogData(t, payload)
					sawW = true
					if walEnd > ackLSN {
						ackLSN = walEnd
					}
				case 'k':
					walEnd, _ := parseKeepalive(t, payload)
					sawK = true
					if walEnd > ackLSN {
						ackLSN = walEnd
					}
				}
			}
			require.True(t, sawW, "must receive at least one XLogData frame")
			require.True(t, sawK, "must receive at least one keepalive frame")

			require.NoError(t, conn.WriteReplicationData(buildStandbyStatus(ackLSN)))

			// confirmed_flush_lsn should advance. Poll via a normal conn.
			requireConfirmedFlushAdvances(t, setup, slot, ackLSN)
		})
	}
}

// TestLogicalReplicationStreamSlotPersistence verifies that a permanent slot
// survives a client disconnect (and can be resumed by a fresh connection),
// while a temporary slot is dropped when its owning connection closes.
func TestLogicalReplicationStreamSlotPersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)

	startCmd := func(slot string) string {
		return fmt.Sprintf(
			"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s')",
			slot, pub)
	}

	// --- permanent slot survives disconnect and resumes ---
	permSlot := fmt.Sprintf("slot_persist_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, permSlot) })

	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx := utils.WithTimeout(t, 30*time.Second)
	_, err := conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", permSlot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, startCmd(permSlot))
	require.NoError(t, err)
	insertRow(t, setup, table, 1, "persist")
	streamUntilFrame(t, conn, 20*time.Second)
	require.NoError(t, conn.Close())

	// Slot still present, now inactive.
	requireSlotInactive(t, setup, permSlot)

	// A fresh connection resumes the same slot.
	conn2 := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx2 := utils.WithTimeout(t, 30*time.Second)
	_, err = conn2.StartReplication(ctx2, startCmd(permSlot))
	require.NoError(t, err, "permanent slot must resume after reconnect")
	insertRow(t, setup, table, 2, "resume")
	streamUntilFrame(t, conn2, 20*time.Second)
	require.NoError(t, conn2.Close())

	// --- temporary slot disappears on disconnect ---
	tmpSlot := fmt.Sprintf("slot_tmp_%d", fixtureCounter.Add(1))
	conn3 := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx3 := utils.WithTimeout(t, 30*time.Second)
	_, err = conn3.Query(ctx3, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", tmpSlot))
	require.NoError(t, err)
	requireSlotPresent(t, setup, tmpSlot)
	require.NoError(t, conn3.Close())
	requireSlotGone(t, setup, tmpSlot)
}

// TestLogicalReplicationStreamLargePayload streams a row whose text value
// exceeds 1 MiB and asserts the WAL bytes round-trip across 'w' frames without
// frame-boundary corruption (the reader must reassemble a multi-MiB message).
func TestLogicalReplicationStreamLargePayload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("slot_large_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx := utils.WithTimeout(t, 60*time.Second)
	_, err := conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s')",
		slot, pub))
	require.NoError(t, err)

	const payloadSize = 2 * 1024 * 1024 // 2 MiB, comfortably over 1 MiB
	insertRow(t, setup, table, 1, strings.Repeat("x", payloadSize))

	// Accumulate WAL bytes from 'w' frames until we've covered the payload.
	var walBytes int
	deadline := time.Now().Add(40 * time.Second)
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
	require.NoError(t, conn.Close())
}

// slotActivePID polls until the slot has a non-null active_pid (the walsender
// backend serving it) and returns it.
func slotActivePID(t *testing.T, setup *MultipoolerTestSetup, slot string) int64 {
	t.Helper()
	conn := dialNormalConn(t, setup)
	var pid int64
	require.Eventually(t, func() bool {
		ctx := utils.WithTimeout(t, 5*time.Second)
		rows, err := conn.Query(ctx, fmt.Sprintf(
			"SELECT active_pid FROM pg_replication_slots WHERE slot_name = '%s'", slot))
		if err != nil || len(rows) == 0 || len(rows[0].Rows) == 0 {
			return false
		}
		v := string(rows[0].Rows[0].Values[0])
		if v == "" {
			return false // active_pid is NULL — walsender not attached yet
		}
		p, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil {
			return false
		}
		pid = p
		return true
	}, 15*time.Second, 250*time.Millisecond, "a walsender must attach to slot %s", slot)
	return pid
}

// requireBackendGone polls pg_stat_activity until the given backend pid is gone.
func requireBackendGone(t *testing.T, setup *MultipoolerTestSetup, pid int64) {
	t.Helper()
	conn := dialNormalConn(t, setup)
	require.Eventually(t, func() bool {
		ctx := utils.WithTimeout(t, 5*time.Second)
		rows, err := conn.Query(ctx, fmt.Sprintf(
			"SELECT 1 FROM pg_stat_activity WHERE pid = %d", pid))
		return err == nil && (len(rows) == 0 || len(rows[0].Rows) == 0)
	}, 15*time.Second, 250*time.Millisecond, "walsender backend pid %d must terminate", pid)
}

// TestLogicalReplicationStreamNonexistentSlot verifies a START_REPLICATION
// against a missing slot returns an error and leaves the connection usable.
func TestLogicalReplicationStreamNonexistentSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx := utils.WithTimeout(t, 30*time.Second)

	slot := fmt.Sprintf("does_not_exist_%d", fixtureCounter.Add(1))
	_, err := conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names 'nopub')", slot))
	require.Error(t, err, "START_REPLICATION on a missing slot must fail")

	// The trailing ReadyForQuery was drained, so the conn is still usable.
	rows, err := conn.Query(ctx, "IDENTIFY_SYSTEM")
	require.NoError(t, err, "connection must remain usable after a failed START_REPLICATION")
	require.Len(t, rows, 1)
}

// TestLogicalReplicationStreamDuplicateSlot verifies creating a slot twice
// surfaces SQLSTATE 42710 (duplicate_object) from PostgreSQL.
func TestLogicalReplicationStreamDuplicateSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	slot := fmt.Sprintf("slot_dup_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx := utils.WithTimeout(t, 30*time.Second)

	create := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot)
	_, err := conn.Query(ctx, create)
	require.NoError(t, err)
	_, err = conn.Query(ctx, create)
	require.Error(t, err, "creating a duplicate slot must fail")
	require.Equal(t, "42710", mterrors.ExtractSQLSTATE(err), "duplicate slot must be duplicate_object (42710)")
}

// TestLogicalReplicationStreamCleanTermination verifies the graceful two-phase
// copy-both shutdown (CopyDone + FinishReplication): the slot is released and,
// once the connection closes, the walsender backend terminates.
func TestLogicalReplicationStreamCleanTermination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("slot_clean_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx := utils.WithTimeout(t, 30*time.Second)
	_, err := conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s')", slot, pub))
	require.NoError(t, err)
	insertRow(t, setup, table, 1, "clean")
	streamUntilFrame(t, conn, 20*time.Second)

	pid := slotActivePID(t, setup, slot)

	// Graceful copy-both shutdown.
	require.NoError(t, conn.WriteCopyDone())
	_, _, err = conn.FinishReplication(ctx)
	require.NoError(t, err, "FinishReplication must drain to ReadyForQuery cleanly")

	// The slot is released (still present — it's non-temporary).
	requireSlotInactive(t, setup, slot)

	// Closing the connection terminates the walsender backend.
	require.NoError(t, conn.Close())
	requireBackendGone(t, setup, pid)
}

// TestLogicalReplicationStreamAbruptTermination verifies that force-closing the
// socket mid-stream terminates the walsender backend and drops a temporary slot.
func TestLogicalReplicationStreamAbruptTermination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("slot_abrupt_%d", fixtureCounter.Add(1))

	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx := utils.WithTimeout(t, 30*time.Second)
	_, err := conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s')", slot, pub))
	require.NoError(t, err)
	insertRow(t, setup, table, 1, "abrupt")
	streamUntilFrame(t, conn, 20*time.Second)

	pid := slotActivePID(t, setup, slot)

	// Abrupt teardown: no copy-both shutdown, just kill the socket.
	require.NoError(t, conn.ForceClose())

	requireBackendGone(t, setup, pid)
	requireSlotGone(t, setup, slot) // temporary slot vanishes with its backend
}

// TestLogicalReplicationStreamServerTerminate verifies that when the walsender
// backend is terminated server-side mid-stream, a blocked read surfaces a clear
// error promptly rather than hanging. (Design Tier-2 case.)
func TestLogicalReplicationStreamServerTerminate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("slot_srvterm_%d", fixtureCounter.Add(1))

	conn := dialReplicationConn(t, setup.PrimaryPgctld.PgPort)
	ctx := utils.WithTimeout(t, 30*time.Second)
	// Temporary slot: it vanishes with the terminated backend, no cleanup needed.
	_, err := conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s')", slot, pub))
	require.NoError(t, err)
	insertRow(t, setup, table, 1, "srvterm")
	streamUntilFrame(t, conn, 20*time.Second)

	pid := slotActivePID(t, setup, slot)

	// Terminate the walsender backend from a normal connection.
	killer := dialNormalConn(t, setup)
	kctx := utils.WithTimeout(t, 10*time.Second)
	_, err = killer.Query(kctx, fmt.Sprintf("SELECT pg_terminate_backend(%d)", pid))
	require.NoError(t, err)

	// A subsequent read must surface an error promptly. Skip any in-flight
	// frames that were buffered before the termination propagated; the per-read
	// context is bound to the outer deadline so a regression that ignored the
	// backend death fails loudly instead of hanging.
	deadline := time.Now().Add(20 * time.Second)
	var readErr error
	for time.Now().Before(deadline) {
		rctx, cancel := context.WithDeadline(t.Context(), deadline)
		_, e := conn.ReadReplicationMessage(rctx)
		cancel()
		if e != nil {
			readErr = e
			break
		}
	}
	require.Error(t, readErr, "server-side backend termination must surface as a read error, not a hang")
}

// TestLogicalReplicationStreamKeepaliveReplyAck mirrors how Realtime acks: when
// the server sends a keepalive with reply-requested set, the consumer responds
// with a Standby Status Update acking walEnd+1 (realtime adapters/postgres/
// protocol.ex). PG's walsender sends a reply-requested keepalive after
// wal_sender_timeout/2 of silence, so the connection lowers wal_sender_timeout
// to make one arrive quickly; the ack is then sent well within the timeout.
func TestLogicalReplicationStreamKeepaliveReplyAck(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("slot_kaack_%d", fixtureCounter.Add(1))

	// Replication connection with a low wal_sender_timeout (8s): the server
	// then sends a reply-requested keepalive ~every 4s, and terminates only
	// after 8s of no reply — ample margin to ack the one we receive.
	ctx := utils.WithTimeout(t, 30*time.Second)
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        constants.DefaultPostgresUser,
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
		Parameters: map[string]string{
			"replication": "database",
			"options":     "-c wal_sender_timeout=8000",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Query(ctx, fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	require.NoError(t, err)
	_, err = conn.StartReplication(ctx, fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	require.NoError(t, err)

	insertRow(t, setup, table, 1, "kaack")
	dataLSN := streamUntilFrame(t, conn, 20*time.Second)

	// Read until the server sends a keepalive that requests a reply, then ack
	// walEnd+1 — exactly what Realtime does in response.
	var ackedReplyRequested bool
	deadline := time.Now().Add(20 * time.Second)
	for !ackedReplyRequested && time.Now().Before(deadline) {
		rctx, cancel := context.WithDeadline(t.Context(), deadline)
		msg, err := conn.ReadReplicationMessage(rctx)
		cancel()
		require.NoError(t, err)
		if msg.Notice != nil {
			continue
		}
		require.NotEmpty(t, msg.Data)
		if msg.Data[0] == 'k' {
			walEnd, replyRequested := parseKeepalive(t, msg.Data)
			if replyRequested {
				require.NoError(t, conn.WriteReplicationData(buildStandbyStatus(walEnd+1)))
				ackedReplyRequested = true
			}
		}
	}
	require.True(t, ackedReplyRequested,
		"server must send a reply-requested keepalive on the wal_sender_timeout schedule")

	requireConfirmedFlushAdvances(t, setup, slot, dataLSN)
}
