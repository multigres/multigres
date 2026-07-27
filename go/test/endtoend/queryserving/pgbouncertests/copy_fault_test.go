// Copyright 2026 Supabase, Inc.
// Portions derived from PgBouncer (ISC License),
// Copyright (c) 2007-2009 Marko Kreen, Skype Technologies OÜ.
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

package pgbouncertests

import (
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// Group 6 of the PgBouncer port — COPY under fault, ported from test_copy.py.
//
// COPY FROM STDIN pins a reserved backend (ReasonCopy) for the duration of the
// stream. test_copy.py already covers the happy paths (formats, TO STDOUT,
// transactions, multi-statement, error recovery), which the existing
// copy_test.go mirrors — so the net-new delta here is the fault path: a backend
// hard-killed mid-stream must surface a clean error to the client (not a hang),
// the proxy must recover, and the interrupted COPY must commit nothing (COPY
// FROM is all-or-nothing).
//
// Black-box: the client streams real COPY data over the wire and observes only
// the COPY result and post-recovery queries. The fault is KillPostgres (SIGKILL),
// after which the multipooler's monitor auto-restarts the same backend. Runs in
// its own NewIsolated cluster so the crash cannot leak into shared-cluster tests.

// TestCopyInterruptedByBackendCrash streams a large COPY FROM STDIN, hard-kills
// postgres mid-stream, and asserts: the COPY fails cleanly (a returned error
// within a deadline, not a hang), the proxy recovers so a fresh connection
// serves queries again, and the killed COPY left zero rows (clean rollback — no
// partial/torn data).
func TestCopyInterruptedByBackendCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 120*time.Second)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn := connectGateway(t, ctx, gatewayDSN)
	defer conn.Close(ctx)

	_, err := conn.Exec(ctx, "CREATE TABLE copy_fault (id int)")
	require.NoError(t, err)

	// Stream a COPY that runs long enough to be killed mid-flight. The reader
	// generates rows continuously, so CopyData keeps flowing to the backend and
	// the dead socket is noticed promptly once postgres is killed.
	copyDone := make(chan error, 1)
	go func() {
		_, copyErr := conn.PgConn().CopyFrom(ctx, &copyRowReader{max: 500_000_000}, "COPY copy_fault FROM STDIN")
		copyDone <- copyErr
	}()

	// Let the COPY get going (rows streaming to the reserved backend), then crash
	// postgres underneath it.
	time.Sleep(time.Second)
	setup.KillPostgres(t, setup.PrimaryName)

	select {
	case copyErr := <-copyDone:
		require.Error(t, copyErr, "a COPY interrupted by a backend crash must fail cleanly")
		t.Logf("COPY failed cleanly on backend crash: %v", copyErr)
	case <-time.After(30 * time.Second):
		t.Fatal("COPY did not return after the backend was killed (possible hang or torn stream)")
	}

	waitForPostgresReady(t, setup, 60*time.Second)
	t.Log("postgres auto-restarted by the monitor")

	// A fresh connection must serve queries again after recovery.
	conn2 := connectGateway(t, ctx, gatewayDSN)
	defer conn2.Close(ctx)

	// Disable statement_timeout on the recovery connection. This test isn't
	// exercising timeout behavior — it just needs the post-restart queries to
	// complete. Under coverage-instrumented binaries (which run ~2-3x slower) a
	// query issued right after crash recovery can otherwise exceed the gateway's
	// default statement_timeout and fail with SQLSTATE 57014.
	_, err = conn2.Exec(ctx, "SET statement_timeout = 0")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		recoverCtx := utils.WithShortDeadline(t)
		var n int
		return conn2.QueryRow(recoverCtx, "SELECT 1").Scan(&n) == nil && n == 1
	}, 30*time.Second, 250*time.Millisecond, "a fresh connection must work after the COPY-fault recovery")

	// COPY FROM is all-or-nothing: the interrupted stream committed nothing, so
	// the (durable, pre-crash) table survived crash recovery with zero rows.
	var rows int
	require.NoError(t, conn2.QueryRow(ctx, "SELECT count(*) FROM copy_fault").Scan(&rows))
	require.Zerof(t, rows, "an interrupted COPY must commit nothing; found %d rows", rows)
}

// copyRowReader streams COPY FROM STDIN text data for a single int column —
// "0\n1\n2\n…" — generating rows on demand up to max. max is only a runaway
// guard: the test kills the backend long before it is reached, so CopyFrom
// errors out mid-stream and stops reading.
type copyRowReader struct {
	next int64
	max  int64
}

func (r *copyRowReader) Read(p []byte) (int, error) {
	if r.next >= r.max {
		return 0, io.EOF
	}
	n := 0
	// Leave headroom for the longest int64 line ("-9223372036854775808\n").
	for r.next < r.max && len(p)-n >= 24 {
		line := strconv.AppendInt(p[n:n], r.next, 10)
		n += len(line)
		p[n] = '\n'
		n++
		r.next++
	}
	return n, nil
}
