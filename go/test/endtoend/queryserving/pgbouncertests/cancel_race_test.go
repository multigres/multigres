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
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// Group 5 of the PgBouncer port — cancel-race under pooling, ported from
// test_cancel.py::test_cancel_race.
//
// PgBouncer's worry: a CancelRequest for client A's query must not cancel a
// different query that client B is now running on a backend A just released into
// the pool. In a raw-postgres / PgBouncer world the cancel key maps to a
// *backend*, so reuse is where the race lives.
//
// Multigres routes cancels by the *client↔gateway connection* instead: the
// gateway hands each client a synthetic BackendKeyData (its own encoded PID + a
// per-connection random secret) and, on a CancelRequest, looks up that client
// connection by PID, checks the secret, and cancels only that connection's
// in-flight query (go/services/multigateway/{cancel.go,listener.go}). The
// backend a query happens to be borrowing is never the cancel's addressing unit,
// so the backend-reuse race is structurally absent. This test guards that it
// stays absent.
//
// Black-box throughout: clients read pg_backend_pid() from their own rows and
// send a standalone CancelRequest over a raw socket (the same machinery as
// query_cancel_test.go). A capacity-1 pool makes backend reuse deterministic.
//
// Group 3's TestPoolExhaustionWaiterCancelledCleanly already covers cancelling a
// pool-blocked waiter (via context cancellation), so it is not repeated here.

// cancelPoolCapacity sizes the pool so the lone test user's regular sub-pool
// settles to exactly one backend (global 2 × (1 − 0.5)), making backend reuse
// deterministic. Pairs with connpoolReservedRatio / connpoolRebalanceFast from
// pool_exhaustion_test.go.
const cancelPoolCapacity = "--connpool-global-capacity=2"

// TestCancelForReleasedBackendDoesNotHitReuser is the headline race test. With a
// 1-backend pool: client A runs a statement (then goes idle, releasing the
// backend), client B starts a long statement that reuses *that same backend*,
// and a stale CancelRequest for A is delivered while B runs. B must finish
// uncancelled — proving A's cancel is scoped to A's (idle) connection, never to
// the shared backend B inherited.
func TestCancelForReleasedBackendDoesNotHitReuser(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + standby (bootstrap needs 2)
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs(cancelPoolCapacity, connpoolReservedRatio, connpoolRebalanceFast),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	ctx := utils.WithTimeout(t, 120*time.Second)
	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// Settle the user's regular pool down to a single backend so reuse is forced.
	settleDB := openGatewayDB(t, setup)
	settleConnpool(t, ctx, settleDB)
	settleDB.Close()

	// Client A: learn its backend and capture its cancel key, then stay idle so
	// the backend returns to the pool.
	connA := connectGateway(t, ctx, gatewayDSN)
	defer connA.Close(ctx)
	pidA := queryBackendPid(t, ctx, connA)
	procA, secretA := backendCancelKey(t, connA)
	t.Logf("client A used backend %d; captured gateway cancel key pid=%d", pidA, procA)

	// Client B: a long statement that, with capacity 1, must reuse A's backend.
	connB := connectGateway(t, ctx, gatewayDSN)
	defer connB.Close(ctx)
	type result struct {
		pid int
		err error
	}
	bDone := make(chan result, 1)
	go func() {
		var pid int
		var dummy string
		err := connB.QueryRow(ctx, "SELECT pg_backend_pid(), pg_sleep(3)::text").Scan(&pid, &dummy)
		bDone <- result{pid: pid, err: err}
	}()

	// Let B's statement reach the backend, then deliver A's stale cancel.
	time.Sleep(750 * time.Millisecond)
	require.NoError(t, sendCancelRequest("127.0.0.1", setup.MultigatewayPgPort, procA, secretA),
		"sending client A's CancelRequest")

	select {
	case r := <-bDone:
		require.NoErrorf(t, r.err,
			"client B's query must not be cancelled by a CancelRequest meant for client A (got %v)", r.err)
		require.Equalf(t, pidA, r.pid,
			"with a capacity-1 pool B must reuse A's exact backend (A=%d, B=%d) — otherwise the reuse race isn't exercised", pidA, r.pid)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for client B's query")
	}
	t.Logf("client B completed uncancelled on the reused backend %d", pidA)

	// A's idle-connection cancel was a harmless no-op; A is still usable.
	var n int
	require.NoError(t, connA.QueryRow(ctx, "SELECT 1").Scan(&n))
	require.Equal(t, 1, n)
}

// backendCancelKey returns the gateway-issued cancel key (synthetic PID and
// secret) for a pgx connection, decoding the 4-byte secret the gateway sends in
// BackendKeyData.
func backendCancelKey(t *testing.T, conn *pgx.Conn) (procID, secret uint32) {
	t.Helper()
	sk := conn.PgConn().SecretKey()
	require.Lenf(t, sk, 4, "gateway should issue a 4-byte cancel secret, got %d bytes", len(sk))
	return conn.PgConn().PID(), binary.BigEndian.Uint32(sk)
}

// sendCancelRequest dials the gateway's PostgreSQL port and sends a standalone
// CancelRequest for (processID, secretKey) — the out-of-band cancel a real
// client uses. Mirrors the helper in query_cancel_test.go.
func sendCancelRequest(host string, port int, processID, secretKey uint32) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial cancel endpoint %s: %w", addr, err)
	}
	defer conn.Close()

	packet := make([]byte, 16)
	binary.BigEndian.PutUint32(packet[0:4], 16)
	binary.BigEndian.PutUint32(packet[4:8], protocol.CancelRequestCode)
	binary.BigEndian.PutUint32(packet[8:12], processID)
	binary.BigEndian.PutUint32(packet[12:16], secretKey)

	if _, err := conn.Write(packet); err != nil {
		return fmt.Errorf("write cancel packet: %w", err)
	}
	return nil
}
