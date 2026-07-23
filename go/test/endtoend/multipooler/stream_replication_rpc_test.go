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

package multipooler

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/test/utils"
)

// primaryReplTarget builds the routing target for the primary pooler.
func primaryReplTarget() *querypb.Target {
	// WRITABLE = leader-bound (#1183); the primary pooler is the consensus leader,
	// so this admits. An empty ShardKey skips the tablegroup/shard checks in the
	// single-shard test cluster.
	return &querypb.Target{Mode: querypb.Mode_MODE_WRITABLE}
}

// dialPoolerClient opens a gRPC client to the primary multipooler.
func dialPoolerClient(t *testing.T, setup *MultipoolerTestSetup) multipoolerpb.MultipoolerServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return multipoolerpb.NewMultipoolerServiceClient(conn)
}

// replTunnel drives the replication wire protocol over a StreamReplication gRPC
// stream. The tunnel delivers opaque, arbitrarily-chunked bytes (it is not
// frame-aligned), so the reader reassembles pgwire messages across chunk
// boundaries. This deliberately re-implements just enough framing to exercise
// the tunnel end-to-end without a gateway; the inner CopyData payloads reuse
// the shared parseXLogData / parseKeepalive / buildStandbyStatus helpers.
type replTunnel struct {
	t      *testing.T
	stream multipoolerpb.MultipoolerService_StreamReplicationClient
	rbuf   []byte
}

// openReplTunnel opens a StreamReplication, sends init, and waits for ready.
func openReplTunnel(t *testing.T, cl multipoolerpb.MultipoolerServiceClient, ctx context.Context) *replTunnel {
	t.Helper()
	stream, err := cl.StreamReplication(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&multipoolerpb.StreamReplicationRequest{
		Msg: &multipoolerpb.StreamReplicationRequest_Init{
			Init: &multipoolerpb.StreamReplicationInit{
				Target: primaryReplTarget(),
				Mode:   multipoolerpb.ReplicationMode_REPLICATION_MODE_DATABASE,
				User:   constants.DefaultPostgresUser,
			},
		},
	}))
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.GetReady(), "first response must be ready, got %v", resp.GetMsg())
	return &replTunnel{t: t, stream: stream}
}

func (rt *replTunnel) sendData(b []byte) {
	rt.t.Helper()
	require.NoError(rt.t, rt.stream.Send(&multipoolerpb.StreamReplicationRequest{
		Msg: &multipoolerpb.StreamReplicationRequest_Data{Data: b},
	}))
}

// fill pulls one more server->client chunk into rbuf, failing on a structured
// infrastructure error.
func (rt *replTunnel) fill() error {
	resp, err := rt.stream.Recv()
	if err != nil {
		return err
	}
	if e := resp.GetError(); e != nil {
		return fmt.Errorf("tunnel infrastructure error: %v", e.GetDiagnostic())
	}
	rt.rbuf = append(rt.rbuf, resp.GetData()...)
	return nil
}

func (rt *replTunnel) readN(n int) ([]byte, error) {
	for len(rt.rbuf) < n {
		if err := rt.fill(); err != nil {
			return nil, err
		}
	}
	out := append([]byte(nil), rt.rbuf[:n]...) // copy; rbuf is reused
	rt.rbuf = rt.rbuf[n:]
	return out, nil
}

// readMessage reads one pgwire message: type byte + int32 length (incl self) + body.
func (rt *replTunnel) readMessage() (byte, []byte, error) {
	hdr, err := rt.readN(5)
	if err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(hdr[1:5])
	body, err := rt.readN(int(length) - 4)
	if err != nil {
		return 0, nil, err
	}
	return hdr[0], body, nil
}

// sendQuery sends a simple Query ('Q') message through the tunnel.
func (rt *replTunnel) sendQuery(sql string) {
	rt.t.Helper()
	buf := make([]byte, 5, len(sql)+6)
	buf[0] = protocol.MsgQuery
	buf = append(buf, []byte(sql)...)
	buf = append(buf, 0)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(buf)-1))
	rt.sendData(buf)
}

// sendCopyData wraps payload in a CopyData ('d') message and sends it.
func (rt *replTunnel) sendCopyData(payload []byte) {
	rt.t.Helper()
	buf := make([]byte, 5, len(payload)+5)
	buf[0] = protocol.MsgCopyData
	buf = append(buf, payload...)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(buf)-1))
	rt.sendData(buf)
}

// sendCopyDone sends a CopyDone ('c') message, ending the client's half of the
// copy-both stream.
func (rt *replTunnel) sendCopyDone() {
	rt.t.Helper()
	var buf [5]byte
	buf[0] = protocol.MsgCopyDone
	binary.BigEndian.PutUint32(buf[1:5], 4)
	rt.sendData(buf[:])
}

// streamOneFrame reads CopyData frames until it sees an XLogData or keepalive,
// confirming the stream is live. Returns the highest walEnd LSN seen.
func (rt *replTunnel) streamOneFrame(timeout time.Duration) uint64 {
	rt.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		typ, body, err := rt.readMessage()
		require.NoError(rt.t, err)
		if typ != protocol.MsgCopyData || len(body) == 0 {
			continue
		}
		switch body[0] {
		case 'w':
			_, walEnd := parseXLogData(rt.t, body)
			return walEnd
		case 'k':
			walEnd, _ := parseKeepalive(rt.t, body)
			return walEnd
		}
	}
	rt.t.Fatal("did not observe any stream frame before timeout")
	return 0
}

// drainUntilReadyForQuery reads (and discards) messages until ReadyForQuery,
// failing on an ErrorResponse.
func (rt *replTunnel) drainUntilReadyForQuery() {
	rt.t.Helper()
	for {
		typ, body, err := rt.readMessage()
		require.NoError(rt.t, err)
		switch typ {
		case protocol.MsgReadyForQuery:
			return
		case protocol.MsgErrorResponse:
			rt.t.Fatalf("unexpected ErrorResponse: %s", string(body))
		}
	}
}

// expectCopyBothResponse reads until CopyBothResponse ('W'), the signal that
// START_REPLICATION succeeded and the stream is live.
func (rt *replTunnel) expectCopyBothResponse() {
	rt.t.Helper()
	for {
		typ, body, err := rt.readMessage()
		require.NoError(rt.t, err)
		switch typ {
		case protocol.MsgCopyBothResponse:
			return
		case protocol.MsgErrorResponse:
			rt.t.Fatalf("START_REPLICATION failed: %s", string(body))
		case protocol.MsgNoticeResponse, protocol.MsgParameterStatus:
			// informational; keep reading
		default:
			rt.t.Fatalf("unexpected message before CopyBothResponse: %q", typ)
		}
	}
}

// TestStreamReplicationRPCHappyPath drives a full logical replication session
// through the multipooler StreamReplication RPC against real postgres: create
// slot -> START_REPLICATION -> INSERT -> receive XLogData + keepalive -> ack ->
// confirmed_flush_lsn advances.
func TestStreamReplicationRPCHappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("rpc_slot_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	ctx := utils.WithTimeout(t, 60*time.Second)
	rt := openReplTunnel(t, dialPoolerClient(t, setup), ctx)

	// Permanent slot, created through the tunnel.
	rt.sendQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	rt.drainUntilReadyForQuery()

	rt.sendQuery(fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	rt.expectCopyBothResponse()

	insertRow(t, setup, table, 1, "hello")

	var sawW, sawK bool
	var ackLSN uint64
	deadline := time.Now().Add(20 * time.Second)
	for (!sawW || !sawK) && time.Now().Before(deadline) {
		typ, body, err := rt.readMessage()
		require.NoError(t, err)
		if typ != protocol.MsgCopyData {
			continue // notices/keepalive-less frames
		}
		require.NotEmpty(t, body)
		switch body[0] {
		case 'w':
			_, walEnd := parseXLogData(t, body)
			sawW = true
			if walEnd > ackLSN {
				ackLSN = walEnd
			}
		case 'k':
			walEnd, _ := parseKeepalive(t, body)
			sawK = true
			if walEnd > ackLSN {
				ackLSN = walEnd
			}
		}
	}
	require.True(t, sawW, "must receive at least one XLogData frame through the tunnel")
	require.True(t, sawK, "must receive at least one keepalive frame through the tunnel")

	rt.sendCopyData(buildStandbyStatus(ackLSN))
	requireConfirmedFlushAdvances(t, setup, slot, ackLSN)
}

// TestStreamReplicationRPCErrorPassthrough verifies a PostgreSQL ErrorResponse
// (here: START_REPLICATION on a nonexistent slot) flows back through the tunnel
// as opaque bytes — a real 'E' message, not a structured infrastructure error.
func TestStreamReplicationRPCErrorPassthrough(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)
	rt := openReplTunnel(t, dialPoolerClient(t, setup), ctx)

	rt.sendQuery("START_REPLICATION SLOT does_not_exist LOGICAL 0/0")

	// The PG ErrorResponse must arrive verbatim as a tunneled 'E' message.
	var sawError bool
	for !sawError {
		typ, body, err := rt.readMessage()
		require.NoError(t, err, "PG error must arrive as opaque data, not a tunnel error")
		if typ == protocol.MsgErrorResponse {
			require.Contains(t, string(body), "does_not_exist")
			sawError = true
		}
	}
	require.True(t, sawError)
}

// TestStreamReplicationRPCLargePayloadByteIdentical verifies a large row (whose
// XLogData spans multiple 64 KB tunnel chunks) arrives byte-identical.
func TestStreamReplicationRPCLargePayloadByteIdentical(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("rpc_big_slot_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	ctx := utils.WithTimeout(t, 60*time.Second)
	rt := openReplTunnel(t, dialPoolerClient(t, setup), ctx)

	rt.sendQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	rt.drainUntilReadyForQuery()
	rt.sendQuery(fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	rt.expectCopyBothResponse()

	// > 1 MiB value, spanning many 64 KB tunnel chunks.
	const bigLen = 1 << 21 // 2 MiB
	big := make([]byte, bigLen)
	for i := range big {
		big[i] = byte('a' + (i % 26))
	}
	insertRow(t, setup, table, 1, string(big))

	// Reassemble all XLogData WAL bytes and confirm the inserted value survives
	// byte-identical across chunk boundaries.
	var wal []byte
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		typ, body, err := rt.readMessage()
		require.NoError(t, err)
		if typ != protocol.MsgCopyData || len(body) == 0 || body[0] != 'w' {
			continue
		}
		wal = append(wal, body[xlogDataHeaderLen:]...)
		if bytes.Contains(wal, big) {
			return // found the full value intact
		}
	}
	t.Fatalf("did not observe the full %d-byte payload byte-identical through the tunnel", bigLen)
}

// TestStreamReplicationRPCCleanTermination verifies a graceful CopyDone through
// the tunnel completes the command (ReadyForQuery flows back) and the permanent
// slot becomes inactive.
func TestStreamReplicationRPCCleanTermination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("rpc_clean_slot_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	ctx := utils.WithTimeout(t, 60*time.Second)
	rt := openReplTunnel(t, dialPoolerClient(t, setup), ctx)

	rt.sendQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	rt.drainUntilReadyForQuery()
	rt.sendQuery(fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	rt.expectCopyBothResponse()

	insertRow(t, setup, table, 1, "bye")
	rt.streamOneFrame(20 * time.Second)

	// Graceful end: CopyDone -> server completes the command (CopyDone +
	// CommandComplete + ReadyForQuery), and the slot goes inactive.
	rt.sendCopyDone()
	rt.drainUntilReadyForQuery()
	requireSlotInactive(t, setup, slot)
}

// TestStreamReplicationRPCAbruptTermination verifies that abruptly dropping the
// gRPC stream (no CopyDone) makes the pooler tear the backend down: the
// temporary slot — owned by that backend session — disappears.
func TestStreamReplicationRPCAbruptTermination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("rpc_abrupt_slot_%d", fixtureCounter.Add(1))

	ctx, cancel := context.WithCancel(utils.WithTimeout(t, 60*time.Second))
	rt := openReplTunnel(t, dialPoolerClient(t, setup), ctx)

	rt.sendQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	rt.drainUntilReadyForQuery()
	rt.sendQuery(fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	rt.expectCopyBothResponse()

	insertRow(t, setup, table, 1, "abrupt")
	rt.streamOneFrame(20 * time.Second)

	// Abrupt client disconnect (no CopyDone): cancel the gRPC stream.
	cancel()

	// The pooler must close the backend; the temporary slot it owned vanishes.
	requireSlotGone(t, setup, slot)
}

// TestStreamReplicationRPCBackpressure verifies the tunnel applies backpressure
// rather than buffering an unbounded backlog: when the client stops reading, the
// pooler's gRPC Send blocks, which makes the tunnel stop reading the postgres
// socket (propagating the stall to PG). After the client resumes, every byte
// must arrive byte-identical and in commit order — no drops, no error.
//
// The pooler keeps only its 64 KiB tunnel buffer plus the gRPC flow-control
// window in memory; the multi-MiB backlog stays in postgres. We can't read the
// pooler's heap from a black-box client, so we assert the observable guarantee:
// correctness under a stalled receiver. The total payload (~12 MiB) far exceeds
// gRPC's largest BDP-grown window, so the stall genuinely engages Send blocking.
func TestStreamReplicationRPCBackpressure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("rpc_bp_slot_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	ctx := utils.WithTimeout(t, 90*time.Second)
	rt := openReplTunnel(t, dialPoolerClient(t, setup), ctx)

	rt.sendQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	rt.drainUntilReadyForQuery()
	rt.sendQuery(fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	rt.expectCopyBothResponse()

	// Generate far more server->client WAL than any gRPC window can hold. Each
	// row carries a unique prefix followed by a large filler, so a byte-identical
	// match of the whole value proves no corruption across chunk boundaries.
	const rows = 24
	const valLen = 512 * 1024 // 512 KiB each => ~12 MiB total
	markers := make([][]byte, rows)
	for i := range rows {
		v := fmt.Sprintf("BPMARK-%04d-", i) + strings.Repeat("x", valLen)
		markers[i] = []byte(v)
		insertRow(t, setup, table, i+1, v)
	}

	// Stall the receiver: stop reading so the pooler's Send blocks and the tunnel
	// stops draining the PG socket. Backpressure must hold the backlog in postgres.
	time.Sleep(4 * time.Second)

	// Resume. Every row must arrive, byte-identical and in commit order.
	var wal []byte
	next := 0
	deadline := time.Now().Add(60 * time.Second)
	for next < rows && time.Now().Before(deadline) {
		typ, body, err := rt.readMessage()
		require.NoError(t, err, "stream must survive backpressure without error")
		if typ != protocol.MsgCopyData || len(body) == 0 || body[0] != 'w' {
			continue
		}
		wal = append(wal, body[xlogDataHeaderLen:]...)
		for next < rows && bytes.Contains(wal, markers[next]) {
			next++
		}
	}
	require.Equal(t, rows, next,
		"all %d rows must arrive byte-identical after the receiver resumes from a stall", rows)
}

// TestStreamReplicationRPCIdleLiveness verifies a long-lived, low-traffic
// replication stream stays alive — the steady state for Realtime. With no WAL to
// send, postgres emits keepalives; the client acks on its timer. The stream must
// survive past both wal_sender_timeout (PG would drop an unacked walsender) and
// the gRPC server keepalive interval (a misconfigured server could reap a
// long-lived stream). A session-scoped low wal_sender_timeout keeps the test
// fast while still exercising the ack path.
func TestStreamReplicationRPCIdleLiveness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedTestSetup(t)
	table, pub := setupReplicationFixture(t, setup)
	slot := fmt.Sprintf("rpc_idle_slot_%d", fixtureCounter.Add(1))
	t.Cleanup(func() { dropReplicationSlot(t, setup, slot) })

	ctx := utils.WithTimeout(t, 90*time.Second)
	rt := openReplTunnel(t, dialPoolerClient(t, setup), ctx)

	// Drive wal_sender_timeout low (session-scoped on this backend's walsender) so
	// PG demands acks frequently — without them it would drop the walsender well
	// inside the idle window below.
	rt.sendQuery("SET wal_sender_timeout = 2000")
	rt.drainUntilReadyForQuery()

	rt.sendQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput NOEXPORT_SNAPSHOT", slot))
	rt.drainUntilReadyForQuery()
	rt.sendQuery(fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '2', publication_names '%s', binary 'true')",
		slot, pub))
	rt.expectCopyBothResponse()

	// Hold idle (no WAL) past both wal_sender_timeout (2s) and the gRPC server
	// keepalive time (10s), acking every keepalive that requests one.
	const idleWindow = 14 * time.Second
	grpcPingMark := time.Now().Add(11 * time.Second) // just past the 10s server keepalive
	deadline := time.Now().Add(idleWindow)
	var keepalivesAfterGrpcPing int
	for time.Now().Before(deadline) {
		typ, body, err := rt.readMessage()
		require.NoError(t, err, "idle replication stream must stay alive (no PG timeout, no gRPC reap)")
		if typ != protocol.MsgCopyData || len(body) == 0 || body[0] != 'k' {
			continue
		}
		walEnd, replyRequested := parseKeepalive(t, body)
		if replyRequested {
			rt.sendCopyData(buildStandbyStatus(walEnd))
		}
		if time.Now().After(grpcPingMark) {
			keepalivesAfterGrpcPing++
		}
	}
	require.Positive(t, keepalivesAfterGrpcPing,
		"must keep receiving keepalives past the gRPC keepalive interval — the stream stayed live")

	// Final proof the stream is still fully usable: a fresh INSERT must stream
	// through as an XLogData frame.
	insertRow(t, setup, table, 1, "still-alive")
	require.Positive(t, rt.streamOneFrame(20*time.Second),
		"stream must still deliver WAL after the long idle period")
}
