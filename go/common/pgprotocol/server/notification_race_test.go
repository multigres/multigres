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

package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// raceTestNetConn is a goroutine-safe net.Conn implementation for the race
// regression test. The async pusher's flush and the synchronous handler's
// flush both call Write on the underlying conn — bytes.Buffer is not
// goroutine-safe on its own, so wrap it in a mutex.
//
// Locking the underlying conn is independent from the bug under test:
// the race is on bufio.Writer state, which is upstream of this Write.
// Without serialization here, a -race hit on bytes.Buffer would mask
// the actual signal we're chasing.
type raceTestNetConn struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (m *raceTestNetConn) Read(b []byte) (int, error) {
	return 0, io.EOF
}

func (m *raceTestNetConn) Write(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.Write(b)
}

func (m *raceTestNetConn) Bytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]byte, m.buf.Len())
	copy(out, m.buf.Bytes())
	return out
}

func (m *raceTestNetConn) Close() error                       { return nil }
func (m *raceTestNetConn) LocalAddr() net.Addr                { return nil }
func (m *raceTestNetConn) RemoteAddr() net.Addr               { return nil }
func (m *raceTestNetConn) SetDeadline(t time.Time) error      { return nil }
func (m *raceTestNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *raceTestNetConn) SetWriteDeadline(t time.Time) error { return nil }

// newRaceTestConn constructs a Conn wired to a goroutine-safe in-memory
// net.Conn. No listener is attached, so all packet writes must stay on
// the fast path (fit within bufferedWriter's available capacity). The
// regression test uses small packets to guarantee that.
func newRaceTestConn() (*Conn, *raceTestNetConn) {
	netConn := &raceTestNetConn{}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		conn:           netConn,
		bufferedWriter: bufio.NewWriter(netConn),
		txnStatus:      protocol.TxnStatusIdle,
		ctx:            ctx,
		cancel:         cancel,
	}
	return c, netConn
}

// TestAsyncNotificationRace exercises the original race shape: one
// goroutine drives the synchronous reply path (RowDescription +
// DataRow + CommandComplete + ReadyForQuery — the same packets a
// normal pipelined query produces), while a second goroutine pushes
// NotificationResponse packets through the same Conn at the same time.
//
// Before startPacket/writePacket held bufMu across body encoding,
// the two goroutines could interleave byte-by-byte writes into the
// shared bufio.Writer. -race would report a data race on the
// bufio.Writer's internal cursor, and the resulting wire bytes would
// contain torn packets.
//
// With the lock bracket in place, every packet body lands as one
// atomic Write under bufMu. The test confirms two invariants:
//
//  1. -race reports no data race on the shared bufio.Writer state.
//  2. Walking the captured wire bytes by (type, length) consumes
//     exactly to EOF — no truncation, no overrun, no garbage between
//     packets — and every type byte is one of the expected message
//     types.
//
// Run with -race -count=20 to surface flakiness; the harness inside
// drives enough work per iteration that a single -race run is also a
// reasonable signal.
func TestAsyncNotificationRace(t *testing.T) {
	const (
		queryIters        = 2000
		notifIters        = 2000
		fieldName         = "n"
		commandTag        = "SELECT 1"
		notifChannel      = "race"
		notifPayload      = "p"
		concurrentRetries = 5
	)

	for range concurrentRetries {
		c, netConn := newRaceTestConn()

		// Build a small RowDescription/DataRow pair we can reuse so
		// neither side goes off the fast path. Each packet body is
		// well under bufferedWriter's 4 KB available capacity.
		fields := []*query.Field{{
			Name:                 fieldName,
			TableOid:             0,
			TableAttributeNumber: 0,
			DataTypeOid:          23, // int4
			DataTypeSize:         4,
			TypeModifier:         -1,
			Format:               0,
		}}
		row := &sqltypes.Row{Values: []sqltypes.Value{[]byte("1")}}

		var (
			wg      sync.WaitGroup
			handErr atomic.Value // holds error
			pushErr atomic.Value
		)

		// Synchronous handler: tight loop of the wire packets a
		// pipelined query reply produces, plus a flush per "Sync".
		wg.Go(func() {
			for range queryIters {
				if err := c.writeRowDescription(fields); err != nil {
					handErr.Store(err)
					return
				}
				if err := c.writeDataRow(row); err != nil {
					handErr.Store(err)
					return
				}
				if err := c.writeCommandComplete(commandTag); err != nil {
					handErr.Store(err)
					return
				}
				if err := c.writeReadyForQuery(); err != nil {
					handErr.Store(err)
					return
				}
				if err := c.flush(); err != nil {
					handErr.Store(err)
					return
				}
			}
		})

		// Async pusher: same shape as the goroutine started by
		// EnableAsyncNotifications — write a NotificationResponse,
		// then flush. Calls writeNotificationResponseMsg directly so
		// the test does not depend on the channel/select machinery.
		wg.Go(func() {
			for i := range notifIters {
				if err := c.writeNotificationResponseMsg(int32(i+1), notifChannel, notifPayload); err != nil {
					pushErr.Store(err)
					return
				}
				if err := c.flush(); err != nil {
					pushErr.Store(err)
					return
				}
			}
		})

		wg.Wait()
		if v := handErr.Load(); v != nil {
			t.Fatalf("handler goroutine error: %v", v.(error))
		}
		if v := pushErr.Load(); v != nil {
			t.Fatalf("notification goroutine error: %v", v.(error))
		}

		// Final flush in case the last packet from either goroutine
		// is still buffered and the other goroutine's flush already
		// drained the prior bytes.
		require.NoError(t, c.flush())

		wire := netConn.Bytes()
		require.NotEmpty(t, wire)

		// Walk the captured bytes packet-by-packet. Every type byte
		// must match one of the message types we wrote, and the
		// length-prefixed body must consume exactly to EOF. A torn
		// packet (interleaved bodies) shows up either as an unknown
		// type byte mid-stream or as a length that overruns the
		// buffer.
		got, err := walkPackets(wire)
		require.NoError(t, err, "wire bytes are malformed (torn packet?)")

		// Sanity-check counts: each query iteration emits exactly 4
		// packets (T, D, C, Z); each notification iteration emits 1
		// (A). Anything else means a packet was dropped or duplicated.
		assert.Equal(t, queryIters, got['T'], "unexpected RowDescription count")
		assert.Equal(t, queryIters, got['D'], "unexpected DataRow count")
		assert.Equal(t, queryIters, got['C'], "unexpected CommandComplete count")
		assert.Equal(t, queryIters, got['Z'], "unexpected ReadyForQuery count")
		assert.Equal(t, notifIters, got['A'], "unexpected NotificationResponse count")
	}
}

// walkPackets parses a stream of pgwire backend packets (1-byte type +
// 4-byte length-including-self + body). Returns a count of each type
// byte seen. Errors on truncation, overrun, or any unrecognised type
// byte from the set this test produces.
func walkPackets(buf []byte) (map[byte]int, error) {
	counts := map[byte]int{}
	allowed := map[byte]bool{
		protocol.MsgRowDescription:       true,
		protocol.MsgDataRow:              true,
		protocol.MsgCommandComplete:      true,
		protocol.MsgReadyForQuery:        true,
		protocol.MsgNotificationResponse: true,
	}
	for i := 0; i < len(buf); {
		if i+5 > len(buf) {
			return nil, errors.New("truncated header")
		}
		typ := buf[i]
		length := binary.BigEndian.Uint32(buf[i+1 : i+5])
		if length < 4 {
			return nil, errors.New("invalid length: < 4")
		}
		end := i + 1 + int(length)
		if end > len(buf) {
			return nil, errors.New("body overruns buffer")
		}
		if !allowed[typ] {
			return nil, errors.New("unexpected message type byte: " + string([]byte{typ}))
		}
		counts[typ]++
		i = end
	}
	return counts, nil
}
