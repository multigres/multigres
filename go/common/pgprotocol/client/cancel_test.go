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

package client

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/test/utils"
)

const (
	testProcessID uint32 = 4242
	testSecretKey uint32 = 987654321
)

// fakePostmaster listens on a local port, reads one cancel request from the first
// connection, and then runs afterRead on it.
func fakePostmaster(t *testing.T, afterRead func(net.Conn)) (conn *Conn, packets <-chan [cancelRequestLength]byte) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	received := make(chan [cancelRequestLength]byte, 1)
	go func() {
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		var packet [cancelRequestLength]byte
		if _, err := io.ReadFull(c, packet[:]); err != nil {
			return
		}
		received <- packet
		afterRead(c)
	}()

	addr := listener.Addr().(*net.TCPAddr)
	return &Conn{
		config:    &Config{Host: "127.0.0.1", Port: addr.Port},
		processID: testProcessID,
		secretKey: testSecretKey,
	}, received
}

// sendCancel runs SendCancelRequest and fails the test, rather than hanging, if it
// does not return in time.
func sendCancel(t *testing.T, conn *Conn, ctx context.Context) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- conn.SendCancelRequest(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(utils.ScaleTimeout(5 * time.Second)):
		t.Fatal("SendCancelRequest did not return")
		return nil
	}
}

func TestSendCancelRequest_SendsTheCancelPacketAndWaitsForTheServerToClose(t *testing.T) {
	conn, received := fakePostmaster(t, func(net.Conn) {})

	require.NoError(t, conn.SendCancelRequest(utils.WithTimeout(t, 5*time.Second)))

	packet := <-received
	assert.Equal(t, uint32(cancelRequestLength), binary.BigEndian.Uint32(packet[0:]))
	assert.Equal(t, uint32(protocol.CancelRequestCode), binary.BigEndian.Uint32(packet[4:]))
	assert.Equal(t, testProcessID, binary.BigEndian.Uint32(packet[8:]))
	assert.Equal(t, testSecretKey, binary.BigEndian.Uint32(packet[12:]))
}

func TestSendCancelRequest_NoServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	conn := &Conn{config: &Config{Host: "127.0.0.1", Port: port}}

	err = conn.SendCancelRequest(utils.WithTimeout(t, 5*time.Second))

	require.ErrorContains(t, err, "cancel request: failed to connect to 127.0.0.1:")
}

// A server that never closes its end must not hold the caller past its deadline.
func TestSendCancelRequest_ServerNeverCloses(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	conn, _ := fakePostmaster(t, func(net.Conn) { <-release })
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := sendCancel(t, conn, ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// Cancelling the context, with no deadline set, also ends the wait.
func TestSendCancelRequest_ContextCancelledWithoutDeadline(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	conn, received := fakePostmaster(t, func(net.Conn) { <-release })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- conn.SendCancelRequest(ctx) }()

	<-received
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(utils.ScaleTimeout(5 * time.Second)):
		t.Fatal("SendCancelRequest did not return after its context was cancelled")
	}
}

// A connection that fails for a reason other than the context ending reports that error.
func TestSendCancelRequest_ConnectionReset(t *testing.T) {
	conn, _ := fakePostmaster(t, func(c net.Conn) {
		// Closing with no linger resets the connection instead of closing it cleanly.
		_ = c.(*net.TCPConn).SetLinger(0)
	})

	err := sendCancel(t, conn, utils.WithTimeout(t, 5*time.Second))

	require.ErrorContains(t, err, "cancel request:")
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, context.Canceled)
}
