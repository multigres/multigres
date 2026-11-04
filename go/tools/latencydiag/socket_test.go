// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package latencydiag

import (
	"context"
	"crypto/tls"
	"net"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSocketSnapshotsLinuxAndBounds(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	peer, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer peer.Close()
	conn, err := listener.Accept()
	require.NoError(t, err)
	defer conn.Close()
	c, err := Begin(Options{ID: "sockets", SampleEvery: 1, MaxEvents: 100, Sockets: true})
	require.NoError(t, err)
	defer c.Stop()
	root := Message(context.Background(), 1, 1)
	// The TLS wrapper's underlying socket is inspected without a handshake or
	// consuming any bytes. Frontend and PostgreSQL PID namespaces stay distinct.
	ObserveSocket(root, 1, tls.Server(conn, &tls.Config{MinVersion: tls.VersionTLS12}))
	ObserveBackendSocket(root, 1, peer)
	snapshots := c.socketSamples()
	require.Len(t, snapshots, 2)
	for _, sample := range snapshots {
		require.NotEmpty(t, sample.Peer)
		require.NotEmpty(t, sample.Local)
		if runtime.GOOS == "linux" {
			require.Equal(t, "ok", sample.Status)
			require.NotNil(t, sample.Info)
		} else {
			require.Equal(t, "unsupported_os", sample.Status)
		}
	}
	for i := uint32(2); i < 300; i++ {
		ObserveSocket(root, i, conn)
	}
	c.mu.Lock()
	size := len(c.sockets)
	omitted := c.socketOmitted
	c.mu.Unlock()
	require.Equal(t, 256, size)
	require.Positive(t, omitted)
	root.End(nil, 0)
	c.Stop()
	require.Empty(t, c.sockets)
}
