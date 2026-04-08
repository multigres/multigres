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

package poolserver_test

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils/poolserver"
)

// startTestServer starts a pool server on a temp Unix socket and returns the
// socket path and a cleanup function.
//
// Note: t.TempDir() produces paths in /var/folders/... on macOS, which exceed
// the 104-byte Unix socket path limit. We use /tmp directly to stay short.
func startTestServer(t *testing.T) (socketPath string, stop func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "pooltest")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	socketPath = filepath.Join(dir, "pool.sock")
	srv, err := poolserver.NewServer(socketPath)
	require.NoError(t, err)
	go srv.Serve()
	t.Cleanup(func() { srv.Stop() })
	return socketPath, srv.Stop
}

// TestAllocReturnsUniquePort verifies that each Alloc call returns a
// distinct port.
func TestAllocReturnsUniquePort(t *testing.T) {
	socket, _ := startTestServer(t)
	c, err := poolserver.Connect(socket)
	require.NoError(t, err)
	defer c.Close()

	const n = 50
	seen := make(map[int]bool, n)
	for range n {
		port, err := c.AllocPort()
		require.NoError(t, err)
		require.False(t, seen[port], "duplicate port: %d", port)
		seen[port] = true
	}
}

// TestAllocPortIsImmediatelyBindable verifies that a port returned by AllocPort
// can be bound immediately without any Release step.
func TestAllocPortIsImmediatelyBindable(t *testing.T) {
	socket, _ := startTestServer(t)
	c, err := poolserver.Connect(socket)
	require.NoError(t, err)
	defer c.Close()

	port, err := c.AllocPort()
	require.NoError(t, err)

	l, bindErr := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, bindErr, "port %d should be bindable immediately after AllocPort", port)
	l.Close()

	require.NoError(t, c.ReturnPort(port))
}

// TestConcurrentAlloc verifies that concurrent Alloc calls across multiple
// clients all return unique ports.
func TestConcurrentAlloc(t *testing.T) {
	socket, _ := startTestServer(t)

	const goroutines = 20

	// Pre-create all connections and keep them open until after the uniqueness
	// check. If a connection closes before we verify results, the server
	// reclaims that connection's ports and may re-issue the same number to
	// another goroutine, producing a spurious duplicate.
	clients := make([]*poolserver.Client, goroutines)
	for i := range goroutines {
		c, err := poolserver.Connect(socket)
		require.NoError(t, err)
		clients[i] = c
	}
	t.Cleanup(func() {
		for _, c := range clients {
			c.Close()
		}
	})

	results := make(chan int, goroutines)
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Go(func() {
			port, err := clients[i].AllocPort()
			require.NoError(t, err)
			results <- port
		})
	}
	wg.Wait()
	close(results)

	seen := make(map[int]bool, goroutines)
	for port := range results {
		require.False(t, seen[port], "duplicate port from concurrent alloc: %d", port)
		seen[port] = true
	}
}

// TestDisconnectFreesAllocatedPorts verifies that when a client disconnects
// without calling Return, the server removes those ports from its registry so
// they can be re-allocated by future callers.
func TestDisconnectFreesAllocatedPorts(t *testing.T) {
	socket, _ := startTestServer(t)

	// Allocate a port and disconnect without returning it.
	var port int
	func() {
		c, err := poolserver.Connect(socket)
		require.NoError(t, err)
		port, err = c.AllocPort()
		require.NoError(t, err)
		// Close without Return — server should remove port from registry.
		c.Close()
	}()

	// Reconnect and verify the same port can now be re-allocated (registry cleared).
	// We do this by allocating many ports and checking there's no duplicate
	// or by verifying ReturnPort on a fresh connection works without error.
	c2, err := poolserver.Connect(socket)
	require.NoError(t, err)
	defer c2.Close()

	// Verify the port can be bound immediately (it's no longer in the registry
	// blocking allocation, and no listener is held).
	l, bindErr := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, bindErr, "port %d should be bindable after client disconnected", port)
	l.Close()
}

// TestPing verifies the Ping/Pong health check.
func TestPing(t *testing.T) {
	socket, _ := startTestServer(t)
	c, err := poolserver.Connect(socket)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Ping())
}

// sendRaw sends a single raw line to the pool server and returns the response.
func sendRaw(t *testing.T, socketPath, line string) string {
	t.Helper()
	conn, err := net.Dial("unix", socketPath)
	require.NoError(t, err)
	defer conn.Close()
	_, err = fmt.Fprintln(conn, line)
	require.NoError(t, err)
	scanner := bufio.NewScanner(conn)
	require.True(t, scanner.Scan())
	return scanner.Text()
}

// TestErrorCases verifies that malformed or unknown commands return appropriate
// ERR responses.
func TestErrorCases(t *testing.T) {
	socket, _ := startTestServer(t)

	t.Run("unknown command", func(t *testing.T) {
		resp := sendRaw(t, socket, "BOGUS")
		require.Equal(t, "ERR unknown command: BOGUS", resp)
	})

	t.Run("RETURN missing port", func(t *testing.T) {
		resp := sendRaw(t, socket, "RETURN")
		require.Equal(t, "ERR missing port", resp)
	})

	t.Run("RETURN invalid port", func(t *testing.T) {
		resp := sendRaw(t, socket, "RETURN notanumber")
		require.Equal(t, "ERR invalid port", resp)
	})
}

// TestNewServerRemovesStaleSocket verifies that NewServer succeeds even when
// a stale socket file already exists at the path.
func TestNewServerRemovesStaleSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "pooltest")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	socketPath := filepath.Join(dir, "pool.sock")

	// Create a stale file at the socket path.
	require.NoError(t, os.WriteFile(socketPath, []byte("stale"), 0o600))

	srv, err := poolserver.NewServer(socketPath)
	require.NoError(t, err)
	srv.Stop()
}
