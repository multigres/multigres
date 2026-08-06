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
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shortTempDir returns a tmp dir under /tmp rather than t.TempDir() to keep
// Unix socket paths within sun_path's 104-byte limit on macOS.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "p")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// listenUnix starts a Unix socket listener at path and registers cleanup.
func listenUnix(t *testing.T, path string) string {
	t.Helper()
	require.LessOrEqualf(t, len(path), 104, "unix socket path exceeds sun_path limit: %s", path)
	l, err := net.Listen("unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	return path
}

// listenTCP starts a TCP listener on the given host:0 and returns the assigned
// port. t.Skip()s if the host can't be bound (e.g. "::1" on an IPv4-only CI
// runner) so these tests don't flake by environment.
func listenTCP(t *testing.T, host string) int {
	t.Helper()
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Skipf("cannot listen on %s: %v", host, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().(*net.TCPAddr).Port
}

// closedTCPPort returns a port that was briefly bound and then closed, so
// nothing is listening on it. Useful for "not accepting" assertions.
func closedTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func TestPostgresAccepting(t *testing.T) {
	t.Run("live unix socket returns true", func(t *testing.T) {
		dir := shortTempDir(t)
		sock := listenUnix(t, filepath.Join(dir, "pg.sock"))
		// host/port are ignored when a socket file is configured.
		assert.True(t, postgresAccepting(sock, "", closedTCPPort(t)))
	})

	t.Run("missing unix socket returns false", func(t *testing.T) {
		dir := shortTempDir(t)
		assert.False(t, postgresAccepting(filepath.Join(dir, "missing.sock"), "", 0))
	})

	t.Run("stale socket file returns false", func(t *testing.T) {
		dir := shortTempDir(t)
		path := filepath.Join(dir, "stale.sock")
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		assert.False(t, postgresAccepting(path, "", 0))
	})

	t.Run("tcp fallback when no socket", func(t *testing.T) {
		port := listenTCP(t, "127.0.0.1")
		assert.True(t, postgresAccepting("", "127.0.0.1", port))
	})

	t.Run("tcp wildcard host dials localhost", func(t *testing.T) {
		port := listenTCP(t, "127.0.0.1")
		assert.True(t, postgresAccepting("", "", port))
		assert.True(t, postgresAccepting("", "0.0.0.0", port))
	})

	t.Run("tcp not listening returns false", func(t *testing.T) {
		port := closedTCPPort(t)
		assert.False(t, postgresAccepting("", "127.0.0.1", port))
	})
}
