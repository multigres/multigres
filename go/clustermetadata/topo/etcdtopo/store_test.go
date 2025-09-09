/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcdtopo

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/test"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Use the global port allocator for consistent port allocation across all tests

// checkPortAvailable checks if a port is available for binding
func checkPortAvailable(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return fmt.Errorf("port %d is already in use - this could be from a previous test run, another service, or a port conflict. Try running 'lsof -i :%d' to see what's using it", port, port)
	}
	ln.Close()
	return nil
}

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T, port int) (string, *exec.Cmd) {
	// Check if etcd is available in PATH
	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd not found in PATH")

	// Create a temporary directory.
	dataDir := t.TempDir()

	// Get our two ports to listen to.
	if port == 0 {
		port = utils.GetNextEtcd2Port()
	}

	// Check if ports are available before starting etcd
	err = checkPortAvailable(port)
	require.NoError(t, err, "Port check failed")
	err = checkPortAvailable(port + 1)
	require.NoError(t, err, "Peer port check failed")

	name := "multigres_unit_test"
	clientAddr := fmt.Sprintf("http://localhost:%v", port)
	peerAddr := fmt.Sprintf("http://localhost:%v", port+1)
	initialCluster := fmt.Sprintf("%v=%v", name, peerAddr)

	cmd := exec.Command("etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)
	err = cmd.Start()
	require.NoError(t, err, "failed to start etcd")

	// Create a client to connect to the created etcd.
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "newCellClient(%v) failed", clientAddr)
	defer cli.Close()

	// Wait until we can list "/", or timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	for {
		if _, err := cli.Get(ctx, "/"); err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("Failed to start etcd daemon in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		// Ensure the process is killed and cleaned up
		if cmd.Process != nil {
			// Try graceful shutdown first
			if err := cmd.Process.Signal(os.Interrupt); err == nil {
				// Wait a bit for graceful shutdown
				time.Sleep(100 * time.Millisecond)
			}

			// Force kill if still running
			if err := cmd.Process.Kill(); err != nil {
				slog.Error("cmd.Process.Kill() failed killing etcd", "error", err)
			}

			// Wait for process to finish
			if err := cmd.Wait(); err != nil {
				// Ignore "signal: killed" and "signal: interrupt" errors as they're expected
				if !strings.Contains(err.Error(), "signal: killed") && !strings.Contains(err.Error(), "signal: interrupt") {
					slog.Error("cmd.Wait() failed killing etcd", "error", err)
				}
			}
		}

		// Additional cleanup: try to release the ports
		time.Sleep(50 * time.Millisecond)
	})

	return clientAddr, cmd
}

func TestEtcd2Topo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping topology etcd integration test in short mode")
	}
	// Start a single etcd in the background.
	clientAddr, _ := startEtcd(t, 0)

	testIndex := 0
	newServer := func() topo.Store {
		// Each test will use its own subdirectories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("etcd2", path.Join(testRoot, topo.GlobalCell), []string{clientAddr})
		require.NoError(t, err, "OpenServer() failed")

		// Create the CellInfo.
		err = ts.CreateCell(context.Background(), test.LocalCellName, &clustermetadatapb.Cell{
			ServerAddresses: []string{clientAddr},
			Root:            path.Join(testRoot, test.LocalCellName),
		})
		require.NoError(t, err, "CreateCellInfo() failed")

		return ts
	}

	// Run the TopoServerTestSuite tests.
	ctx := t.Context()
	test.TopoServerTestSuite(t, ctx, func() topo.Store {
		return newServer()
	})

	// Run etcd-specific tests.
	ts := newServer()
	testDatabaseLock(t, ts)
	ts.Close()
}

// testDatabaseLock tests etcd-specific heartbeat (TTL).
// Note TTL granularity is in seconds, even though the API uses time.Duration.
// So we have to wait a long time in these tests.
func testDatabaseLock(t *testing.T, ts topo.Store) {
	ctx := context.Background()
	databasePath := path.Join(topo.DatabasesPath, "test_database")
	err := ts.CreateDatabase(ctx, "test_database", &clustermetadatapb.Database{})
	require.NoError(t, err, "CreateKeyspace")

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err, "ConnForCell failed")

	// Long TTL, unlock before lease runs out.
	leaseTTL = 1000
	lockDescriptor, err := conn.Lock(ctx, databasePath, "ttl")
	require.NoError(t, err, "Lock failed")
	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")

	// Short TTL, make sure it doesn't expire.
	leaseTTL = 1
	lockDescriptor, err = conn.Lock(ctx, databasePath, "short ttl")
	require.NoError(t, err, "Lock failed")
	time.Sleep(2 * time.Second)
	err = lockDescriptor.Unlock(ctx)
	require.NoError(t, err, "Unlock failed")
}
