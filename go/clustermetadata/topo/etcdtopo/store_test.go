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
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/test"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const TopoEtcdPortStart = 6700

// startEtcd starts an etcd subprocess, and waits for it to be ready.
func startEtcd(t *testing.T, port int) (string, *exec.Cmd) {
	// Check if etcd is available in PATH
	if _, err := exec.LookPath("etcd"); err != nil {
		t.Fatalf("etcd not found in PATH: %v", err)
	}

	// Create a temporary directory.
	dataDir := t.TempDir()

	// Get our two ports to listen to.
	if port == 0 {
		port = TopoEtcdPortStart
	}
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
	err := cmd.Start()
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}

	// Create a client to connect to the created etcd.
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("newCellClient(%v) failed: %v", clientAddr, err)
	}
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
		// log error
		if err := cmd.Process.Kill(); err != nil {
			slog.Error("cmd.Process.Kill() failed", "error", err)
		}
		// log error
		if err := cmd.Wait(); err != nil {
			slog.Error("cmd.wait() failed", "error", err)
		}
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
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("etcd2", path.Join(testRoot, topo.GlobalCell), []string{clientAddr})
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo.
		if err := ts.CreateCell(context.Background(), test.LocalCellName, &clustermetadatapb.Cell{
			ServerAddresses: []string{clientAddr},
			Root:            path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	}

	// Run the TopoServerTestSuite tests.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() topo.Store {
		return newServer()
	})

	// Run etcd-specific tests.
	ts := newServer()
	testKeyspaceLock(t, ts)
	ts.Close()
}

// TestEtcd2TopoGetTabletsPartialResults confirms that GetTablets handles partial results
// correctly when etcd2 is used along with the normal vtctldclient <-> vtctld client/server
// path.
func TestEtcd2TopoGetTabletsPartialResults(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping topology etcd integration test in short mode")
	}
	ctx := context.Background()
	cells := []string{"cell1", "cell2"}
	root := "/multigres"
	// Start three etcd instances in the background. One will serve the global topo data
	// while the other two will serve the cell topo data.
	globalClientAddr, _ := startEtcd(t, 0)
	cellClientAddrs := make([]string, len(cells))
	cellClientCmds := make([]*exec.Cmd, len(cells))
	cellTSs := make([]topo.Store, len(cells))
	for i := 0; i < len(cells); i++ {
		addr, cmd := startEtcd(t, TopoEtcdPortStart+(i+100*i))
		cellClientAddrs[i] = addr
		cellClientCmds[i] = cmd
	}
	require.Equal(t, len(cells), len(cellTSs))

	// Setup the global topo server.
	globalTS, err := topo.OpenServer("etcd2", path.Join(root, topo.GlobalCell), []string{globalClientAddr})
	require.NoError(t, err, "OpenServer() failed for global topo server: %v", err)

	// Setup the cell topo servers.
	for i, cell := range cells {
		cellTSs[i], err = topo.OpenServer("etcd2", path.Join(root, topo.GlobalCell), []string{cellClientAddrs[i]})
		require.NoError(t, err, "OpenServer() failed for cell %s topo server: %v", cell, err)
	}

	// Create the CellInfo and Tablet records/keys.
	for i, cell := range cells {
		err = globalTS.CreateCell(ctx, cell, &clustermetadatapb.Cell{
			ServerAddresses: []string{cellClientAddrs[i]},
			Root:            path.Join(root, cell),
		})
		require.NoError(t, err, "CreateCell() failed in global cell for cell %s: %v", cell, err)
		err = globalTS.CreateMultiPooler(ctx, &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      cell,
				Name:      fmt.Sprintf("%d", 100+i),
			},
		})
		require.NoError(t, err, "CreateMultiPooler() failed in cell %s: %v", cell, err)
	}

	// This returns stdout and stderr lines as a slice of strings along with the command error.
	getTablets := func(strict bool) ([]string, []string, error) {
		cmd := exec.Command("vtctldclient", "--server", "internal", "--topo-implementation", "etcd2", "--topo-global-server-address", globalClientAddr, "GetTablets", fmt.Sprintf("--strict=%t", strict))
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		// Trim any leading and trailing newlines so we don't have an empty string at
		// either end of the slices which throws off the logical number of lines produced.
		var stdoutLines, stderrLines []string
		if stdout.Len() > 0 { // Otherwise we'll have a 1 element slice with an empty string
			stdoutLines = strings.Split(strings.Trim(stdout.String(), "\n"), "\n")
		}
		if stderr.Len() > 0 { // Otherwise we'll have a 1 element slice with an empty string
			stderrLines = strings.Split(strings.Trim(stderr.String(), "\n"), "\n")
		}
		return stdoutLines, stderrLines, err
	}

	// Execute the vtctldclient command.
	stdout, stderr, err := getTablets(false)
	require.NoError(t, err, "Unexpected error: %v, output: %s", err, strings.Join(stdout, "\n"))
	// We get each of the single tablets in each cell.
	require.Len(t, stdout, len(cells))
	// And no error message.
	require.Len(t, stderr, 0, "Unexpected error message: %s", strings.Join(stderr, "\n"))

	// Stop the last cell topo server.
	cmd := cellClientCmds[len(cells)-1]
	require.NotNil(t, cmd)
	err = cmd.Process.Kill()
	require.NoError(t, err)
	_ = cmd.Wait()

	// Execute the vtctldclient command to get partial results.
	stdout, stderr, err = getTablets(false)
	require.NoError(t, err, "Unexpected error: %v, output: %s", err, strings.Join(stdout, "\n"))
	// We get partial results, missing the tablet from the last cell.
	require.Len(t, stdout, len(cells)-1, "Unexpected output: %s", strings.Join(stdout, "\n"))
	// We get an error message for the cell that was unreachable.
	require.Greater(t, len(stderr), 0, "Unexpected error message: %s", strings.Join(stderr, "\n"))

	// Execute the vtctldclient command with strict enabled.
	_, stderr, err = getTablets(true)
	require.Error(t, err) // We get an error
	// We still get an error message printed to the console for the cell that was unreachable.
	require.Greater(t, len(stderr), 0, "Unexpected error message: %s", strings.Join(stderr, "\n"))

	globalTS.Close()
	for _, cellTS := range cellTSs {
		cellTS.Close()
	}
}

// testKeyspaceLock tests etcd-specific heartbeat (TTL).
// Note TTL granularity is in seconds, even though the API uses time.Duration.
// So we have to wait a long time in these tests.
func testKeyspaceLock(t *testing.T, ts topo.Store) {
	ctx := context.Background()
	databasePath := path.Join(topo.DatabasesPath, "test_database")
	if err := ts.CreateDatabase(ctx, "test_database", &clustermetadatapb.Database{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell failed: %v", err)
	}

	// Long TTL, unlock before lease runs out.
	leaseTTL = 1000
	lockDescriptor, err := conn.Lock(ctx, databasePath, "ttl")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Short TTL, make sure it doesn't expire.
	leaseTTL = 1
	lockDescriptor, err = conn.Lock(ctx, databasePath, "short ttl")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	time.Sleep(2 * time.Second)
	if err := lockDescriptor.Unlock(ctx); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}
