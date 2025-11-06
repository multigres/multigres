// Copyright 2019 The Vitess Authors.
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

package etcdtopo

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/multigres/multigres/go/test/utils"
)

// checkPortAvailable checks if a port is available for binding
func checkPortAvailable(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return fmt.Errorf("port %d is already in use - this could be from a previous test run, another service, or a port conflict. Try running 'lsof -i :%d' to see what's using it", port, port)
	}
	ln.Close()
	return nil
}

// EtcdOptions contains optional configuration for starting etcd.
type EtcdOptions struct {
	// Port is the client port to listen on (peer port will be port+1).
	// If 0, a port will be automatically assigned.
	Port int

	// DataDir is the directory for etcd data storage.
	// If empty, a temporary directory will be created and cleaned up after the test.
	DataDir string
}

// StartEtcd starts an etcd subprocess with default options, and waits for it to be ready.
func StartEtcd(t *testing.T, port int) (string, *exec.Cmd) {
	return StartEtcdWithOptions(t, EtcdOptions{Port: port})
}

// StartEtcdWithOptions starts an etcd subprocess with custom options, and waits for it to be ready.
func StartEtcdWithOptions(t *testing.T, opts EtcdOptions) (string, *exec.Cmd) {
	// Check if etcd is available in PATH
	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd not found in PATH")

	// Create a temporary directory if not specified.
	dataDir := opts.DataDir
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	// Get our two ports to listen to.
	port := opts.Port
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

	// Wrap etcd with run_in_test.sh to ensure cleanup if test process dies
	cmd := exec.Command("run_in_test.sh", "etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	cmd.Env = append(os.Environ(),
		"MULTIGRES_TESTDATA_DIR="+dataDir,
	)

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
