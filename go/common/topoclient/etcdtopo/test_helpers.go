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
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/retry"
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

// WaitForReady waits for etcd to be ready by querying its /readyz endpoint in a loop.
// metricsAddr must be the HTTP address of etcd's metrics listener (the address passed
// to --listen-metrics-urls), e.g. "http://localhost:2381". /readyz is not served on
// the client listener, so passing the client address here will always return 404.
// Callers should pass a context with an appropriate timeout.
func WaitForReady(ctx context.Context, metricsAddr string) error {
	var lastErr error
	var lastStatusCode int

	for _, err := range retry.New(10*time.Millisecond, time.Second).Attempts(ctx) {
		if err != nil {
			if lastErr != nil {
				// Check if error is "connection refused" and provide helpful diagnostic
				errMsg := lastErr.Error()
				if strings.Contains(errMsg, "connection refused") {
					// Check for orphan etcd processes
					orphanHint := ""
					if out, pgrepErr := exec.Command("pgrep", "-fl", "etcd").Output(); pgrepErr == nil && len(out) > 0 {
						orphanHint = fmt.Sprintf(" Orphan etcd processes detected:\n%s\nTry: pkill -9 etcd", string(out))
					}
					return fmt.Errorf("etcd failed to become ready (connection refused - etcd may have failed to start, or an orphan process may be blocking resources).%s\nLast error: %w", orphanHint, err)
				}
				return fmt.Errorf("etcd failed to become ready (last error: %w): %w", lastErr, err)
			}
			if lastStatusCode != 0 {
				return fmt.Errorf("etcd failed to become ready (last status: %d): %w", lastStatusCode, err)
			}
			return fmt.Errorf("etcd failed to become ready: %w", err)
		}

		url := metricsAddr + "/readyz"
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			lastStatusCode = 0
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = nil
		lastStatusCode = resp.StatusCode
	}
	return errors.New("etcd failed to become ready")
}

// EtcdOptions contains optional configuration for starting etcd.
type EtcdOptions struct {
	// ClientPort is the client port to listen on.
	// If 0, a port will be automatically assigned.
	ClientPort int

	// PeerPort is the peer port for etcd cluster communication.
	// If 0 and ClientPort is also 0, will be automatically assigned.
	// If 0 and ClientPort is specified, defaults to ClientPort+1 for backwards compatibility.
	PeerPort int

	// MetricsPort is the port for etcd's metrics/health listener (--listen-metrics-urls).
	// /readyz is only served on this listener, not on the client listener.
	// If 0, a port will be automatically assigned.
	MetricsPort int

	// DataDir is the directory for etcd data storage.
	// If empty, a temporary directory will be created and cleaned up after the test.
	DataDir string
}

// StartEtcd starts an etcd subprocess with automatically allocated ports.
// Returns the client address, the metrics address (to pass to WaitForReady),
// and the process handle.
func StartEtcd(t *testing.T) (clientAddr, metricsAddr string, cmd *executil.Cmd) {
	clientPort := utils.GetFreePort(t)
	peerPort := utils.GetFreePort(t)
	metricsPort := utils.GetFreePort(t)
	return StartEtcdWithOptions(t, EtcdOptions{
		ClientPort:  clientPort,
		PeerPort:    peerPort,
		MetricsPort: metricsPort,
	})
}

// StartEtcdWithOptions starts an etcd subprocess with custom options, and waits for it to be ready.
// Returns the client address, the metrics address (to pass to WaitForReady), and the process handle.
func StartEtcdWithOptions(t *testing.T, opts EtcdOptions) (clientAddr, metricsAddr string, cmd *executil.Cmd) {
	// Check if etcd is available in PATH
	_, err := exec.LookPath("etcd")
	require.NoError(t, err, "etcd not found in PATH")

	// Create a temporary directory if not specified.
	dataDir := opts.DataDir
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	// Get our ports to listen to - client and peer must be specified; metrics is auto-allocated if unset.
	clientPort := opts.ClientPort
	peerPort := opts.PeerPort
	metricsPort := opts.MetricsPort
	if metricsPort == 0 {
		metricsPort = utils.GetFreePort(t)
	}

	require.NotZero(t, clientPort, "EtcdOptions.ClientPort must be set to a non-zero value")
	require.NotZero(t, peerPort, "EtcdOptions.PeerPort must be set to a non-zero value")

	// Check if ports are available before starting etcd
	err = checkPortAvailable(clientPort)
	require.NoError(t, err, "Port check failed")
	err = checkPortAvailable(peerPort)
	require.NoError(t, err, "Peer port check failed")
	err = checkPortAvailable(metricsPort)
	require.NoError(t, err, "Metrics port check failed")

	name := "multigres_unit_test"
	clientAddr = fmt.Sprintf("http://localhost:%v", clientPort)
	peerAddr := fmt.Sprintf("http://localhost:%v", peerPort)
	metricsAddr = fmt.Sprintf("http://localhost:%v", metricsPort)
	initialCluster := fmt.Sprintf("%v=%v", name, peerAddr)

	// Wrap etcd with run_in_test.sh for orphan protection. Stops gracefully when
	// the test context is cancelled so run_in_test.sh can terminate etcd cleanly.
	cmd = utils.CommandWithOrphanProtection(t.Context(), "etcd",
		"-name", name,
		"-advertise-client-urls", clientAddr,
		"-initial-advertise-peer-urls", peerAddr,
		"-listen-client-urls", clientAddr,
		"-listen-peer-urls", peerAddr,
		"-listen-metrics-urls", metricsAddr,
		"-initial-cluster", initialCluster,
		"-data-dir", dataDir)

	// Set MULTIGRES_TESTDATA_DIR for directory-deletion triggered cleanup
	cmd.Env = append(os.Environ(),
		"MULTIGRES_TESTDATA_DIR="+dataDir,
	)

	err = cmd.Start()
	require.NoError(t, err, "failed to start etcd")

	// Wait for etcd to be ready via the metrics listener (/readyz requires --listen-metrics-urls)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = WaitForReady(ctx, metricsAddr)
	require.NoError(t, err, "etcd failed to become ready")

	return clientAddr, metricsAddr, cmd
}
