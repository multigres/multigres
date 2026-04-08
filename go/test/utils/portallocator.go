// Copyright 2025 Supabase, Inc.
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

package utils

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/utils/poolserver"
)

// portCache tracks ports currently allocated to tests to prevent duplicates
// within a single test binary. Values are always true.
var portCache sync.Map

// poolOnce initialises the singleton pool client exactly once per process.
var (
	poolOnce   sync.Once
	poolClient *poolserver.Client // nil if pool server is not in use
	warnNoPool sync.Once          // prints the "no pool server" warning at most once
)

const (
	poolConnectRetries = 10
	poolConnectDelay   = 500 * time.Millisecond
)

func getPoolClient() *poolserver.Client {
	addr := os.Getenv("MULTIGRES_PORT_POOL_ADDR")
	if addr == "" {
		warnNoPool.Do(func() {
			fmt.Fprintln(os.Stderr,
				"warning: MULTIGRES_PORT_POOL_ADDR is not set; port allocation is not coordinated across parallel test binaries and may cause flaky tests due to port collisions")
		})
		return nil
	}
	poolOnce.Do(func() {
		var err error
		for range poolConnectRetries {
			var c *poolserver.Client
			c, err = poolserver.Connect(addr)
			if err == nil {
				poolClient = c
				return
			}
			time.Sleep(poolConnectDelay)
		}
		fmt.Fprintf(os.Stderr,
			"warning: could not connect to port pool server at %s after %d attempts: %v; falling back to in-process port allocation\n",
			addr, poolConnectRetries, err)
	})
	return poolClient
}

// GetFreePort returns a port number that was verified free by the OS and is
// not currently allocated to another test in this process.
//
// When MULTIGRES_PORT_POOL_ADDR is set and the pool server is reachable, the
// server records the returned port in a central registry. This prevents
// parallel test binaries from receiving the same port from the OS, eliminating
// a class of flaky-test failures caused by port collisions across concurrent
// "go test ./..." invocations. The port is immediately bindable.
//
// When the pool server is not in use, GetFreePort falls back to the original
// in-process behaviour: the OS assigns a free port via net.Listen(":0") and
// the port is tracked in a process-local cache to prevent duplicates.
//
// Ports are automatically released from the cache (and returned to the pool
// server, if in use) when the test completes via t.Cleanup.
func GetFreePort(t *testing.T) int {
	t.Helper()
	if client := getPoolClient(); client != nil {
		return getFreePortFromPool(t, client)
	}
	return getFreePortLocal(t)
}

// getFreePortFromPool allocates a port through the running pool server.
func getFreePortFromPool(t *testing.T, client *poolserver.Client) int {
	t.Helper()

	port, err := client.AllocPort()
	if err != nil {
		t.Fatalf("failed to allocate port from pool server: %v", err)
	}

	// Track in the local cache so GetFreePort never returns it twice within
	// this process (belt-and-suspenders; the server already guarantees this).
	portCache.Store(port, true)

	t.Cleanup(func() {
		portCache.Delete(port)
		if err := client.ReturnPort(port); err != nil {
			t.Logf("warning: failed to return port %d to pool server: %v", port, err)
		}
	})

	return port
}

// getFreePortLocal is the original in-process port allocator used when the
// pool server is not configured.
func getFreePortLocal(t *testing.T) int {
	t.Helper()

	// Track listeners we need to keep open to prevent OS port reuse
	var heldListeners []net.Listener

	// Clean up all held listeners when done
	defer func() {
		for _, lis := range heldListeners {
			lis.Close()
		}
	}()

	for {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("failed to allocate free port: %v", err)
		}

		port := lis.Addr().(*net.TCPAddr).Port

		// Try to claim this port atomically
		_, alreadyExists := portCache.LoadOrStore(port, true)
		if !alreadyExists {
			// Successfully claimed this port
			lis.Close()
			t.Cleanup(func() {
				portCache.Delete(port)
			})
			return port
		}

		// Port already in cache - hold this listener open to prevent OS reuse
		// and try again. Add to slice so we can close all at once when done.
		heldListeners = append(heldListeners, lis)
	}
}
