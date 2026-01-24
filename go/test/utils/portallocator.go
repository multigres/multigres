//go:build !windows

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
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// GetFreePort returns a port number allocated by a coordinator daemon that
// prevents port collisions across concurrent tests.
//
// # Architecture
//
// A single coordinator daemon runs per test process, managing port allocation
// via a Unix socket and lease table. The coordinator:
//   - Allocates ports by asking the OS for ephemeral ports (net.Listen with :0)
//   - Tracks allocated ports in a lease table to prevent duplicates
//   - Retries up to 10 times if the OS returns an already-leased port
//   - Releases ports when tests call their cleanup callbacks
//   - Shuts down after 2 seconds of inactivity with no active leases
//
// # Leader Election
//
// The first test to call GetFreePort becomes the coordinator leader using flock:
//  1. Try to connect to existing coordinator (fast path)
//  2. If connection fails, attempt non-blocking flock on coordinator.lock
//  3. If lock succeeds → become leader, start coordinator
//  4. If lock fails → wait for coordinator to become ready (someone else is leader)
//
// When the leader test finishes, it shuts down the coordinator synchronously,
// ensuring all cleanup (socket removal, lock release) completes before shutdown
// returns. The next test needing a port will automatically restart it with a
// fresh lease table.
//
// # Port Lifecycle
//
// Each test that allocates a port registers a t.Cleanup handler to release it.
// The coordinator tracks leased ports until they're explicitly released or the
// coordinator restarts (which clears the entire lease table).
func GetFreePort(t *testing.T) int {
	t.Helper()

	dir := coordDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir coordinator dir: %v", err)
	}

	sockPath := filepath.Join(dir, "port-coordinator.sock")
	lockPath := filepath.Join(dir, "port-coordinator.lock")

	// Fast path: coordinator already running.
	if port, ok := tryRequestPort(t, sockPath); ok {
		registerCleanup(t, sockPath, port)
		return port
	}

	// Coordinator not running. Try to become leader with non-blocking lock.
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open lock file: %v", err)
	}

	// Try non-blocking exclusive lock
	err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		// Someone else has the lock - they're starting the coordinator
		_ = lockFile.Close()

		// Wait for coordinator to become available
		port, ok := tryRequestPortWithRetry(t, sockPath, 4*time.Second)
		if !ok {
			t.Fatalf("coordinator did not become ready")
		}
		registerCleanup(t, sockPath, port)
		return port
	}

	// We got the lock! Double check coordinator isn't already running
	if port, ok := tryRequestPort(t, sockPath); ok {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		registerCleanup(t, sockPath, port)
		return port
	}

	// We are the leader. Start the coordinator in this process and hold the lock until process exit.
	startCoordinatorOnce(sockPath, lockFile)

	// Register cleanup to shut down coordinator when this test finishes
	registerCoordinatorCleanup(t)

	// Now request a port.
	port, ok := tryRequestPortWithRetry(t, sockPath, 2*time.Second)
	if !ok {
		t.Fatalf("coordinator did not become ready")
	}
	registerCleanup(t, sockPath, port)
	return port
}

func registerCleanup(t *testing.T, sockPath string, port int) {
	t.Cleanup(func() {
		_ = releasePort(sockPath, port)
	})
}

func registerCoordinatorCleanup(t *testing.T) {
	t.Cleanup(func() {
		shutdownCoordinator()
	})
}

// shutdownCoordinator stops the coordinator and waits for cleanup to complete.
// When this function returns, the coordinator goroutine has finished, the Unix
// socket is removed, and the flock is released. This synchronous shutdown
// eliminates race conditions when restarting the coordinator.
func shutdownCoordinator() {
	coordMu.Lock()

	if coordShutdown == nil {
		// Already shutdown
		coordMu.Unlock()
		return
	}

	// Cancel context to signal coordinator to stop
	coordShutdown()
	coordShutdown = nil

	// Release lock before waiting to avoid deadlock
	coordMu.Unlock()

	// Wait for coordinator goroutine to complete cleanup (synchronous)
	coordWg.Wait()

	// Reset coordOnce so coordinator can be restarted
	coordMu.Lock()
	coordOnce = sync.Once{}
	coordMu.Unlock()
}

func tryRequestPort(t *testing.T, sockPath string) (int, bool) {
	t.Helper()
	conn, err := net.DialTimeout("unix", sockPath, 50*time.Millisecond)
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("GET\n")); err != nil {
		return 0, false
	}

	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, false
	}
	line = strings.TrimSpace(line)
	p, err := strconv.Atoi(line)
	if err != nil || p <= 0 {
		return 0, false
	}
	return p, true
}

func tryRequestPortWithRetry(t *testing.T, sockPath string, timeout time.Duration) (int, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if port, ok := tryRequestPort(t, sockPath); ok {
			return port, true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return 0, false
}

func releasePort(sockPath string, port int) error {
	conn, err := net.DialTimeout("unix", sockPath, 50*time.Millisecond)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = fmt.Fprintf(conn, "REL %d\n", port)
	return err
}

func coordDir() string {
	return filepath.Join(os.TempDir(), "e2e-port-coordinator")
}

var (
	coordOnce     sync.Once
	coordShutdown context.CancelFunc
	coordWg       sync.WaitGroup // tracks coordinator goroutine for synchronous shutdown
	coordMu       sync.Mutex
)

func startCoordinatorOnce(sockPath string, lockFile *os.File) {
	coordMu.Lock()
	defer coordMu.Unlock()

	coordOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		coordShutdown = cancel

		coordWg.Go(func() {
			_ = runCoordinator(ctx, sockPath, lockFile)
		})
	})
}

type leaseTable struct {
	mu     sync.Mutex
	leases map[int]struct{}
}

func newLeaseTable() *leaseTable {
	return &leaseTable{
		leases: make(map[int]struct{}),
	}
}

// allocatePort asks the OS for an ephemeral port and verifies it's not already
// leased. If the OS returns a port that's already in the lease table (rare but
// possible), it retries up to 10 times before giving up.
func (lt *leaseTable) allocatePort() (int, error) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	const maxAttempts = 10
	var port int

	for range maxAttempts {
		// Ask OS for an ephemeral port
		lis, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return 0, err
		}
		port = lis.Addr().(*net.TCPAddr).Port
		_ = lis.Close()

		// Check if we've already leased this port
		if _, exists := lt.leases[port]; !exists {
			// Found an unleased port
			lt.leases[port] = struct{}{}
			return port, nil
		}
		// Port collision - retry
	}

	return 0, fmt.Errorf("failed to allocate unique port after %d attempts", maxAttempts)
}

func (lt *leaseTable) release(port int) {
	lt.mu.Lock()
	delete(lt.leases, port)
	lt.mu.Unlock()
}

func runCoordinator(ctx context.Context, sockPath string, lockFile *os.File) error {
	// lockFile is held with LOCK_EX. Keep it open for daemon lifetime.
	// Best-effort cleanup of stale socket file.
	_ = os.Remove(sockPath)

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		return err
	}

	// Ensure socket permissions allow other test processes to connect.
	_ = os.Chmod(sockPath, 0o666)

	lt := newLeaseTable()

	// Periodic cleanup and idle shutdown
	const idleTimeout = 2 * time.Second
	lastActivity := time.Now()
	activityCh := make(chan struct{}, 1)

	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	defer cleanupCancel()

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-cleanupCtx.Done():
				return
			case <-activityCh:
				lastActivity = time.Now()
			case <-ticker.C:
				lt.mu.Lock()
				hasLeases := len(lt.leases) > 0
				lt.mu.Unlock()

				// Shut down if idle and no active leases
				if !hasLeases && time.Since(lastActivity) > idleTimeout {
					_ = lis.Close()
					return
				}
			}
		}
	}()

	// Accept connections until context canceled
	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()

	for {
		c, err := lis.Accept()
		if err != nil {
			_ = lis.Close()
			_ = os.Remove(sockPath)
			_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
			_ = lockFile.Close()

			// Reset coordOnce so coordinator can be restarted
			coordMu.Lock()
			coordOnce = sync.Once{}
			coordShutdown = nil
			coordMu.Unlock()

			return err
		}

		// Signal activity
		select {
		case activityCh <- struct{}{}:
		default:
		}

		go handleConn(c, lt)
	}
}

func handleConn(c net.Conn, lt *leaseTable) {
	defer c.Close()

	r := bufio.NewReader(c)
	line, err := r.ReadString('\n')
	if err != nil {
		return
	}
	line = strings.TrimSpace(line)

	if line == "GET" {
		port, err := lt.allocatePort()
		if err != nil {
			_, _ = c.Write([]byte("0\n"))
			return
		}
		_, _ = fmt.Fprintf(c, "%d\n", port)
		return
	}

	if strings.HasPrefix(line, "REL ") {
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			return
		}
		p, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return
		}
		lt.release(p)
		_, _ = c.Write([]byte("OK\n"))
		return
	}

	_, _ = c.Write([]byte("ERR\n"))
}
