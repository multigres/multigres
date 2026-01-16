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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// GetFreePort returns a port number allocated by the coordinator daemon.
// The coordinator ensures no two concurrent tests receive the same port,
// and ports remain reserved until the test completes (via t.Cleanup).
//
// The coordinator uses SO_REUSEADDR and verification to ensure ports are
// immediately available after allocation, avoiding TIME_WAIT race conditions.
//
// The first test to call GetFreePort becomes the coordinator leader and
// starts the daemon. Subsequent tests connect to the existing coordinator.
// When the test that started the coordinator finishes, it shuts down gracefully,
// and the next test that needs a port will restart it automatically.
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

	// Try to become leader and start coordinator, if needed.
	// We only attempt leadership on unix-like systems.
	if runtime.GOOS == "windows" {
		t.Fatalf("port coordinator: unix socket + flock not supported on windows in this implementation")
	}

	// Try to become leader with non-blocking lock
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
		port, ok := tryRequestPortWithRetry(t, sockPath, 2*time.Second)
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

func shutdownCoordinator() {
	coordMu.Lock()
	defer coordMu.Unlock()

	if coordShutdown != nil {
		coordShutdown()
		coordShutdown = nil
		// Reset coordOnce so coordinator can be restarted by another test
		coordOnce = sync.Once{}
	}
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
	coordMu       sync.Mutex
)

func startCoordinatorOnce(sockPath string, lockFile *os.File) {
	coordOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		coordMu.Lock()
		coordShutdown = cancel
		coordMu.Unlock()

		go func() {
			_ = runCoordinator(ctx, sockPath, lockFile)
		}()
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

func (lt *leaseTable) allocatePort() (int, error) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	const maxAttempts = 10
	var port int

	for range maxAttempts {
		// Let the OS pick a free port
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
