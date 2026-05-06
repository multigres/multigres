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

// Package poolserver implements a cross-process port allocation service for
// integration tests.
//
// # Problem
//
// Integration tests start real processes (pgctld, multipooler, multiorch, …).
// Each process needs a unique TCP port. The current helper [GetFreePort] calls
// net.Listen(":0") to ask the OS for a free port, then closes the listener and
// returns the port number. This works within a single test binary because the
// returned ports are tracked in a process-local cache.
//
// When multiple test binaries run in parallel (the default with "go test
// ./...") they each have their own cache. The OS may hand the same port to two
// different binaries during the window between when the first binary closes its
// probe listener and when it actually starts its child process. The result is a
// bind failure and a flaky test.
//
// # Solution
//
// The pool server is a long-lived process that maintains a central registry of
// every port it has handed out. When a client calls ALLOC, the server binds
// briefly to get a free port from the OS, closes the listener immediately, and
// records the port number. Because all test binaries route their allocations
// through this single registry, the OS will never hand the same number to two
// concurrent callers — if net.Listen(":0") happens to return a port that is
// already in the registry, allocPort detects the collision, closes that
// listener, and retries.
//
// Because the server does not hold a persistent listener, allocated ports are
// immediately bindable by child processes without any extra Release step.
//
// When the test completes, it calls Return so the server removes the port from
// its registry and the number becomes available for future allocations.
//
// # Usage
//
// Start the pool server once before running the test suite (see
// go/cmd/portpoolserver) and export its socket path:
//
//	./bin/portpoolserver --socket /tmp/mgports.sock &
//	export MULTIGRES_PORT_POOL_ADDR=/tmp/mgports.sock
//	go test ./go/test/endtoend/...
//
// Test processes that have MULTIGRES_PORT_POOL_ADDR set will automatically
// route all [utils.GetFreePort] calls through this server. When the pool
// server is not configured, GetFreePort falls back to its original in-process
// behaviour.
//
// # Protocol
//
// Communication happens over a Unix-domain socket using a simple line-oriented
// text protocol:
//
//	ALLOC          → PORT <n>   (server records port n; caller may bind immediately)
//	RETURN <n>     → OK         (test done; server forgets the port)
//	PING           → PONG
package poolserver

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Server manages a pool of pre-allocated TCP ports for integration tests.
type Server struct {
	socketPath string
	listener   net.Listener // Unix-socket listener

	mu        sync.Mutex
	allocated map[int]struct{} // ports handed out and not yet returned
}

// NewServer creates a pool server that listens on socketPath.
// Any stale socket file at that path is removed before binding.
func NewServer(socketPath string) (*Server, error) {
	_ = os.Remove(socketPath) // remove stale socket; ignore error if absent
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("pool server listen on %s: %w", socketPath, err)
	}
	return &Server{
		socketPath: socketPath,
		listener:   l,
		allocated:  make(map[int]struct{}),
	}, nil
}

// Serve accepts connections and handles requests until Stop is called.
// It blocks until the server listener is closed.
func (s *Server) Serve() {
	defer os.Remove(s.socketPath)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

// Stop shuts down the server.
func (s *Server) Stop() {
	s.listener.Close()
}

// SocketPath returns the Unix socket path this server is listening on.
func (s *Server) SocketPath() string {
	return s.socketPath
}

// handleRequest processes a single client request and returns the response.
// clientPorts is updated in place for ALLOC (append) and RETURN (remove).
// The response is empty for blank lines (no reply should be sent).
func (s *Server) handleRequest(request string, clientPorts *[]int) string {
	fields := strings.Fields(request)

	// Ignore empty lines; don't send an error response since some clients
	// may use newlines as delimiters.
	if len(fields) == 0 {
		return ""
	}

	switch fields[0] {
	case cmdPing:
		return respPong

	case cmdAlloc:
		if port, err := s.allocPort(); err != nil {
			return respPrefixErr + " " + err.Error()
		} else {
			*clientPorts = append(*clientPorts, port)
			return fmt.Sprintf(respPrefixPort+" %d", port)
		}

	case cmdReturn:
		if len(fields) < 2 {
			return respPrefixErr + " missing port"
		}
		if port, err := strconv.Atoi(fields[1]); err != nil {
			return respPrefixErr + " invalid port"
		} else {
			s.returnPort(port)
			*clientPorts = removePort(*clientPorts, port)
			return respOK
		}
	}
	return respPrefixErr + " unknown command: " + fields[0]
}

// reclaimAllocatedPorts removes all ports in clientPorts from the registry.
func (s *Server) reclaimAllocatedPorts(clientPorts []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, port := range clientPorts {
		delete(s.allocated, port)
	}
}

// handleConn serves requests from a single client connection until it disconnects.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	var clientPorts []int // ports allocated to this connection, not yet returned
	scanner := bufio.NewScanner(conn)
	w := bufio.NewWriter(conn)

	for scanner.Scan() {
		if response := s.handleRequest(scanner.Text(), &clientPorts); response != "" {
			fmt.Fprintln(w, response)
			_ = w.Flush()
		}
	}

	// On disconnect: reclaim all ports still owned by this connection.
	// A test process may crash after allocating ports but before calling Return,
	// which would otherwise block those port numbers from being re-allocated for
	// the lifetime of the server.
	s.reclaimAllocatedPorts(clientPorts)
}

// allocPort finds a free port, records it in the registry, and returns it.
// The listener is closed immediately so the port can be bound by callers.
func (s *Server) allocPort() (int, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, fmt.Errorf("open listener: %w", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Rare collision: OS returned a port already in our registry.
	if _, exists := s.allocated[port]; exists {
		return 0, errors.New("port collision, retry")
	}
	s.allocated[port] = struct{}{}
	return port, nil
}

// returnPort removes port from the registry.
func (s *Server) returnPort(port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.allocated, port)
}

func removePort(ports []int, port int) []int {
	for i, p := range ports {
		if p == port {
			return append(ports[:i], ports[i+1:]...)
		}
	}
	return ports
}
