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
	"strconv"

	"github.com/multigres/multigres/go/common/timeouts"
)

// grpcAccepting verifies the gRPC server is accepting connections. Prefers the
// Unix socket, and falls back to a TCP dial on the configured bind address when
// only the port is configured. When neither is configured, gRPC is not in use
// and the check is skipped.
//
// This mirrors pgctld's readiness dial (go/cmd/pgctld/command/ready.go); the
// two are intentionally kept as small, self-contained probes rather than a
// shared dependency (consolidating both into go/tools/netutil is a follow-up).
func grpcAccepting(socketPath, bindAddress string, port int) bool {
	if socketPath != "" {
		return unixSocketAccepting(socketPath)
	}
	if port != 0 {
		return tcpAccepting(bindAddress, port)
	}
	return true
}

// unixSocketAccepting returns true if a process is currently accepting
// connections on the given Unix socket path. A successful Dial means the kernel
// handed off to a listener, distinguishing a live listener from a stale socket
// file left behind (by a crash, for example). Any dial error (e.g. ENOENT,
// ECONNREFUSED, EACCES, timeout) returns false.
func unixSocketAccepting(path string) bool {
	conn, err := net.DialTimeout("unix", path, timeouts.ReadyDialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// tcpAccepting returns true if a TCP listener is accepting on the given bind
// address and port. A wildcard bind address ("", "0.0.0.0", or "::") is dialed
// as "localhost" so the probe works on IPv4-only, IPv6-only, and dual-stack
// hosts; a specific address is dialed directly.
func tcpAccepting(bindAddress string, port int) bool {
	host := bindAddress
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), timeouts.ReadyDialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
