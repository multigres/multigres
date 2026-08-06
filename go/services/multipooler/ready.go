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

// postgresAccepting reports whether PostgreSQL is currently accepting
// connections on the multipooler's configured leg to postgres: the Unix socket
// when socketFile is set, otherwise a TCP dial to host:port (matching how
// init.go chooses the postgres address). A successful dial means the postmaster
// handed off to a listener, distinguishing a live server from a dead or
// FATAL-looping one that left a stale socket file behind (by a crash, for
// example). Any dial error (ENOENT, ECONNREFUSED, timeout, ...) returns false.
//
// This mirrors pgctld's readiness dial (go/cmd/pgctld/command/ready.go); the
// two are intentionally kept as small, self-contained probes rather than a
// shared dependency.
func postgresAccepting(socketFile, host string, port int) bool {
	network, address := "unix", socketFile
	if socketFile == "" {
		// TCP fallback. A wildcard/empty host is dialed as "localhost" so the
		// resolver picks a working loopback address on IPv4-only, IPv6-only, and
		// dual-stack hosts.
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "localhost"
		}
		network = "tcp"
		address = net.JoinHostPort(host, strconv.Itoa(port))
	}
	conn, err := net.DialTimeout(network, address, timeouts.ReadyDialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
