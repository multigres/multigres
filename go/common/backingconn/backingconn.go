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

// Package backingconn holds the rules for the endpoint of a backing connection, the external
// PostgreSQL an unmanaged pooler fronts. The default primary applies them to the connections it
// serves and the pooler applies them to the connection it receives, so that a connection the one
// serves is one the other accepts.
package backingconn

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// ErrMultipleHosts is the refusal of a connection that names a list of hosts.
var ErrMultipleHosts = errors.New("host names more than one server; a single server is required")

// Endpoint returns the one host and port of the lists a connection carries, as libpq's
// host=a,b port=1,2 do. A connection is not given more than one server: the lists exist so that
// the type does not change if it ever is, and this is where more than one is refused. A missing
// host or port comes back empty and is left for Validate to name. A list of ports is checked
// first, so a connection with lists of both is refused for its ports.
func Endpoint[P ~int | ~int32](hosts []string, ports []P) (host string, port int, err error) {
	switch {
	case len(ports) > 1:
		return "", 0, errors.New("port is not a single valid port number")
	case len(hosts) > 1:
		return "", 0, ErrMultipleHosts
	}
	if len(hosts) == 1 {
		host = hosts[0]
	}
	if len(ports) == 1 {
		port = int(ports[0])
	}
	return host, port, nil
}

// Validate reports why a connection cannot be used as a backing connection, or nil.
//
// The connection names one server: the pooler's admin connection must reach the same server as
// its application connections, so that what the pooler learns about the server it probes holds
// for the server the application uses. A list of hosts, a path or credentials in the host could
// send the two apart.
func Validate(host string, port int, database, user string) error {
	switch {
	case strings.TrimSpace(host) == "":
		return errors.New("host is required")
	case strings.Contains(host, ","):
		return ErrMultipleHosts
	case strings.HasPrefix(host, "/"):
		return errors.New("host is a socket directory; a network host is required")
	case strings.ContainsFunc(host, unicode.IsSpace) || strings.ContainsAny(host, "/@"):
		return errors.New("host must be a single hostname or IP address")
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", port)
	}
	if database == "" {
		return errors.New("database is required")
	}
	if user == "" {
		return errors.New("user is required")
	}
	return nil
}
