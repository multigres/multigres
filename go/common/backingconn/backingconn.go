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
		return errors.New("host names more than one server; a single server is required")
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
