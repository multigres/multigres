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

package servenv

// Client-certificate authorization shared by the gRPC and HTTP transports.
// Each adapts its own request type to certs plus an allow-list and calls in
// here, so their two allow-list flags stay independent but match alike.

import (
	"crypto/x509"
	"fmt"
	"slices"
	"strings"
)

// certSubjectMatches reports whether any cert's subject contains one of
// substrings.
//
// The match is unanchored against the rendered DN, so an entry also admits
// subjects extending it: "CN=ns-team-a" matches "CN=ns-team-a-evil". Where the
// allow-list is a trust boundary between holders of certs from one CA, anchor
// entries on the next RDN's delimiter ("CN=ns-team-a,O=..."); pkix escapes
// commas inside values, so a caller cannot forge that boundary from within its
// own CN. A bare-CN subject has no trailing delimiter and cannot be anchored at
// all. Comparing parsed identities is the real fix, and would change the
// meaning of every existing allow-list.
func certSubjectMatches(certs []*x509.Certificate, substrings []string) bool {
	for _, substring := range substrings {
		for _, cert := range certs {
			if strings.Contains(cert.Subject.String(), substring) {
				return true
			}
		}
	}
	return false
}

// parseCertSubstrings splits and validates a colon-separated allow-list. The
// error names no flag; callers wrap it with their own.
func parseCertSubstrings(raw string) ([]string, error) {
	substrings := strings.Split(raw, ":")
	// An empty entry would match every subject, authorizing all clients.
	if slices.Contains(substrings, "") {
		return nil, fmt.Errorf("must be a non-empty colon-separated list without empty entries, got %q", raw)
	}
	return substrings, nil
}
