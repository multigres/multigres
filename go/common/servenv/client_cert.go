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

// Client-certificate authorization for both transports, which match by
// different rules: HTTP compares parsed subject attributes for exact
// equality, gRPC tests the rendered subject for a substring. Only the former
// is a sound trust boundary between holders of certificates from one CA - see
// certSubjectMatches for why. They sit together so the difference is visible
// from either side.

import (
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-ldap/ldap/v3"
)

// certSubjectMatches reports whether any cert's subject contains one of
// substrings, backing --grpc-auth-mtls-allowed-substrings.
//
// The test is unanchored, and no entry can anchor it: pkix escapes commas
// inside attribute values but not "=", so a certificate with CN
// "evilCN=ns-team-a" renders as "CN=evilCN=ns-team-a,O=acme" and contains the
// entry "CN=ns-team-a,O=acme" outright. Where the allow-list separates
// holders of certificates from one CA, certSubjectEquals is the sound
// comparison.
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

// certSubject is one parsed allow-list entry: the subject attributes an
// accepted certificate must carry. An attribute may be named more than once,
// since a subject may carry it more than once - "OU=eng,OU=platform" requires
// both. Values are conjunctive within an entry and disjunctive across entries,
// one entry per flag occurrence.
//
// Matching is a subset test: attributes and values an entry does not name are
// unconstrained, so a subject carrying everything named plus more still
// matches. Naming more values narrows what qualifies but never excludes such a
// subject; that would need an exact-set comparison.
type certSubject struct {
	attrs map[string][]string
}

// certSubjectEquals reports whether any cert satisfies one of the allowed
// entries, comparing subject attributes for exact equality. A subject that
// embeds an entry inside one of its own attribute values does not match.
func certSubjectEquals(certs []*x509.Certificate, allowed []certSubject) bool {
	for _, cert := range certs {
		have, err := subjectAttrs(cert)
		if err != nil {
			// A subject that will not parse authorizes nothing.
			continue
		}
		for _, want := range allowed {
			if want.matches(have) {
				return true
			}
		}
	}
	return false
}

// subjectAttrs parses a certificate's rendered subject into its attributes,
// keyed by uppercased type and holding every value for that type, since one
// subject may carry an attribute more than once.
//
// Both sides of the comparison go through ldap.ParseDN, so an allow-list entry
// is written exactly as pkix.Name.String() renders the subject. That includes
// attributes pkix has no short name for, which it renders in OID form
// ("1.2.3.4=value"), so any attribute a certificate carries can be named.
func subjectAttrs(cert *x509.Certificate) (map[string][]string, error) {
	dn, err := ldap.ParseDN(cert.Subject.String())
	if err != nil {
		return nil, err
	}
	attrs := make(map[string][]string, len(dn.RDNs))
	for _, rdn := range dn.RDNs {
		for _, av := range rdn.Attributes {
			attr := strings.ToUpper(av.Type)
			attrs[attr] = append(attrs[attr], av.Value)
		}
	}
	return attrs, nil
}

func (cs certSubject) matches(have map[string][]string) bool {
	for attr, wants := range cs.attrs {
		for _, want := range wants {
			if !slices.Contains(have[attr], want) {
				return false
			}
		}
	}
	return len(cs.attrs) > 0
}

// parseCertSubjects validates an allow-list of subject entries, one per
// --http-auth-mtls-allowed-subjects occurrence. Each entry is an RFC 4514
// distinguished name naming the attributes a certificate must carry, for
// example "CN=ns-team-a,O=acme".
//
// ldap.ParseDN does the tokenizing, so entries follow the same escaping
// pkix.Name.String() emits - a value holding a separator renders escaped, as
// in "CN=Doe\, John,O=acme", and has to read back the same way.
//
// ParseDN accepts more than this allow-list does, hence the checks below: it
// reads an empty value ("CN=") and an empty DN ("") without complaint.
//
// Any attribute name a subject can carry is legal in an entry, so an entry
// naming one a certificate lacks matches nothing rather than failing here.
// requireClientCert logs the rejected subject, which is where that shows up.
//
// An attribute may appear more than once, naming several values it must carry;
// the same value twice is a mistake and is rejected.
//
// The error names no flag; callers wrap it with their own.
func parseCertSubjects(entries []string) ([]certSubject, error) {
	if len(entries) == 0 {
		return nil, errors.New("must name at least one subject")
	}
	subjects := make([]certSubject, 0, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry) == "" {
			return nil, errors.New("must not contain empty entries")
		}
		dn, err := ldap.ParseDN(entry)
		if err != nil {
			return nil, fmt.Errorf("entry %q: %w", entry, err)
		}
		cs := certSubject{attrs: map[string][]string{}}
		for _, rdn := range dn.RDNs {
			for _, av := range rdn.Attributes {
				attr := strings.ToUpper(av.Type)
				if av.Value == "" {
					return nil, fmt.Errorf("entry %q: attribute %q has an empty value", entry, av.Type)
				}
				if slices.Contains(cs.attrs[attr], av.Value) {
					return nil, fmt.Errorf("entry %q: attribute %q repeats the value %q", entry, av.Type, av.Value)
				}
				cs.attrs[attr] = append(cs.attrs[attr], av.Value)
			}
		}
		if len(cs.attrs) == 0 {
			return nil, fmt.Errorf("entry %q: names no subject attributes", entry)
		}
		subjects = append(subjects, cs)
	}
	return subjects, nil
}
