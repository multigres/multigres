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

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCertSubstrings(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []string
		wantErr bool
	}{
		{name: "empty string rejected", raw: "", wantErr: true},
		{name: "empty entry rejected", raw: "client-a::client-b", wantErr: true},
		{name: "trailing empty entry rejected", raw: "client-a:", wantErr: true},
		{name: "single entry", raw: "client-a", want: []string{"client-a"}},
		{name: "multiple entries", raw: "client-a:client-b", want: []string{"client-a", "client-b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCertSubstrings(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "non-empty colon-separated list")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCertSubjectMatches(t *testing.T) {
	certA := generateTestPeerCert(t, "client-a")
	certB := generateTestPeerCert(t, "client-b")

	tests := []struct {
		name       string
		certs      []*x509.Certificate
		substrings []string
		want       bool
	}{
		{name: "no certs", certs: nil, substrings: []string{"client-a"}, want: false},
		{name: "no substrings", certs: []*x509.Certificate{certA}, substrings: nil, want: false},
		{name: "leaf matches", certs: []*x509.Certificate{certA}, substrings: []string{"client-a"}, want: true},
		{name: "leaf does not match", certs: []*x509.Certificate{certB}, substrings: []string{"client-a"}, want: false},
		{name: "match found later in chain", certs: []*x509.Certificate{certB, certA}, substrings: []string{"client-a"}, want: true},
		{name: "match found later in substrings", certs: []*x509.Certificate{certA}, substrings: []string{"client-b", "client-a"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, certSubjectMatches(tt.certs, tt.substrings))
		})
	}
}

func generateTestPeerCert(t *testing.T, cn string) *x509.Certificate {
	t.Helper()
	return generateTestPeerCertWithOrg(t, cn, "")
}

// generateTestPeerCertWithOrg is generateTestPeerCert with a second RDN, so
// tests can exercise subjects that render with an RDN delimiter after the CN.
func generateTestPeerCertWithOrg(t *testing.T, cn, org string) *x509.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	subject := pkix.Name{CommonName: cn}
	if org != "" {
		subject.Organization = []string{org}
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}

// TestCertSubjectMatches_Anchoring characterizes the substring semantics
// documented on certSubjectMatches, which callers relying on the allow-list
// as a tenant boundary have to configure around.
func TestCertSubjectMatches_Anchoring(t *testing.T) {
	t.Run("unanchored entry admits an extended subject", func(t *testing.T) {
		tenant := generateTestPeerCert(t, "ns-team-a")
		neighbour := generateTestPeerCert(t, "ns-team-a-evil")
		allow := []string{"CN=ns-team-a"}

		assert.True(t, certSubjectMatches([]*x509.Certificate{tenant}, allow))
		assert.True(t, certSubjectMatches([]*x509.Certificate{neighbour}, allow),
			"documented hazard: an extended subject also matches")
	})

	t.Run("anchoring on the next RDN's delimiter excludes an extended subject", func(t *testing.T) {
		tenant := generateTestPeerCertWithOrg(t, "ns-team-a", "supabase")
		neighbour := generateTestPeerCertWithOrg(t, "ns-team-a-evil", "supabase")
		allow := []string{"CN=ns-team-a,"}

		assert.True(t, certSubjectMatches([]*x509.Certificate{tenant}, allow))
		assert.False(t, certSubjectMatches([]*x509.Certificate{neighbour}, allow),
			"anchored entry must exclude an extended subject")
	})

	t.Run("a comma in the subject cannot forge an RDN boundary", func(t *testing.T) {
		// pkix escapes commas inside attribute values, so a caller cannot
		// smuggle "CN=ns-team-a," out of its own CN.
		forger := generateTestPeerCertWithOrg(t, "ns-team-a,O=supabase", "evil")
		allow := []string{"CN=ns-team-a,"}

		assert.NotContains(t, forger.Subject.String(), "CN=ns-team-a,O=supabase,")
		assert.False(t, certSubjectMatches([]*x509.Certificate{forger}, allow),
			"escaped comma must not satisfy an anchored entry")
	})

	t.Run("a bare CN cannot be anchored at all", func(t *testing.T) {
		tenant := generateTestPeerCert(t, "ns-team-a")
		allow := []string{"CN=ns-team-a,"}

		assert.False(t, certSubjectMatches([]*x509.Certificate{tenant}, allow),
			"no trailing delimiter exists to anchor against, so the legitimate holder is rejected too")
	})
}

// mustParseSubjects parses an allow-list that the test asserts is well-formed.
func mustParseSubjects(t *testing.T, entries ...string) []certSubject {
	t.Helper()
	subjects, err := parseCertSubjects(entries)
	require.NoError(t, err)
	return subjects
}

// TestCertSubjectEquals_RejectsEmbeddedEntry covers a subject that carries an
// allow-list entry verbatim inside its own CN. pkix escapes commas inside
// attribute values but not "=", so such a subject satisfies a substring test
// against the rendered DN and must not satisfy an exact one.
func TestCertSubjectEquals_RejectsEmbeddedEntry(t *testing.T) {
	victim := generateTestPeerCertWithOrg(t, "ns-team-a", "acme")
	attacker := generateTestPeerCertWithOrg(t, "evilCN=ns-team-a", "acme")
	allowed := mustParseSubjects(t, "CN=ns-team-a,O=acme")

	// The rendered DNs are why the substring test cannot tell these apart.
	require.Equal(t, "CN=ns-team-a,O=acme", victim.Subject.String())
	require.Equal(t, "CN=evilCN=ns-team-a,O=acme", attacker.Subject.String())
	require.True(t, certSubjectMatches([]*x509.Certificate{attacker}, []string{"CN=ns-team-a,O=acme"}),
		"documents why gRPC's substring match is unsafe as a trust boundary")

	assert.True(t, certSubjectEquals([]*x509.Certificate{victim}, allowed))
	assert.False(t, certSubjectEquals([]*x509.Certificate{attacker}, allowed),
		"a subject embedding the entry inside its own CN must be rejected")
}

// TestCertSubjectEquals_RejectsOtherTenant covers a neighbouring tenant holding
// a perfectly valid certificate from the same CA.
func TestCertSubjectEquals_RejectsOtherTenant(t *testing.T) {
	allowed := mustParseSubjects(t, "CN=ns-team-a,O=acme")

	for _, cn := range []string{"ns-team-b", "ns-team-a-evil", "ns-team", ""} {
		other := generateTestPeerCertWithOrg(t, cn, "acme")
		assert.False(t, certSubjectEquals([]*x509.Certificate{other}, allowed),
			"CN %q from the same CA must not be authorized", cn)
	}

	// Same CN, different org.
	assert.False(t, certSubjectEquals(
		[]*x509.Certificate{generateTestPeerCertWithOrg(t, "ns-team-a", "other-org")}, allowed))
}

func TestCertSubjectEquals_Matching(t *testing.T) {
	cert := generateTestPeerCertWithOrg(t, "ns-team-a", "acme")

	t.Run("CN-only entry ignores unlisted attributes", func(t *testing.T) {
		assert.True(t, certSubjectEquals([]*x509.Certificate{cert}, mustParseSubjects(t, "CN=ns-team-a")))
	})
	t.Run("any entry in the list may match", func(t *testing.T) {
		assert.True(t, certSubjectEquals([]*x509.Certificate{cert},
			mustParseSubjects(t, "CN=someone-else", "CN=ns-team-a,O=acme")))
	})
	t.Run("value containing = is compared literally", func(t *testing.T) {
		odd := generateTestPeerCert(t, "a=b")
		assert.True(t, certSubjectEquals([]*x509.Certificate{odd}, mustParseSubjects(t, "CN=a=b")))
		assert.False(t, certSubjectEquals([]*x509.Certificate{odd}, mustParseSubjects(t, "CN=a")))
	})
}

func TestParseCertSubjects(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		wantErr string
	}{
		{name: "no entries", entries: nil, wantErr: "at least one subject"},
		{name: "empty entry", entries: []string{"CN=a", ""}, wantErr: "empty entries"},
		{name: "whitespace entry", entries: []string{"  "}, wantErr: "empty entries"},
		{name: "attribute without value", entries: []string{"CN="}, wantErr: "empty value"},
		{name: "bare attribute", entries: []string{"CN"}, wantErr: "incomplete type"},
		{name: "separator only", entries: []string{","}, wantErr: "incomplete type"},
		{name: "attribute repeating a value", entries: []string{"OU=a,OU=a"}, wantErr: "repeats the value"},
		{name: "valid repeated attribute", entries: []string{"OU=eng,OU=platform"}},
		{name: "valid single", entries: []string{"CN=a"}},
		{name: "valid multi-attribute", entries: []string{"CN=a,O=b,OU=c"}},
		{name: "valid multi-entry", entries: []string{"CN=a", "CN=b,O=c"}},
		// Any attribute name a subject can carry is legal in an entry,
		// including OID form for ones with no short name.
		{name: "valid oid attribute", entries: []string{"1.2.3.4=custom"}},
		{name: "valid uncommon attribute", entries: []string{"STREET=1 Main St"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCertSubjects(tt.entries)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, got)
		})
	}

	t.Run("attribute names are case-insensitive", func(t *testing.T) {
		got, err := parseCertSubjects([]string{"cn=a,o=b"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]string{"CN": {"a"}, "O": {"b"}}, got[0].attrs)
	})
}

// TestParseCertSubjects_RoundTripsRenderedDNs pins the contract between
// rendering and parsing: whatever pkix.Name.String() emits for a certificate
// must parse back into attributes matching that same certificate. Values
// carrying the separators pkix escapes are where that is easiest to get
// wrong.
func TestParseCertSubjects_RoundTripsRenderedDNs(t *testing.T) {
	for _, cn := range []string{
		"ns-team-a",
		"Doe, John",   // comma, escaped by pkix as \,
		"a+b",         // RDN separator
		"weird=value", // "=" inside a value
		`back\slash`,  // escaped backslash
		"  padded  ",  // leading/trailing spaces, escaped by pkix
	} {
		t.Run(cn, func(t *testing.T) {
			cert := generateTestPeerCertWithOrg(t, cn, "acme")
			rendered := cert.Subject.String()

			subjects, err := parseCertSubjects([]string{rendered})
			require.NoError(t, err, "rendered DN %q must parse back", rendered)
			assert.Equal(t, []string{cn}, subjects[0].attrs["CN"],
				"CN must survive render -> parse unchanged (rendered as %q)", rendered)
			assert.True(t, certSubjectEquals([]*x509.Certificate{cert}, subjects),
				"a certificate must match an allow-list entry built from its own rendered subject")

			// And it still refuses a different tenant.
			other := generateTestPeerCertWithOrg(t, cn+"-evil", "acme")
			assert.False(t, certSubjectEquals([]*x509.Certificate{other}, subjects))
		})
	}
}

// TestCertSubjectEquals_AnyAttribute covers attributes pkix has no typed field
// for. They render in OID form and must be nameable in an allow-list like any
// other, so a certificate is not unallowlistable because of what it carries.
func TestCertSubjectEquals_AnyAttribute(t *testing.T) {
	cert := generateTestPeerCertFull(t, pkix.Name{
		CommonName:    "ns-a",
		Organization:  []string{"acme"},
		StreetAddress: []string{"1 Main St"},
		PostalCode:    []string{"94107"},
		ExtraNames: []pkix.AttributeTypeAndValue{
			{Type: asn1.ObjectIdentifier{1, 2, 3, 4}, Value: "custom"},
		},
	})

	for _, entry := range []string{
		"CN=ns-a",
		"STREET=1 Main St",
		"POSTALCODE=94107",
		"1.2.3.4=custom",
		"CN=ns-a,STREET=1 Main St,1.2.3.4=custom",
	} {
		t.Run(entry, func(t *testing.T) {
			assert.True(t, certSubjectEquals([]*x509.Certificate{cert}, mustParseSubjects(t, entry)))
		})
	}

	t.Run("an attribute the subject lacks never matches", func(t *testing.T) {
		assert.False(t, certSubjectEquals([]*x509.Certificate{cert}, mustParseSubjects(t, "CN=ns-a,OU=absent")))
	})
	t.Run("a wrong value never matches", func(t *testing.T) {
		assert.False(t, certSubjectEquals([]*x509.Certificate{cert}, mustParseSubjects(t, "1.2.3.4=other")))
	})
}

// TestCertSubjectEquals_RepeatedAttribute covers a subject carrying the same
// attribute more than once; naming any one of its values is enough.
func TestCertSubjectEquals_RepeatedAttribute(t *testing.T) {
	cert := generateTestPeerCertFull(t, pkix.Name{
		CommonName:   "ns-a",
		Organization: []string{"acme", "other"},
	})
	assert.True(t, certSubjectEquals([]*x509.Certificate{cert}, mustParseSubjects(t, "O=acme")))
	assert.True(t, certSubjectEquals([]*x509.Certificate{cert}, mustParseSubjects(t, "O=other")))
	assert.False(t, certSubjectEquals([]*x509.Certificate{cert}, mustParseSubjects(t, "O=third")))
}

// generateTestPeerCertFull builds a self-signed certificate with an arbitrary
// subject, for cases the CN/org helpers cannot express.
func generateTestPeerCertFull(t *testing.T, subject pkix.Name) *x509.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}

// TestCertSubjectEquals_RepeatedInEntry covers an entry naming an attribute
// more than once, which is how an identity spanning several values of one
// attribute is written out in full.
func TestCertSubjectEquals_RepeatedInEntry(t *testing.T) {
	cert := generateTestPeerCertFull(t, pkix.Name{
		CommonName:         "svc",
		OrganizationalUnit: []string{"eng", "platform"},
	})

	t.Run("all named values must be present", func(t *testing.T) {
		assert.True(t, certSubjectEquals([]*x509.Certificate{cert},
			mustParseSubjects(t, "CN=svc,OU=eng,OU=platform")))
	})

	t.Run("a value the subject lacks fails the entry", func(t *testing.T) {
		assert.False(t, certSubjectEquals([]*x509.Certificate{cert},
			mustParseSubjects(t, "CN=svc,OU=eng,OU=absent")))
	})

	// Naming more values narrows what qualifies, but matching stays a subset
	// test: a subject carrying every named value and another besides still
	// matches. Excluding it would need an exact-set comparison.
	t.Run("values the entry does not name are unconstrained", func(t *testing.T) {
		lacksOne := generateTestPeerCertFull(t, pkix.Name{
			CommonName:         "svc",
			OrganizationalUnit: []string{"eng", "extra"},
		})
		carriesMore := generateTestPeerCertFull(t, pkix.Name{
			CommonName:         "svc",
			OrganizationalUnit: []string{"eng", "platform", "extra"},
		})
		full := mustParseSubjects(t, "CN=svc,OU=eng,OU=platform")

		assert.False(t, certSubjectEquals([]*x509.Certificate{lacksOne}, full),
			"a named value the subject lacks fails the entry")
		assert.True(t, certSubjectEquals([]*x509.Certificate{carriesMore}, full),
			"an unnamed extra value does not fail the entry")
	})

	t.Run("order within the entry does not matter", func(t *testing.T) {
		assert.True(t, certSubjectEquals([]*x509.Certificate{cert},
			mustParseSubjects(t, "OU=platform,OU=eng,CN=svc")))
	})
}
