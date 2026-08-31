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
