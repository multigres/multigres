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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestMtlsAuthPluginInitializer(t *testing.T) {
	tests := []struct {
		name       string
		substrings string
		wantErr    bool
	}{
		{name: "empty allow-list rejected", substrings: "", wantErr: true},
		{name: "empty token rejected", substrings: "client-a::client-b", wantErr: true},
		{name: "trailing empty token rejected", substrings: "client-a:", wantErr: true},
		{name: "valid single entry", substrings: "client-a", wantErr: false},
		{name: "valid multiple entries", substrings: "client-a:client-b", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := clientCertSubstrings
			t.Cleanup(func() { clientCertSubstrings = orig })
			clientCertSubstrings = tt.substrings

			auth, err := mtlsAuthPluginInitializer()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "grpc-auth-mtls-allowed-substrings")
			} else {
				require.NoError(t, err)
				require.NotNil(t, auth)
			}
		})
	}
}

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
			got, err := ParseCertSubstrings(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "grpc-auth-mtls-allowed-substrings")
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

func fakeGRPCContext(cert *x509.Certificate, bearerToken string) context.Context {
	ctx := context.Background()
	if cert != nil {
		ctx = peer.NewContext(ctx, &peer.Peer{
			AuthInfo: credentials.TLSInfo{
				State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}},
			},
		})
	}
	if bearerToken != "" {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+bearerToken))
	}
	return ctx
}
