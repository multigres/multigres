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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// generateTestPeerCert builds a self-signed certificate with the given
// Subject CommonName, entirely in-memory (no disk I/O). MtlsAuthPlugin's own
// Authenticate only inspects cert.Subject - it never validates the chain -
// since chain validation already happened during the real TLS handshake
// (tls.RequireAndVerifyClientCert) before the connection was ever accepted,
// so a self-signed cert is sufficient here.
func generateTestPeerCert(t *testing.T, cn string) *x509.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}

// fakeGRPCContext builds a context carrying peer certificate info (if cert
// is non-nil) and an authorization metadata header (if bearerToken is
// non-empty), combined the way a real incoming gRPC call would carry both -
// peer.FromContext and metadata.FromIncomingContext use independent context
// keys, so both can be present simultaneously.
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

func TestMTLSOrJWTAuthPluginInitializer(t *testing.T) {
	origSubstrings := clientCertSubstrings
	origIssuer, origJWKSURI := jwtIssuer, jwtJWKSURI
	t.Cleanup(func() {
		clientCertSubstrings = origSubstrings
		jwtIssuer, jwtJWKSURI = origIssuer, origJWKSURI
	})

	jwks := newTestJWKSet(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks.jwksJSON)
	}))
	t.Cleanup(srv.Close)

	t.Run("missing mtls substrings rejected", func(t *testing.T) {
		clientCertSubstrings = ""
		jwtIssuer, jwtJWKSURI = testJWTIssuer, srv.URL

		_, err := mtlsOrJWTAuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mtls-or-jwt")
		assert.Contains(t, err.Error(), "grpc-auth-mtls-allowed-substrings")
	})

	t.Run("missing jwt issuer rejected", func(t *testing.T) {
		clientCertSubstrings = "operator"
		jwtIssuer, jwtJWKSURI = "", srv.URL

		_, err := mtlsOrJWTAuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mtls-or-jwt")
		assert.Contains(t, err.Error(), "grpc-auth-jwt-issuer")
	})

	t.Run("valid config succeeds", func(t *testing.T) {
		clientCertSubstrings = "operator"
		jwtIssuer, jwtJWKSURI = testJWTIssuer, srv.URL

		auth, err := mtlsOrJWTAuthPluginInitializer()
		require.NoError(t, err)
		require.NotNil(t, auth)

		_, ok := auth.(TokenVerifier)
		assert.True(t, ok, "mtls-or-jwt plugin must implement TokenVerifier for HTTP/Connect callers")
	})
}

func TestMTLSOrJWTAuthPlugin_Authenticate(t *testing.T) {
	jwks := newTestJWKSet(t)
	validToken := signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid)

	plugin := &MTLSOrJWTAuthPlugin{
		mtls:        &MtlsAuthPlugin{clientCertSubstrings: []string{"operator"}},
		jwt:         jwks.staticVerifier(t, testJWTIssuer, ""),
		jwtVerifier: jwks.staticVerifier(t, testJWTIssuer, ""),
	}

	tests := []struct {
		name    string
		cert    *x509.Certificate
		token   string
		wantErr bool
	}{
		{
			name: "matching cert, no token: allowed via mtls",
			cert: generateTestPeerCert(t, "operator.internal"),
		},
		{
			name:  "no cert, valid token: allowed via jwt",
			token: validToken,
		},
		{
			name:  "non-matching cert, valid token: falls through to jwt, allowed",
			cert:  generateTestPeerCert(t, "some-other-component"),
			token: validToken,
		},
		{
			name:  "matching cert, invalid token: allowed via mtls without ever needing a valid token",
			cert:  generateTestPeerCert(t, "operator.internal"),
			token: "not-a-valid-jwt",
		},
		{
			name:    "non-matching cert, no token: rejected",
			cert:    generateTestPeerCert(t, "some-other-component"),
			wantErr: true,
		},
		{
			name:    "no cert, no token: rejected",
			wantErr: true,
		},
		{
			name:    "no cert, invalid token: rejected",
			token:   "not-a-valid-jwt",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := fakeGRPCContext(tt.cert, tt.token)
			_, err := plugin.Authenticate(ctx, "/multiadmin.MultiadminService/GetPoolerStatus")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestMTLSOrJWTAuthPlugin_Authenticate_NoSpuriousMtlsMetricOnJWTFallthrough
// guards against a real bug: MTLSOrJWTAuthPlugin.Authenticate used to call
// MtlsAuthPlugin.Authenticate (which logs and records every outcome) for
// its cert check, so every JWT-only caller - which by construction never
// has a matching cert - produced a logged "rejection" and a failure metric
// for a check it was never going to pass, on every single successful
// request. The cert check must be silent on a miss; only a cert success
// (or the JWT fallthrough's own outcome) should ever be observable.
func TestMTLSOrJWTAuthPlugin_Authenticate_NoSpuriousMtlsMetricOnJWTFallthrough(t *testing.T) {
	m, reader := setupAuthMetricsTest(t)
	jwks := newTestJWKSet(t)
	validToken := signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid)

	jwtPlugin := jwks.staticVerifier(t, testJWTIssuer, "")
	jwtPlugin.metrics = m
	plugin := &MTLSOrJWTAuthPlugin{
		mtls:        &MtlsAuthPlugin{clientCertSubstrings: []string{"operator"}},
		jwt:         jwtPlugin,
		jwtVerifier: jwtPlugin,
		metrics:     m,
	}

	// No cert at all - a routine JWT-only caller, not a cert-holder that
	// merely failed to match.
	ctx := fakeGRPCContext(nil, validToken)
	_, err := plugin.Authenticate(ctx, "/multiadmin.MultiadminService/GetPoolerStatus")
	require.NoError(t, err)

	sum := findAuthAttempts(t, reader)
	mtlsCounts := countsByOutcome(t, sum, "mtls")
	assert.Empty(t, mtlsCounts, "a JWT-only success must not record any mtls outcome, spurious rejection or otherwise")

	jwtCounts := countsByOutcome(t, sum, "jwt")
	assert.Equal(t, int64(1), jwtCounts[AuthOutcomeSuccess])
}

func TestMTLSOrJWTAuthPlugin_VerifyToken(t *testing.T) {
	jwks := newTestJWKSet(t)
	verifier := jwks.staticVerifier(t, testJWTIssuer, "")
	plugin := &MTLSOrJWTAuthPlugin{
		mtls:        &MtlsAuthPlugin{clientCertSubstrings: []string{"operator"}},
		jwt:         verifier,
		jwtVerifier: verifier,
	}

	t.Run("valid token accepted", func(t *testing.T) {
		token := signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid)
		_, err := plugin.VerifyToken(token)
		assert.NoError(t, err)
	})

	t.Run("invalid token rejected", func(t *testing.T) {
		_, err := plugin.VerifyToken("not-a-valid-jwt")
		assert.Error(t, err)
	})
}
