// Copyright 2019 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package servenv

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestEmpty(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	if len(interceptors.Build()) > 0 {
		t.Fatalf("expected empty builder to report as empty")
	}
}

func TestSingleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake := &FakeInterceptor{}

	interceptors.Add(fake.StreamServerInterceptor, fake.UnaryServerInterceptor)

	if len(interceptors.streamInterceptors) != 1 {
		t.Fatalf("expected 1 server options to be available")
	}
	if len(interceptors.unaryInterceptors) != 1 {
		t.Fatalf("expected 1 server options to be available")
	}
}

func TestDoubleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake1 := &FakeInterceptor{name: "ettan"}
	fake2 := &FakeInterceptor{name: "tvaon"}

	interceptors.Add(fake1.StreamServerInterceptor, fake1.UnaryServerInterceptor)
	interceptors.Add(fake2.StreamServerInterceptor, fake2.UnaryServerInterceptor)

	if len(interceptors.streamInterceptors) != 2 {
		t.Fatalf("expected 1 server options to be available")
	}
	if len(interceptors.unaryInterceptors) != 2 {
		t.Fatalf("expected 1 server options to be available")
	}
}

type FakeInterceptor struct {
	name       string
	streamSeen any
	unarySeen  any
}

func (fake *FakeInterceptor) StreamServerInterceptor(value any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fake.streamSeen = value
	return handler(value, stream)
}

func (fake *FakeInterceptor) UnaryServerInterceptor(ctx context.Context, value any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	fake.unarySeen = value
	return handler(ctx, value)
}

func newEnabledGRPCServerForTest() *GrpcServer {
	reg := viperutil.NewRegistry()
	g := NewGrpcServer(reg)
	g.port.Set(12345)
	return g
}

// TestGrpcServerKeepaliveDefaults guards the keepalive invariant that long-lived
// streams depend on. The StreamReplication tunnel has no client-side
// reconnect/resume, so a finite MaxConnectionAge (server sends GoAway) or a set
// MaxConnectionIdle (idle reap) would tear an active replication stream down with
// no recovery. These params are global to every servenv gRPC server and nothing
// else enforces the requirement, so this test fails loudly if a future change
// makes them finite — forcing a conscious decision (and the per-workload
// dedicated-listener follow-up) rather than silently breaking replication.
func TestGrpcServerKeepaliveDefaults(t *testing.T) {
	g := NewGrpcServer(viperutil.NewRegistry())
	ka := g.keepaliveServerParameters()

	const unbounded = time.Duration(math.MaxInt64)
	assert.Equal(t, unbounded, ka.MaxConnectionAge,
		"MaxConnectionAge must stay unbounded; long-lived replication streams have no resume")
	assert.Equal(t, unbounded, ka.MaxConnectionAgeGrace,
		"MaxConnectionAgeGrace must stay unbounded")
	assert.Zero(t, ka.MaxConnectionIdle,
		"MaxConnectionIdle must stay unset; an idle replication stream must not be reaped")
}

func TestGrpcServerCreate_SucceedsWithoutTLS(t *testing.T) {
	g := newEnabledGRPCServerForTest()

	err := g.Create()
	require.NoError(t, err)
	require.NotNil(t, g.Server)
}

func TestGrpcServerCreate_FailsWhenCRLSet(t *testing.T) {
	g := newEnabledGRPCServerForTest()
	g.crl.Set("/tmp/test.crl")

	err := g.Create()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--grpc-crl is not implemented yet")
}

func TestGrpcServerCreate_FailsWhenOptionalTLSEnabled(t *testing.T) {
	g := newEnabledGRPCServerForTest()
	g.enableOptionalTLS.Set(true)

	err := g.Create()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--grpc-enable-optional-tls is not implemented yet")
}

func TestGrpcServerCreate_FailsWhenMTLSAuthWithoutTLS(t *testing.T) {
	// clientCertSubstrings must be valid so the mtls plugin itself resolves
	// successfully - resolveAuthPlugin() now runs unconditionally at the top
	// of Create() (see fix for the auth-plugin startup race), before the
	// gRPC-specific "mtls requires TLS" check below it, so this test needs a
	// fully-valid-except-for-TLS config to actually isolate that check.
	origSubstrings := clientCertSubstrings
	t.Cleanup(func() { clientCertSubstrings = origSubstrings })
	clientCertSubstrings = "some-client"

	g := newEnabledGRPCServerForTest()
	g.auth.Set("mtls")

	err := g.Create()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--grpc-auth-mode=mtls requires --grpc-cert and --grpc-key for transport TLS")
}

// TestGrpcServerCreate_SucceedsWithJWTAuth proves --grpc-auth-mode=jwt selects
// and installs the JWT plugin through the exact same shared selection/wiring
// path (GetAuthenticator -> interceptors() -> g.authPluginBox) already trusted
// for mtls above - i.e. the full gRPC server wiring for JWT auth, not just
// the plugin's own logic in isolation (see grpc_server_auth_jwt_test.go for
// that).
func TestGrpcServerCreate_SucceedsWithJWTAuth(t *testing.T) {
	origIssuer, origJWKSURI := jwtIssuer, jwtJWKSURI
	t.Cleanup(func() { jwtIssuer, jwtJWKSURI = origIssuer, origJWKSURI })

	jwks := newTestJWKSet(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks.jwksJSON)
	}))
	t.Cleanup(srv.Close)
	jwtIssuer, jwtJWKSURI = testJWTIssuer, srv.URL

	g := newEnabledGRPCServerForTest()
	g.auth.Set("jwt")

	err := g.Create()
	require.NoError(t, err)
	require.NotNil(t, g.Server)

	verifier, ok := g.AuthPlugin().(TokenVerifier)
	require.True(t, ok, "AuthPlugin() must return a TokenVerifier when --grpc-auth-mode=jwt is selected")

	validToken := signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid)
	_, err = verifier.VerifyToken(validToken)
	assert.NoError(t, err)
}

// TestGrpcServerCreate_ResolvesAuthPluginWhenGRPCDisabled guards against the
// auth plugin silently never resolving for HTTP-only deployments
// (grpc-port=0, no socket-file). Before this fix, Create() returned before
// interceptors()/resolveAuthPlugin() ever ran when IsEnabled() was false,
// so --grpc-auth-mode=jwt looked configured but g.AuthPlugin() stayed nil
// forever - silently leaving anything gated via ServEnv.SetAuthPlugin (e.g.
// MultiAdmin's HTTP routes, pprof) completely unauthenticated with no error.
func TestGrpcServerCreate_ResolvesAuthPluginWhenGRPCDisabled(t *testing.T) {
	origIssuer, origJWKSURI := jwtIssuer, jwtJWKSURI
	t.Cleanup(func() { jwtIssuer, jwtJWKSURI = origIssuer, origJWKSURI })

	jwks := newTestJWKSet(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks.jwksJSON)
	}))
	t.Cleanup(srv.Close)
	jwtIssuer, jwtJWKSURI = testJWTIssuer, srv.URL

	g := NewGrpcServer(viperutil.NewRegistry()) // port defaults to 0, no socket-file: IsEnabled() == false
	g.auth.Set("jwt")
	require.False(t, g.IsEnabled())

	err := g.Create()
	require.NoError(t, err)
	assert.Nil(t, g.Server, "gRPC server itself should not be built when disabled")

	_, ok := g.AuthPlugin().(TokenVerifier)
	require.True(t, ok, "AuthPlugin() must still resolve even when the gRPC listener is disabled")
}

// TestGrpcServerAuthPlugin_PendingBeforeResolve guards the fail-closed
// behavior run.go now relies on: the HTTP-serving goroutine starts before
// Create()/resolveAuthPlugin runs (so a slow or unreachable JWKS endpoint
// can't block K8s startup/liveness probes), which means a request can arrive
// while an auth mode is configured but not yet resolved. AuthPlugin must not
// return nil in that window - nil means "no auth, pass everything through"
// everywhere it's checked - or every HTTP/Connect/REST/pprof request racing
// startup would sail through unauthenticated.
func TestGrpcServerAuthPlugin_PendingBeforeResolve(t *testing.T) {
	g := NewGrpcServer(viperutil.NewRegistry())
	g.auth.Set("jwt") // configured, but resolveAuthPlugin deliberately never called

	plugin := g.AuthPlugin()
	require.NotNil(t, plugin)

	_, err := plugin.Authenticate(context.Background(), "/some/Method")
	assert.Error(t, err, "must fail closed while auth resolution is pending")

	verifier, ok := plugin.(TokenVerifier)
	require.True(t, ok, "pending plugin must implement TokenVerifier so HTTP/Connect also fail closed")
	_, err = verifier.VerifyToken("anything")
	assert.Error(t, err, "must fail closed while auth resolution is pending")
}

// TestGrpcServerAuthPlugin_NilWhenAuthDisabled proves the "no auth mode
// configured" case is unaffected by the pending-state handling above: it
// must still mean nil (pass-through), even before Create() ever runs.
func TestGrpcServerAuthPlugin_NilWhenAuthDisabled(t *testing.T) {
	g := NewGrpcServer(viperutil.NewRegistry())
	assert.Nil(t, g.AuthPlugin())
}

// generateTestCAAndCert creates a self-signed CA plus a cert/key signed by
// it, written to files in a temp dir - grpccommon.BuildServerTLSConfig (used
// by Create()) loads certs from file paths, unlike the in-memory certs used
// in grpc_server_auth_mtls_or_jwt_test.go's direct Authenticate() tests.
// Mirrors go/tools/grpccommon/tls_test.go's generateTestCA/generateTestCert
// shape, per this repo's convention of per-package test cert helpers rather
// than a shared testutil.
func generateTestCAAndCert(t *testing.T, cn string) (caCertPath, certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTmpl, &caTmpl, caKey.Public(), caKey)
	require.NoError(t, err)
	caCertPath = filepath.Join(dir, "ca.crt")
	writeTestPEM(t, caCertPath, "CERTIFICATE", caDER)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, caCert, key.Public(), caKey)
	require.NoError(t, err)
	certPath = filepath.Join(dir, "cert.crt")
	writeTestPEM(t, certPath, "CERTIFICATE", certDER)
	keyPath = filepath.Join(dir, "cert.key")
	writeTestPEM(t, keyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))

	return caCertPath, certPath, keyPath
}

func writeTestPEM(t *testing.T, path, blockType string, data []byte) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, pem.Encode(f, &pem.Block{Type: blockType, Bytes: data}))
}

// TestGrpcServerCreate_FailsWhenMTLSOrJWTWithoutTLS extends the existing
// mtls-requires-TLS guard to the new composite mode - without this, the
// mode would silently degrade to JWT-only instead of failing loudly, since
// the certificate check can never succeed without a TLS handshake to
// inspect.
func TestGrpcServerCreate_FailsWhenMTLSOrJWTWithoutTLS(t *testing.T) {
	// Both sub-plugins' config must be valid so the composite plugin itself
	// resolves successfully - otherwise resolveAuthPlugin() (which now runs
	// unconditionally at the top of Create()) fails first on the missing
	// jwt config, never reaching the TLS-requirement check this test means
	// to isolate. Same reasoning as TestGrpcServerCreate_FailsWhenMTLSAuthWithoutTLS.
	origSubstrings := clientCertSubstrings
	origIssuer, origJWKSURI := jwtIssuer, jwtJWKSURI
	t.Cleanup(func() {
		clientCertSubstrings = origSubstrings
		jwtIssuer, jwtJWKSURI = origIssuer, origJWKSURI
	})
	clientCertSubstrings = "operator"

	jwks := newTestJWKSet(t)
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks.jwksJSON)
	}))
	t.Cleanup(jwksSrv.Close)
	jwtIssuer, jwtJWKSURI = testJWTIssuer, jwksSrv.URL

	g := newEnabledGRPCServerForTest()
	g.auth.Set("mtls-or-jwt")

	err := g.Create()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--grpc-auth-mode=mtls-or-jwt requires --grpc-cert and --grpc-key for transport TLS")
}

// TestGrpcServerCreate_SucceedsWithMTLSOrJWTAuth proves the full wiring for
// --grpc-auth-mode=mtls-or-jwt: real transport TLS configured via
// --grpc-cert/--grpc-key/--grpc-ca, the composite plugin resolves, and it
// authenticates a matching client certificate without ever needing a JWT.
func TestGrpcServerCreate_SucceedsWithMTLSOrJWTAuth(t *testing.T) {
	origSubstrings := clientCertSubstrings
	origIssuer, origJWKSURI := jwtIssuer, jwtJWKSURI
	t.Cleanup(func() {
		clientCertSubstrings = origSubstrings
		jwtIssuer, jwtJWKSURI = origIssuer, origJWKSURI
	})

	jwks := newTestJWKSet(t)
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks.jwksJSON)
	}))
	t.Cleanup(jwksSrv.Close)
	jwtIssuer, jwtJWKSURI = testJWTIssuer, jwksSrv.URL
	clientCertSubstrings = "operator"

	caCertPath, certPath, keyPath := generateTestCAAndCert(t, "operator.internal")

	g := newEnabledGRPCServerForTest()
	g.auth.Set("mtls-or-jwt")
	g.cert.Set(certPath)
	g.key.Set(keyPath)
	g.ca.Set(caCertPath)

	err := g.Create()
	require.NoError(t, err)
	require.NotNil(t, g.Server)

	plugin := g.AuthPlugin()
	require.NotNil(t, plugin)
	_, ok := plugin.(TokenVerifier)
	assert.True(t, ok, "composite plugin must still support the HTTP/Connect JWT path")

	matchingCert := generateTestPeerCert(t, "operator.internal")
	ctx := fakeGRPCContext(matchingCert, "")
	_, err = plugin.Authenticate(ctx, "/multiadmin.MultiadminService/GetPoolerStatus")
	assert.NoError(t, err, "a matching client certificate must authenticate without any JWT present")
}
