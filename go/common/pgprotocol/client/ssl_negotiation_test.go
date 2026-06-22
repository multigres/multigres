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

// Tests for the libpq sslnegotiation parameter mirror (PostgreSQL 17
// direct SSL). The option-compatibility cases come straight from libpq's
// fe-connect.c validation ("weak sslmode ... may not be used with
// sslnegotiation=direct") and the ALPN requirement from
// fe-secure-openssl.c ("direct SSL connection was established without
// ALPN protocol negotiation extension").

package client

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

func TestParseSSLNegotiation(t *testing.T) {
	tests := []struct {
		input    string
		expected SSLNegotiation
		wantErr  bool
	}{
		{"", SSLNegotiationPostgres, false},
		{"postgres", SSLNegotiationPostgres, false},
		{"POSTGRES", SSLNegotiationPostgres, false},
		{" direct ", SSLNegotiationDirect, false},
		{"direct", SSLNegotiationDirect, false},
		{"Direct", SSLNegotiationDirect, false},
		{"requiredirect", "", true}, // dropped from PG 17 before release
		{"on", "", true},
		{"tls", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseSSLNegotiation(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid sslnegotiation")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestValidateSSLNegotiation mirrors libpq's option compatibility matrix:
// direct may only pair with a TLS-enforcing sslmode.
func TestValidateSSLNegotiation(t *testing.T) {
	// postgres (and unset) pair with everything.
	for _, mode := range []SSLMode{SSLModeDisable, SSLModePrefer, SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull} {
		require.NoError(t, ValidateSSLNegotiation("", mode))
		require.NoError(t, ValidateSSLNegotiation(SSLNegotiationPostgres, mode))
	}

	// direct requires require/verify-ca/verify-full.
	for _, mode := range []SSLMode{SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull} {
		require.NoError(t, ValidateSSLNegotiation(SSLNegotiationDirect, mode))
	}
	for _, mode := range []SSLMode{SSLModeDisable, SSLModePrefer, SSLMode("")} {
		err := ValidateSSLNegotiation(SSLNegotiationDirect, mode)
		require.Error(t, err, "mode %q must be rejected with direct", mode)
		assert.Contains(t, err.Error(), "may not be used with sslnegotiation=direct")
	}

	// Unknown negotiation values are rejected.
	require.Error(t, ValidateSSLNegotiation("bogus", SSLModeRequire))
}

// TestDirectTLS_WeakSSLModeFailsBeforeDial: startup must fail fast on the
// libpq compatibility rule without writing anything to the server.
func TestDirectTLS_WeakSSLModeFailsBeforeDial(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Conn{config: &Config{
		SSLMode:        SSLModePrefer,
		SSLNegotiation: SSLNegotiationDirect,
	}}
	c.resetConn(clientConn)

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errCh <- c.startup(ctx)
	}()

	err := <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "may not be used with sslnegotiation=direct")

	// Nothing must have been written to the wire.
	require.NoError(t, serverConn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	buf := make([]byte, 1)
	n, _ := serverConn.Read(buf)
	assert.Equal(t, 0, n, "weak-sslmode failure must not write to the connection")
}

// TestDirectTLS_NilTLSConfigRejected: direct mode with no TLS config is a
// configuration error, reported before any handshake bytes go out.
func TestDirectTLS_NilTLSConfigRejected(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Conn{config: &Config{
		Host:           "127.0.0.1",
		SSLMode:        SSLModeRequire,
		SSLNegotiation: SSLNegotiationDirect,
		TLSConfig:      nil,
	}}
	c.resetConn(clientConn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.startup(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS config is nil")
}

// generateClientTestServerTLS builds an ephemeral self-signed server TLS
// config for the raw TLS listeners below.
func generateClientTestServerTLS(t *testing.T, nextProtos []string) *tls.Config {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)

	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   nextProtos,
	}
}

// TestDirectTLS_ALPNRequired: a server that completes the handshake without
// selecting the "postgresql" ALPN protocol must be rejected with libpq's
// diagnostic. Such a server is either not PostgreSQL or predates direct
// TLS — proceeding would defeat ALPN's cross-protocol hardening.
func TestDirectTLS_ALPNRequired(t *testing.T) {
	// Raw TLS listener with NO ALPN protocols: Go ignores the client's
	// offer and negotiates none.
	srvCfg := generateClientTestServerTLS(t, nil)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		// Drive the handshake; the client should drop the connection
		// right after when it notices the missing ALPN.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
		conn.Close()
	}()

	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)

	clientTLS, err := BuildTLSConfig(SSLModeRequire, "", "")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = Connect(ctx, ctx, &Config{
		Host:           "127.0.0.1",
		Port:           addr.Port,
		User:           "u",
		Password:       "p",
		Database:       "d",
		SSLMode:        SSLModeRequire,
		SSLNegotiation: SSLNegotiationDirect,
		TLSConfig:      clientTLS,
		DialTimeout:    5 * time.Second,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"direct SSL connection was established without ALPN protocol negotiation extension")
}

// TestDirectTLS_OffersALPN verifies the wire behavior: the client's first
// bytes are a TLS ClientHello (no SSLRequest), and the handshake offers
// ALPN "postgresql".
func TestDirectTLS_OffersALPN(t *testing.T) {
	srvCfg := generateClientTestServerTLS(t, []string{protocol.ALPNProtocol})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	alpnCh := make(chan string, 1)
	go func() {
		raw, acceptErr := ln.Accept()
		if acceptErr != nil {
			alpnCh <- "accept-error"
			return
		}
		tlsConn := tls.Server(raw, srvCfg)
		if hsErr := tlsConn.Handshake(); hsErr != nil {
			alpnCh <- "handshake-error"
			return
		}
		alpnCh <- tlsConn.ConnectionState().NegotiatedProtocol
		// Hold the conn open until the client gives up on startup; the
		// raw server never answers the startup message.
		buf := make([]byte, 64)
		_, _ = tlsConn.Read(buf)
		tlsConn.Close()
	}()

	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)

	clientTLS, err := BuildTLSConfig(SSLModeRequire, "", "")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Connect will fail (the fake server never completes startup), but the
	// handshake portion is what this test observes.
	_, _ = Connect(ctx, ctx, &Config{
		Host:           "127.0.0.1",
		Port:           addr.Port,
		User:           "u",
		Password:       "p",
		Database:       "d",
		SSLMode:        SSLModeRequire,
		SSLNegotiation: SSLNegotiationDirect,
		TLSConfig:      clientTLS,
		DialTimeout:    2 * time.Second,
	})

	assert.Equal(t, protocol.ALPNProtocol, <-alpnCh,
		"client must offer (and server select) the postgresql ALPN protocol")
}

// TestDirectTLS_SharedTLSConfigNotMutated: adding the ALPN offer must not
// mutate a TLSConfig shared across dials (pool managers reuse one config
// for every connection).
func TestDirectTLS_SharedTLSConfigNotMutated(t *testing.T) {
	shared, err := BuildTLSConfig(SSLModeRequire, "", "")
	require.NoError(t, err)
	require.Empty(t, shared.NextProtos)

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	c := &Conn{config: &Config{
		Host:           "127.0.0.1",
		SSLMode:        SSLModeRequire,
		SSLNegotiation: SSLNegotiationDirect,
		TLSConfig:      shared,
	}}
	c.resetConn(clientConn)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() {
		// Sink whatever the client handshake writes so it doesn't block
		// on the pipe; the context timeout ends the attempt.
		buf := make([]byte, 4096)
		for {
			if _, readErr := serverConn.Read(buf); readErr != nil {
				return
			}
		}
	}()
	_ = c.startup(ctx)

	assert.Empty(t, shared.NextProtos, "shared TLS config must not be mutated by directTLS")
}
