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

package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// generateTestTLSConfig creates an ephemeral self-signed TLS certificate for testing.
// Returns a server TLS config and a CA cert pool for client verification.
func generateTestTLSConfig(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()

	// Generate CA key and certificate
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)

	// Generate server key and certificate signed by CA
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(1 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:    []string{"localhost"},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, caCert, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	serverCert := tls.Certificate{
		Certificate: [][]byte{serverCertDER},
		PrivateKey:  serverKey,
	}

	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	}

	// Create CA cert pool for client verification
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	return serverConfig, caPool
}

// writeSSLRequest writes an SSLRequest startup packet to a writer.
func writeSSLRequest(t *testing.T, w io.Writer) {
	t.Helper()
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(8))
	_ = binary.Write(&buf, binary.BigEndian, uint32(protocol.SSLRequestCode))
	_, err := w.Write(buf.Bytes())
	require.NoError(t, err)
}

// writeGSSENCRequest writes a GSSENCRequest startup packet to a writer.
func writeGSSENCRequest(t *testing.T, w io.Writer) {
	t.Helper()
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(8))
	_ = binary.Write(&buf, binary.BigEndian, uint32(protocol.GSSENCRequestCode))
	_, err := w.Write(buf.Bytes())
	require.NoError(t, err)
}

// readSingleByte reads a single byte from a reader.
func readSingleByte(t *testing.T, r io.Reader) byte {
	t.Helper()
	b := make([]byte, 1)
	_, err := io.ReadFull(r, b)
	require.NoError(t, err)
	return b[0]
}

// newTestConn creates a Conn suitable for testing with the given TLS config.
func newTestConn(t *testing.T, serverConn net.Conn, tlsConfig *tls.Config) *Conn {
	t.Helper()
	listener := testListener(t)
	c := &Conn{
		conn:           serverConn,
		listener:       listener,
		handler:        listener.handler,
		hashProvider:   listener.hashProvider,
		bufferedReader: bufio.NewReader(serverConn),
		params:         make(map[string]string),
		txnStatus:      protocol.TxnStatusIdle,
		tlsConfig:      tlsConfig,
	}
	c.ctx = context.Background()
	c.logger = testLogger(t)
	return c
}

func TestSSLRequest_Declined_NoTLSConfig(t *testing.T) {
	// When no TLS config is set, SSLRequest should be declined with 'N',
	// and the connection should continue to accept a StartupMessage.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	c := newTestConn(t, serverConn, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Send SSLRequest
	writeSSLRequest(t, clientConn)

	// Read 'N' response
	resp := readSingleByte(t, clientConn)
	assert.Equal(t, byte('N'), resp, "should decline SSL with 'N'")

	// Send startup message and authenticate
	params := map[string]string{"user": "testuser", "database": "testdb"}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)
	scramClientHelper(t, clientConn, "testuser", "postgres")

	err := <-errCh
	require.NoError(t, err)
	assert.Equal(t, "testuser", c.user)
	assert.Equal(t, "testdb", c.database)
}

func TestSSLRequest_Accepted_WithTLSConfig(t *testing.T) {
	// When TLS config is set, SSLRequest should be accepted with 'S',
	// TLS handshake should succeed, and connection should work over TLS.

	tlsConfig, caPool := generateTestTLSConfig(t)

	// Use real TCP connections for TLS (pipes don't support TLS handshake)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		netConn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}

		c := newTestConn(t, netConn, tlsConfig)
		errCh <- c.handleStartup()
	}()

	// Client side: connect to the server
	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()

	// Send SSLRequest
	writeSSLRequest(t, clientConn)

	// Read 'S' response
	resp := readSingleByte(t, clientConn)
	require.Equal(t, byte('S'), resp, "should accept SSL with 'S'")

	// Perform TLS handshake
	tlsClientConn := tls.Client(clientConn, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	err = tlsClientConn.Handshake()
	require.NoError(t, err)

	// Send startup message over TLS and authenticate
	writeStartupPacketToPipe(t, tlsClientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "tlsuser",
		"database": "tlsdb",
	})
	scramClientHelper(t, tlsClientConn, "tlsuser", "postgres")

	err = <-errCh
	require.NoError(t, err)
}

func TestSSLRequest_NonSSLClient_WithTLSConfigured(t *testing.T) {
	// When TLS is configured but client doesn't send SSLRequest,
	// the connection should still work (StartupMessage directly).
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	tlsConfig, _ := generateTestTLSConfig(t)
	c := newTestConn(t, serverConn, tlsConfig)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Send startup message directly (no SSLRequest)
	params := map[string]string{"user": "plainuser", "database": "plaindb"}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)
	scramClientHelper(t, clientConn, "plainuser", "postgres")

	err := <-errCh
	require.NoError(t, err)
	assert.Equal(t, "plainuser", c.user)
	assert.Equal(t, "plaindb", c.database)
}

func TestSSLRequest_TLSHandshakeFailure(t *testing.T) {
	// When the TLS handshake fails, the server should return an error.

	tlsConfig, _ := generateTestTLSConfig(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		netConn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}

		c := newTestConn(t, netConn, tlsConfig)
		errCh <- c.handleStartup()
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()

	// Send SSLRequest
	writeSSLRequest(t, clientConn)

	// Read 'S' response
	resp := readSingleByte(t, clientConn)
	require.Equal(t, byte('S'), resp)

	// Send garbage instead of a proper TLS ClientHello
	_, _ = clientConn.Write([]byte("not a tls handshake"))
	clientConn.Close()

	// Server should get a TLS handshake error
	err = <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS handshake failed")
}

func TestGSSENCRequest_ThenSSLRequest_Fallback(t *testing.T) {
	// Per PostgreSQL protocol, a client may try GSSENCRequest first (gets 'N'),
	// then SSLRequest. This should work correctly.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	// No TLS configured — both should be declined
	c := newTestConn(t, serverConn, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Send GSSENCRequest
	writeGSSENCRequest(t, clientConn)

	// Read 'N' response
	resp := readSingleByte(t, clientConn)
	assert.Equal(t, byte('N'), resp)

	// Send SSLRequest (fallback)
	writeSSLRequest(t, clientConn)

	// Read 'N' response
	resp = readSingleByte(t, clientConn)
	assert.Equal(t, byte('N'), resp)

	// Send startup message and authenticate
	params := map[string]string{"user": "fallback_user", "database": "fallback_db"}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)
	scramClientHelper(t, clientConn, "fallback_user", "postgres")

	err := <-errCh
	require.NoError(t, err)
	assert.Equal(t, "fallback_user", c.user)
}

func TestSSLRequest_ThenGSSENCRequest_Fallback(t *testing.T) {
	// Reverse fallback: SSLRequest first (declined), then GSSENCRequest.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	c := newTestConn(t, serverConn, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Send SSLRequest
	writeSSLRequest(t, clientConn)

	// Read 'N' response
	resp := readSingleByte(t, clientConn)
	assert.Equal(t, byte('N'), resp)

	// Send GSSENCRequest (fallback)
	writeGSSENCRequest(t, clientConn)

	// Read 'N' response
	resp = readSingleByte(t, clientConn)
	assert.Equal(t, byte('N'), resp)

	// Send startup message and authenticate
	params := map[string]string{"user": "reverse_user", "database": "reverse_db"}
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, params)
	scramClientHelper(t, clientConn, "reverse_user", "postgres")

	err := <-errCh
	require.NoError(t, err)
	assert.Equal(t, "reverse_user", c.user)
}

func TestSSLRequest_DuplicateRejected(t *testing.T) {
	// Sending SSLRequest twice should be rejected.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	c := newTestConn(t, serverConn, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Send first SSLRequest
	writeSSLRequest(t, clientConn)

	// Read 'N' response
	resp := readSingleByte(t, clientConn)
	assert.Equal(t, byte('N'), resp)

	// Send second SSLRequest — should be rejected
	writeSSLRequest(t, clientConn)

	// Server should return an error about duplicate SSLRequest
	err := <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate SSLRequest")
}

func TestGSSENCRequest_DuplicateRejected(t *testing.T) {
	// Sending GSSENCRequest twice should be rejected.
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	c := newTestConn(t, serverConn, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	// Send first GSSENCRequest
	writeGSSENCRequest(t, clientConn)

	// Read 'N' response
	resp := readSingleByte(t, clientConn)
	assert.Equal(t, byte('N'), resp)

	// Send second GSSENCRequest — should be rejected
	writeGSSENCRequest(t, clientConn)

	// Server should return an error about duplicate GSSENCRequest
	err := <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate GSSENCRequest")
}

func TestSSLRequest_BufferStuffingPrevention(t *testing.T) {
	// CVE-2021-23222: If unencrypted data is buffered between SSLRequest
	// and TLS handshake, the connection should be rejected.

	tlsConfig, _ := generateTestTLSConfig(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		netConn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}

		c := newTestConn(t, netConn, tlsConfig)
		errCh <- c.handleStartup()
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()

	// Write SSLRequest AND extra data in a single write (buffer stuffing attack).
	// The extra data arrives before the TLS handshake, simulating a MITM injection.
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(8))
	_ = binary.Write(&buf, binary.BigEndian, uint32(protocol.SSLRequestCode))
	buf.WriteString("INJECTED DATA") // This is the attack payload
	_, err = clientConn.Write(buf.Bytes())
	require.NoError(t, err)

	// Read 'S' response — server accepted SSL
	resp := readSingleByte(t, clientConn)
	require.Equal(t, byte('S'), resp)

	// Server should detect the buffered data and reject the connection.
	// The server reads 'S' response is already sent, but then checks buffered data.
	err = <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unencrypted data after SSL request")
}

func TestGSSENCRequest_ThenSSLRequest_WithTLS(t *testing.T) {
	// A client tries GSSENCRequest first (declined), then SSLRequest with TLS configured.
	// The SSLRequest should be accepted and TLS upgrade should work.

	tlsConfig, caPool := generateTestTLSConfig(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		netConn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}

		c := newTestConn(t, netConn, tlsConfig)
		errCh <- c.handleStartup()
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()

	// Send GSSENCRequest first
	writeGSSENCRequest(t, clientConn)

	// Read 'N' response (GSSAPI declined)
	resp := readSingleByte(t, clientConn)
	require.Equal(t, byte('N'), resp)

	// Send SSLRequest
	writeSSLRequest(t, clientConn)

	// Read 'S' response (SSL accepted)
	resp = readSingleByte(t, clientConn)
	require.Equal(t, byte('S'), resp)

	// Perform TLS handshake
	tlsClientConn := tls.Client(clientConn, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	err = tlsClientConn.Handshake()
	require.NoError(t, err)

	// Send startup message over TLS and authenticate
	writeStartupPacketToPipe(t, tlsClientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "gss_ssl_user",
		"database": "gss_ssl_db",
	})
	scramClientHelper(t, tlsClientConn, "gss_ssl_user", "postgres")

	err = <-errCh
	require.NoError(t, err)
}
