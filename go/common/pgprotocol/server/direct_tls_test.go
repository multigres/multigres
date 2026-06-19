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

// Direct-TLS (PostgreSQL 17 sslnegotiation=direct) server-side tests.
//
// The scenarios mirror the direct-SSL rows of PostgreSQL's connection
// negotiation matrix in src/test/libpq/t/005_negotiate_encryption.pl and
// the ALPN checks in src/test/ssl/t/001_ssltests.pl, translated to the
// multigres unit-test harness:
//
//	server has SSL, client direct + ALPN          -> connect over TLS
//	server has SSL, client direct without ALPN    -> reject (protocol violation)
//	server has SSL, client direct with wrong ALPN -> handshake failure
//	server lacks SSL, client direct               -> silently closed
//	SSLRequest inside the direct tunnel           -> 'N' (decline, continue)
//	cancel request over direct TLS                -> processed, conn closed

package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/test/utils"
)

// startDirectTLSServer accepts one connection on a fresh TCP listener,
// builds a test Conn around it with the supplied TLS config, and runs
// handleStartup, delivering the result on the returned channel. configure
// (optional) tweaks the Conn before startup runs.
func startDirectTLSServer(t *testing.T, tlsConfig *tls.Config, configure func(*Conn)) (net.Listener, <-chan error) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	errCh := make(chan error, 1)
	go func() {
		netConn, acceptErr := ln.Accept()
		if acceptErr != nil {
			errCh <- acceptErr
			return
		}
		c := newTestConn(t, netConn, tlsConfig)
		if configure != nil {
			configure(c)
		}
		errCh <- c.handleStartup()
	}()

	return ln, errCh
}

type recordingAuthMetrics struct {
	tlsHandshakes  map[string]int
	tlsConnections []string
	directRejected map[string]int
}

func newRecordingAuthMetrics() *recordingAuthMetrics {
	return &recordingAuthMetrics{
		tlsHandshakes:  make(map[string]int),
		directRejected: make(map[string]int),
	}
}

func (r *recordingAuthMetrics) RecordSCRAMDuration(context.Context, string, time.Duration) {}
func (r *recordingAuthMetrics) RecordAuthAttempt(context.Context, string)                  {}
func (r *recordingAuthMetrics) RecordCredentialLookup(context.Context, time.Duration)      {}
func (r *recordingAuthMetrics) RecordPlaintextRejected(context.Context, string)            {}
func (r *recordingAuthMetrics) RecordSSLRequestDeclined(context.Context)                   {}

func (r *recordingAuthMetrics) RecordTLSHandshake(_ context.Context, negotiation, outcome string, _ time.Duration) {
	r.tlsHandshakes[negotiation+"/"+outcome]++
}

func (r *recordingAuthMetrics) RecordTLSConnection(_ context.Context, negotiation string, _, _ uint16) {
	r.tlsConnections = append(r.tlsConnections, negotiation)
}

func (r *recordingAuthMetrics) RecordDirectTLSRejected(_ context.Context, reason string) {
	r.directRejected[reason]++
}

// dialDirect opens a TCP connection and immediately performs a client-side
// TLS handshake (no SSLRequest first) — the libpq sslnegotiation=direct
// wire behavior. Returns the established *tls.Conn.
func dialDirect(t *testing.T, addr string, clientCfg *tls.Config) *tls.Conn {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { raw.Close() })
	tlsConn := tls.Client(raw, clientCfg)
	require.NoError(t, tlsConn.Handshake(), "direct TLS handshake should succeed")
	return tlsConn
}

// TestDirectTLS_AcceptedWithALPN is the happy path: TLS-first connection
// offering ALPN "postgresql" is admitted, and startup + SCRAM complete over
// the encrypted channel. Mirrors 005_negotiate_encryption.pl's
// sslnegotiation=direct / server ssl=on row.
func TestDirectTLS_AcceptedWithALPN(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, tlsConfig, nil)

	tlsConn := dialDirect(t, ln.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{protocol.ALPNProtocol},
	})
	require.Equal(t, protocol.ALPNProtocol, tlsConn.ConnectionState().NegotiatedProtocol,
		"server must negotiate the postgresql ALPN protocol")

	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "direct_user",
		"database": "direct_db",
	})
	scramClientHelper(t, tlsConn, "direct_user", "postgres")

	require.NoError(t, <-errCh)
}

// TestDirectTLS_ServerConfigWithALPNPreset verifies the directTLSConfig
// fast path: when the listener's TLS config already advertises
// "postgresql" (multigateway's production shape), it is used as-is and the
// connection is admitted.
func TestDirectTLS_ServerConfigWithALPNPreset(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)
	tlsConfig.NextProtos = []string{protocol.ALPNProtocol}
	ln, errCh := startDirectTLSServer(t, tlsConfig, nil)

	tlsConn := dialDirect(t, ln.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{protocol.ALPNProtocol},
	})
	require.Equal(t, protocol.ALPNProtocol, tlsConn.ConnectionState().NegotiatedProtocol)

	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user": "alpn_preset_user",
	})
	scramClientHelper(t, tlsConn, "alpn_preset_user", "postgres")

	require.NoError(t, <-errCh)
}

// TestDirectTLS_RejectedWithoutALPN: a client that completes the direct TLS
// handshake without offering ALPN must be rejected with PostgreSQL's exact
// diagnostic (SQLSTATE 08P01). Mirrors the server-side requirement added in
// PG 17 (backend_startup.c: "received direct SSL connection request without
// ALPN protocol negotiation extension").
func TestDirectTLS_RejectedWithoutALPN(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, tlsConfig, nil)

	// No NextProtos: handshake succeeds (Go ignores absent ALPN) but the
	// server's post-handshake gate must fire.
	dialDirect(t, ln.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})

	err := <-errCh
	require.Error(t, err)
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, mterrors.PgSSProtocolViolation, diag.Code)
	assert.Contains(t, diag.Message, "without ALPN protocol negotiation extension")
}

func TestDirectTLS_MetricsCountOnlyAdmittedConnections(t *testing.T) {
	t.Run("missing ALPN is rejected but not counted as a TLS connection", func(t *testing.T) {
		tlsConfig, caPool := generateTestTLSConfig(t)
		metrics := newRecordingAuthMetrics()
		ln, errCh := startDirectTLSServer(t, tlsConfig, func(c *Conn) {
			c.authMetrics = metrics
		})

		dialDirect(t, ln.Addr().String(), &tls.Config{
			RootCAs:    caPool,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12,
		})

		err := <-errCh
		require.Error(t, err)
		assert.Equal(t, 1, metrics.tlsHandshakes[TLSNegotiationDirect+"/"+TLSOutcomeSuccess],
			"TLS handshake still completed successfully")
		assert.Empty(t, metrics.tlsConnections,
			"ALPN-rejected direct TLS must not be counted as an admitted TLS connection")
		assert.Equal(t, 1, metrics.directRejected[DirectTLSRejectedReasonNoALPN])
	})

	t.Run("accepted direct TLS is counted after ALPN validation", func(t *testing.T) {
		tlsConfig, caPool := generateTestTLSConfig(t)
		metrics := newRecordingAuthMetrics()
		ln, errCh := startDirectTLSServer(t, tlsConfig, func(c *Conn) {
			c.authMetrics = metrics
		})

		tlsConn := dialDirect(t, ln.Addr().String(), &tls.Config{
			RootCAs:    caPool,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{protocol.ALPNProtocol},
		})
		writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
			"user": "metrics_direct_user",
		})
		scramClientHelper(t, tlsConn, "metrics_direct_user", "postgres")

		require.NoError(t, <-errCh)
		assert.Equal(t, 1, metrics.tlsHandshakes[TLSNegotiationDirect+"/"+TLSOutcomeSuccess])
		assert.Equal(t, []string{TLSNegotiationDirect}, metrics.tlsConnections)
		assert.Empty(t, metrics.directRejected)
	})
}

// TestDirectTLS_RejectedWithoutALPN_WireLevel drives the full serve() path
// and asserts the FATAL ErrorResponse is delivered to the client through
// the TLS tunnel, then the connection is closed.
func TestDirectTLS_RejectedWithoutALPN_WireLevel(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)

	listener, err := NewListener(ListenerConfig{
		Address:            "127.0.0.1:0",
		Handler:            &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"),
		TLSConfig:          tlsConfig,
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	defer listener.Close()

	serverErrCh := make(chan error, 1)
	go func() {
		netConn, acceptErr := listener.listener.Accept()
		if acceptErr != nil {
			serverErrCh <- acceptErr
			return
		}
		c := newConn(netConn, listener, 1)
		c.handler = listener.handler
		c.credentialProvider = listener.credentialProvider
		c.tlsConfig = listener.tlsConfig
		serveErr := c.serve()
		_ = c.Close()
		serverErrCh <- serveErr
	}()

	tlsConn := dialDirect(t, listener.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		// No ALPN offered.
	})
	require.NoError(t, tlsConn.SetReadDeadline(time.Now().Add(5*time.Second)))

	msgType, body := readMessage(t, tlsConn)
	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.True(t, containsErrField(body, 'S', "FATAL"), "severity should be FATAL")
	assert.True(t, containsErrField(body, 'C', "08P01"), "SQLSTATE should be 08P01 (protocol_violation)")

	// Server closes after the FATAL — next read returns EOF/reset.
	one := make([]byte, 1)
	_, readErr := tlsConn.Read(one)
	require.Error(t, readErr, "server must close connection after FATAL")

	require.Error(t, <-serverErrCh)
}

// TestDirectTLS_RejectedWrongALPN: a client offering only non-PostgreSQL
// ALPN protocols fails the handshake itself with a no_application_protocol
// alert — the cross-protocol-attack hardening ALPN provides.
func TestDirectTLS_RejectedWrongALPN(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, tlsConfig, nil)

	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer raw.Close()

	tlsConn := tls.Client(raw, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"http/1.1"},
	})
	require.Error(t, tlsConn.Handshake(), "handshake must fail when no ALPN protocol overlaps")

	err = <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS handshake failed")
}

// TestDirectTLS_NoTLSConfig_ClosedSilently: when the server has no TLS
// config, a TLS-first client is rejected by closing the connection without
// writing anything — a plaintext ErrorResponse would be garbage inside the
// client's TLS state machine. Mirrors PG's silent reject in
// ProcessSSLStartup when !LoadedSSL.
func TestDirectTLS_NoTLSConfig_ClosedSilently(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	serverErrCh := make(chan error, 1)
	go func() {
		netConn, acceptErr := ln.Accept()
		if acceptErr != nil {
			serverErrCh <- acceptErr
			return
		}
		c := newTestConn(t, netConn, nil) // no TLS config
		serveErr := c.handleStartup()
		// Mirror serve()'s error path decision: it must conclude that no
		// plaintext reply can be sent.
		assert.False(t, c.canSendPlaintextStartupError(),
			"plaintext startup error must be suppressed after a direct TLS rejection")
		_ = netConn.Close()
		serverErrCh <- serveErr
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer clientConn.Close()

	// First byte 0x16 marks a TLS ClientHello.
	_, err = clientConn.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x10, 0x01, 0x02, 0x03})
	require.NoError(t, err)

	serveErr := <-serverErrCh
	require.Error(t, serveErr)
	assert.Contains(t, serveErr.Error(), "TLS is not configured")

	// The client must receive zero bytes before the close.
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, 16)
	n, readErr := clientConn.Read(buf)
	assert.Equal(t, 0, n, "server must not write anything to a rejected direct TLS client")
	assert.ErrorIs(t, readErr, io.EOF)
}

// TestDirectTLS_SSLRequestInTunnel_DeclinedWithN: PostgreSQL responds 'N'
// to an SSLRequest that arrives inside an established direct TLS tunnel
// (the ssl_in_use check in ProcessStartupPacket); the client then proceeds
// with a regular StartupMessage on the same encrypted channel.
func TestDirectTLS_SSLRequestInTunnel_DeclinedWithN(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, tlsConfig, nil)

	tlsConn := dialDirect(t, ln.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{protocol.ALPNProtocol},
	})

	// SSLRequest inside the tunnel.
	writeSSLRequest(t, tlsConn)
	require.Equal(t, byte('N'), readSingleByte(t, tlsConn),
		"SSLRequest inside a direct TLS tunnel must be declined with 'N'")

	// Continue with the normal startup on the same TLS channel.
	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user": "tunnel_user",
	})
	scramClientHelper(t, tlsConn, "tunnel_user", "postgres")

	require.NoError(t, <-errCh)
}

// TestDirectTLS_GSSENCRequestInTunnel_Declined: GSSENCRequest inside the
// tunnel is declined with 'N' (we never support GSS encryption), and
// startup continues.
func TestDirectTLS_GSSENCRequestInTunnel_Declined(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, tlsConfig, nil)

	tlsConn := dialDirect(t, ln.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{protocol.ALPNProtocol},
	})

	writeGSSENCRequest(t, tlsConn)
	require.Equal(t, byte('N'), readSingleByte(t, tlsConn))

	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user": "gss_tunnel_user",
	})
	scramClientHelper(t, tlsConn, "gss_tunnel_user", "postgres")

	require.NoError(t, <-errCh)
}

// TestDirectTLS_RequireTLS_Admitted: a direct TLS client satisfies the
// --pg-require-ssl posture (tlsHandshakeComplete is set before the
// StartupMessage is read), so the plaintext-rejection gate must not fire.
func TestDirectTLS_RequireTLS_Admitted(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, tlsConfig, func(c *Conn) {
		c.requireTLS = true
	})

	tlsConn := dialDirect(t, ln.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{protocol.ALPNProtocol},
	})

	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user": "require_direct_user",
	})
	scramClientHelper(t, tlsConn, "require_direct_user", "postgres")

	require.NoError(t, <-errCh)
}

// TestDirectTLS_SCRAMPlusChannelBinding: the server certificate captured
// during the direct handshake must feed SCRAM-SHA-256-PLUS channel binding,
// exactly as on the negotiated path.
func TestDirectTLS_SCRAMPlusChannelBinding(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, tlsConfig, nil)

	tlsConn := dialDirect(t, ln.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{protocol.ALPNProtocol},
	})

	writeStartupPacketToPipe(t, tlsConn, protocol.ProtocolVersionNumber, map[string]string{
		"user": "scram_plus_direct",
	})
	scramPlusClientHelper(t, tlsConn, "scram_plus_direct", "postgres")

	require.NoError(t, <-errCh)
}

// TestDirectTLS_CancelRequest: a CancelRequest may follow the direct TLS
// handshake instead of a StartupMessage (libpq sends cancels through the
// same encryption negotiation it used for the original connection). The
// request is processed and the connection closed without a reply.
func TestDirectTLS_CancelRequest(t *testing.T) {
	tlsConfig, caPool := generateTestTLSConfig(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	listener := testListener(t)
	connCh := make(chan *Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		netConn, acceptErr := ln.Accept()
		if acceptErr != nil {
			errCh <- acceptErr
			return
		}
		c := newConn(netConn, listener, 1)
		c.handler = listener.handler
		c.credentialProvider = listener.credentialProvider
		c.tlsConfig = tlsConfig
		connCh <- c
		errCh <- c.handleStartup()
	}()

	tlsConn := dialDirect(t, ln.Addr().String(), &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{protocol.ALPNProtocol},
	})

	// CancelRequest over the tunnel.
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(16))
	_ = binary.Write(&buf, binary.BigEndian, uint32(protocol.CancelRequestCode))
	_ = binary.Write(&buf, binary.BigEndian, uint32(12345)) // PID
	_ = binary.Write(&buf, binary.BigEndian, uint32(67890)) // secret
	_, err = tlsConn.Write(buf.Bytes())
	require.NoError(t, err)

	c := <-connCh
	require.NoError(t, <-errCh)
	assert.True(t, c.closed.Load(), "connection must be closed after a cancel request")
}

// TestDirectTLS_TLS11Rejected: MinVersion enforcement applies to the direct
// path the same as the negotiated one. Mirrors 001_ssltests.pl's TLS
// version-range coverage.
func TestDirectTLS_TLS11Rejected(t *testing.T) {
	tlsConfig, _ := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, tlsConfig, nil)

	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer raw.Close()

	//nolint:gosec // G402: intentionally using TLS 1.1 to test version enforcement
	tlsConn := tls.Client(raw, &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS10,
		MaxVersion:         tls.VersionTLS11,
		NextProtos:         []string{protocol.ALPNProtocol},
	})
	require.Error(t, tlsConn.Handshake(), "TLS 1.1 must be rejected")

	// Unblock the server's handshake read in case the client aborted
	// without sending a ClientHello.
	raw.Close()

	err = <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS handshake failed")
}

// TestDirectTLS_DisconnectBeforeFirstByte: a client that connects and
// disconnects without sending anything surfaces as a wrapped io.EOF, which
// serve() suppresses as a clean disconnect (matching PG's "don't clutter
// the log" rule in ProcessSSLStartup).
func TestDirectTLS_DisconnectBeforeFirstByte(t *testing.T) {
	ln, errCh := startDirectTLSServer(t, nil, nil)

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	clientConn.Close()

	err = <-errCh
	require.Error(t, err)
	assert.ErrorIs(t, err, io.EOF)
}

// TestDirectTLS_PGProtocolClientLoopback wires the in-repo pgprotocol
// client's sslnegotiation=direct mode against this package's listener — a
// full loopback of multigres' own client and server direct-TLS
// implementations including SCRAM auth over the tunnel.
func TestDirectTLS_PGProtocolClientLoopback(t *testing.T) {
	tlsConfig, _ := generateTestTLSConfig(t)

	listener, err := NewListener(ListenerConfig{
		Address:            "127.0.0.1:0",
		Handler:            &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"),
		TLSConfig:          tlsConfig,
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	defer listener.Close()
	go func() { _ = listener.Serve() }()

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)

	clientTLS, err := client.BuildTLSConfig(client.SSLModeRequire, "", "")
	require.NoError(t, err)

	ctx := utils.WithTimeout(t, 10*time.Second)
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:           "127.0.0.1",
		Port:           tcpAddr.Port,
		User:           "loopback_user",
		Password:       "postgres",
		Database:       "loopback_db",
		SSLMode:        client.SSLModeRequire,
		SSLNegotiation: client.SSLNegotiationDirect,
		TLSConfig:      clientTLS,
		DialTimeout:    5 * time.Second,
	})
	require.NoError(t, err, "direct TLS loopback connect must succeed")
	defer conn.Close()
}
