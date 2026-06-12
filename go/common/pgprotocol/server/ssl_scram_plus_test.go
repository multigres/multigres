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
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
)

// readSASLMechanisms parses the mechanism list from an AuthenticationSASL
// message body (skipping the 4-byte auth code prefix). Mechanisms are
// null-terminated strings followed by an empty terminator.
func readSASLMechanisms(t *testing.T, body []byte) []string {
	t.Helper()
	// body[:4] is the AuthSASL int32. Mechanism list follows.
	rest := body[4:]
	var out []string
	for {
		idx := bytes.IndexByte(rest, 0)
		require.GreaterOrEqual(t, idx, 0, "malformed AuthenticationSASL: no null terminator")
		if idx == 0 {
			return out
		}
		out = append(out, string(rest[:idx]))
		rest = rest[idx+1:]
	}
}

// scramPlusClientHelper drives a SCRAM-SHA-256-PLUS handshake from the
// client side using the channel binding hash derived from the TLS peer cert.
// Fails the test if the server doesn't advertise PLUS or if the handshake
// doesn't complete.
func scramPlusClientHelper(t *testing.T, conn *tls.Conn, username, password string) {
	t.Helper()

	// AuthenticationSASL: read advertised mechanisms.
	msgType, body := readMessage(t, conn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	require.Equal(t, uint32(protocol.AuthSASL), binary.BigEndian.Uint32(body[:4]))
	mechs := readSASLMechanisms(t, body)
	require.Contains(t, mechs, scram.ScramSHA256PlusMechanism,
		"server must advertise %s over TLS", scram.ScramSHA256PlusMechanism)

	// Compute tls-server-end-point hash from the server cert the TLS handshake
	// actually surfaced — this is what a real libpq client does.
	state := conn.ConnectionState()
	require.NotEmpty(t, state.PeerCertificates)
	cbHash, err := scram.ComputeTLSServerEndPointHash(state.PeerCertificates[0])
	require.NoError(t, err)

	client := scram.NewSCRAMClientWithPassword(username, password)
	client.EnableChannelBinding(cbHash)

	clientFirst, err := client.ClientFirstMessage()
	require.NoError(t, err)
	writeSASLInitialResponse(t, conn, scram.ScramSHA256PlusMechanism, clientFirst)

	msgType, body = readMessage(t, conn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	require.Equal(t, uint32(protocol.AuthSASLContinue), binary.BigEndian.Uint32(body[:4]))
	serverFirst := string(body[4:])

	clientFinal, err := client.ProcessServerFirst(serverFirst)
	require.NoError(t, err)
	writeSASLResponse(t, conn, clientFinal)

	msgType, body = readMessage(t, conn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	require.Equal(t, uint32(protocol.AuthSASLFinal), binary.BigEndian.Uint32(body[:4]))
	require.NoError(t, client.VerifyServerFinal(string(body[4:])))

	msgType, body = readMessage(t, conn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	require.Equal(t, uint32(protocol.AuthOk), binary.BigEndian.Uint32(body[:4]))

	for {
		mt, _ := readMessage(t, conn)
		if mt == byte(protocol.MsgReadyForQuery) {
			return
		}
	}
}

// TestSSL_SCRAMPlus_HappyPath verifies the full PostgreSQL-parity flow over
// TLS: server advertises SCRAM-SHA-256-PLUS, client negotiates it using the
// tls-server-end-point hash of the server cert, and auth completes.
func TestSSL_SCRAMPlus_HappyPath(t *testing.T) {
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

	writeSSLRequest(t, clientConn)
	require.Equal(t, byte('S'), readSingleByte(t, clientConn))

	tlsClientConn := tls.Client(clientConn, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	require.NoError(t, tlsClientConn.Handshake())

	writeStartupPacketToPipe(t, tlsClientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "tlsuser",
		"database": "tlsdb",
	})
	scramPlusClientHelper(t, tlsClientConn, "tlsuser", "postgres")

	require.NoError(t, <-errCh)
}

// TestSSL_SCRAMPlus_TamperedCBindRejected feeds the server a wrong
// tls-server-end-point hash and expects a FATAL ErrorResponse with SQLSTATE
// 28000 — matching PG's "channel binding authentication failed" class.
func TestSSL_SCRAMPlus_TamperedCBindRejected(t *testing.T) {
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

	writeSSLRequest(t, clientConn)
	require.Equal(t, byte('S'), readSingleByte(t, clientConn))

	tlsClientConn := tls.Client(clientConn, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	require.NoError(t, tlsClientConn.Handshake())

	writeStartupPacketToPipe(t, tlsClientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "tlsuser",
		"database": "tlsdb",
	})

	// AuthenticationSASL.
	msgType, body := readMessage(t, tlsClientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	require.Equal(t, uint32(protocol.AuthSASL), binary.BigEndian.Uint32(body[:4]))

	// Use a hash that has nothing to do with the real cert.
	bogusHash := bytes.Repeat([]byte{0xAA}, 32)
	client := scram.NewSCRAMClientWithPassword("tlsuser", "postgres")
	client.EnableChannelBinding(bogusHash)
	clientFirst, err := client.ClientFirstMessage()
	require.NoError(t, err)
	writeSASLInitialResponse(t, tlsClientConn, scram.ScramSHA256PlusMechanism, clientFirst)

	// Server-first (cbind validation happens in client-final, so we still
	// get a normal continue here).
	_, body = readMessage(t, tlsClientConn)
	clientFinal, err := client.ProcessServerFirst(string(body[4:]))
	require.NoError(t, err)
	writeSASLResponse(t, tlsClientConn, clientFinal)

	// Expect ErrorResponse FATAL/28000 with PG-verbatim "SCRAM channel
	// binding check failed" (matches PG17 auth-scram.c).
	msgType, body = readMessage(t, tlsClientConn)
	require.Equal(t, byte('E'), msgType, "expected ErrorResponse, got %c", msgType)
	bodyStr := string(body)
	require.Contains(t, bodyStr, "28000", "expected SQLSTATE 28000, body=%q", bodyStr)
	require.Contains(t, bodyStr, "SCRAM channel binding check failed",
		"expected PG-style message, body=%q", bodyStr)

	// Server emits the ErrorResponse and returns from handleStartup. The
	// FATAL is communicated to the client via wire, not via a Go error
	// (mirrors how the other auth-failure paths behave).
	<-errCh
}

// TestSSL_SCRAMPlus_DowngradeYRejected: over TLS, the client picks plain
// SCRAM-SHA-256 with the "y" gs2 flag (claiming the server doesn't support
// cbind). Per RFC 5802 §6 this is a downgrade attempt and must fail.
func TestSSL_SCRAMPlus_DowngradeYRejected(t *testing.T) {
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

	writeSSLRequest(t, clientConn)
	require.Equal(t, byte('S'), readSingleByte(t, clientConn))

	tlsClientConn := tls.Client(clientConn, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	require.NoError(t, tlsClientConn.Handshake())

	writeStartupPacketToPipe(t, tlsClientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "tlsuser",
		"database": "tlsdb",
	})

	// AuthenticationSASL — read but ignore advertised list.
	_, _ = readMessage(t, tlsClientConn)

	// Hand-craft a client-first with the "y" downgrade flag.
	clientFirst := "y,,n=tlsuser,r=fyko+d2lbbFgONRv9qkxdawL"
	writeSASLInitialResponse(t, tlsClientConn, scram.ScramSHA256Mechanism, clientFirst)

	// Expect FATAL ErrorResponse with PG-verbatim 28000 / "SCRAM channel
	// binding negotiation error" immediately — before any continue.
	msgType, body := readMessage(t, tlsClientConn)
	require.Equal(t, byte('E'), msgType, "expected ErrorResponse, got %c", msgType)
	bodyStr := string(body)
	assert.Contains(t, bodyStr, "28000")
	assert.Contains(t, bodyStr, "SCRAM channel binding negotiation error")

	// Server emits ErrorResponse via wire and returns from handleStartup.
	<-errCh
}

// TestSSL_SCRAM_AuthzidRejected: client sets SASL authzid in gs2-header.
// PG rejects with 0A000 "client uses authorization identity, but it is not
// supported". Mirror that exactly.
func TestSSL_SCRAM_AuthzidRejected(t *testing.T) {
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	c := newTestConn(t, serverConn, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "tlsuser",
		"database": "tlsdb",
	})

	// Read AuthenticationSASL.
	_, _ = readMessage(t, clientConn)

	// Hand-craft a client-first with authzid.
	clientFirst := "n,a=evil-authz,n=tlsuser,r=nonce"
	writeSASLInitialResponse(t, clientConn, scram.ScramSHA256Mechanism, clientFirst)

	msgType, body := readMessage(t, clientConn)
	require.Equal(t, byte('E'), msgType)
	bodyStr := string(body)
	assert.Contains(t, bodyStr, "0A000", "expected SQLSTATE 0A000, body=%q", bodyStr)
	assert.Contains(t, bodyStr, "client uses authorization identity")
	<-errCh
}

// TestSSL_SCRAM_SingleErrorAfterCBindFatal asserts that after sending the
// FATAL ErrorResponse for a cbind protocol error, the server does NOT write
// a second wrapper ErrorResponse. PG protocol says no bytes follow a FATAL;
// a regression here would surface as a duplicate "ErrorResponse" message
// on the wire, which would confuse libpq's error parsing.
func TestSSL_SCRAM_SingleErrorAfterCBindFatal(t *testing.T) {
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	c := newTestConn(t, serverConn, nil)

	errCh := make(chan error, 1)
	go func() {
		// Drive the full conn lifecycle (handleStartup + serve()'s error
		// path) so any double-write would appear on the wire.
		_ = c.handleStartup()
		errCh <- nil
	}()

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "tlsuser",
		"database": "tlsdb",
	})
	_, _ = readMessage(t, clientConn) // AuthenticationSASL

	// Send a protocol-violating client-first (authzid → 0A000).
	writeSASLInitialResponse(t, clientConn, scram.ScramSHA256Mechanism,
		"n,a=evil,n=tlsuser,r=nonce")

	// First message must be the FATAL with 0A000.
	mt, body := readMessage(t, clientConn)
	require.Equal(t, byte('E'), mt)
	require.Contains(t, string(body), "0A000")

	// Drain any subsequent bytes the server might send within a short
	// deadline. PG sends nothing after FATAL, so the read must return EOF
	// (pipe close) and no extra ErrorResponse must precede it.
	type readResult struct {
		mt   byte
		body []byte
		err  error
	}
	done := make(chan readResult, 1)
	go func() {
		header := make([]byte, 5)
		_, err := io.ReadFull(clientConn, header)
		if err != nil {
			done <- readResult{err: err}
			return
		}
		length := binary.BigEndian.Uint32(header[1:5]) - 4
		body := make([]byte, length)
		_, err = io.ReadFull(clientConn, body)
		done <- readResult{mt: header[0], body: body, err: err}
	}()

	// Close the server side after a brief grace window so the read can
	// resolve to EOF if no extra bytes were written.
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = serverConn.Close()
	}()

	select {
	case rr := <-done:
		require.Error(t, rr.err, "expected EOF after FATAL, got extra message type=%c body=%q", rr.mt, string(rr.body))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connection close after FATAL")
	}
	<-errCh
}

// TestSSL_NoPlusAdvertisedWithoutTLS asserts that on a plaintext connection,
// the server advertises ONLY SCRAM-SHA-256 — never PLUS.
func TestSSL_NoPlusAdvertisedWithoutTLS(t *testing.T) {
	serverConn, clientConn := newPipeConnPair()
	defer serverConn.Close()
	defer clientConn.Close()

	c := newTestConn(t, serverConn, nil) // no TLS config

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStartup()
	}()

	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber, map[string]string{
		"user":     "plainuser",
		"database": "plaindb",
	})

	msgType, body := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	require.Equal(t, uint32(protocol.AuthSASL), binary.BigEndian.Uint32(body[:4]))
	mechs := readSASLMechanisms(t, body)
	require.Equal(t, []string{scram.ScramSHA256Mechanism}, mechs,
		"plaintext connection must NOT advertise SCRAM-SHA-256-PLUS")

	// Complete auth so the server goroutine exits cleanly.
	client := scram.NewSCRAMClientWithPassword("plainuser", "postgres")
	clientFirst, err := client.ClientFirstMessage()
	require.NoError(t, err)
	writeSASLInitialResponse(t, clientConn, scram.ScramSHA256Mechanism, clientFirst)
	_, body = readMessage(t, clientConn)
	clientFinal, err := client.ProcessServerFirst(string(body[4:]))
	require.NoError(t, err)
	writeSASLResponse(t, clientConn, clientFinal)
	for {
		mt, _ := readMessage(t, clientConn)
		if mt == byte(protocol.MsgReadyForQuery) {
			break
		}
	}
	require.NoError(t, <-errCh)
}
