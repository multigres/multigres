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
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// timeoutTestServer accepts a single connection on a fresh local TCP listener
// and runs the full server-side serve() loop against it. The auth-timeout
// path is exercised end-to-end (deadline applied → expiry → PG-format
// FATAL → connection closed). When tlsConfig is non-nil, the listener
// will accept SSLRequest and upgrade to TLS.
func timeoutTestServer(t *testing.T, authTimeout time.Duration, tlsConfig ...*tls.Config) (clientConn net.Conn, serverErrCh <-chan error, cleanup func()) {
	t.Helper()

	cfg := ListenerConfig{
		Address:               "127.0.0.1:0",
		Handler:               &mockHandler{},
		CredentialProvider:    newMockCredentialProvider("postgres"),
		AuthenticationTimeout: authTimeout,
		Logger:                testLogger(t),
	}
	if len(tlsConfig) > 0 {
		cfg.TLSConfig = tlsConfig[0]
	}
	listener, err := NewListener(cfg)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		netConn, acceptErr := listener.listener.Accept()
		if acceptErr != nil {
			errCh <- acceptErr
			return
		}
		c := newConn(netConn, listener, 1)
		c.handler = listener.handler
		c.credentialProvider = listener.credentialProvider
		c.tlsConfig = listener.tlsConfig
		c.requireTLS = listener.requireTLS
		serveErr := c.serve()
		_ = c.Close()
		errCh <- serveErr
	}()

	cc, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	cleanup = func() {
		cc.Close()
		listener.Close()
	}
	return cc, errCh, cleanup
}

// containsErrField checks whether a PG ErrorResponse body contains a field
// (1-byte type code followed by a null-terminated string) with the given
// type code and value. ErrorResponse fields are 'S' (severity), 'C' (code),
// 'M' (message), etc.
func containsErrField(body []byte, code byte, value string) bool {
	for i := 0; i < len(body); {
		if body[i] == 0 {
			return false
		}
		fieldCode := body[i]
		i++
		end := i
		for end < len(body) && body[end] != 0 {
			end++
		}
		if end >= len(body) {
			return false
		}
		fieldVal := string(body[i:end])
		if fieldCode == code && fieldVal == value {
			return true
		}
		i = end + 1
	}
	return false
}

// TestAuthenticationTimeout_NoStartupMessage verifies that a client which
// connects but never sends a StartupMessage is disconnected after the
// configured timeout, and the server emits a PG-format FATAL ErrorResponse
// with SQLSTATE 08006 instead of letting the goroutine hang.
func TestAuthenticationTimeout_NoStartupMessage(t *testing.T) {
	const timeout = 200 * time.Millisecond
	clientConn, serverErrCh, cleanup := timeoutTestServer(t, timeout)
	defer cleanup()

	// Client never writes anything. Server must time out and reply with
	// a FATAL ErrorResponse (SQLSTATE 08006). Bound the read with a
	// generous client-side deadline so a buggy server doesn't hang the
	// test indefinitely.
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))

	start := time.Now()
	msgType, body := readMessage(t, clientConn)
	elapsed := time.Since(start)

	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.True(t, containsErrField(body, 'S', "FATAL"), "severity should be FATAL")
	assert.True(t, containsErrField(body, 'C', "08006"), "SQLSTATE should be 08006 (connection_failure)")
	assert.True(t, containsErrField(body, 'M', "canceling authentication due to timeout"))
	assert.GreaterOrEqual(t, elapsed, timeout-50*time.Millisecond,
		"server should not respond before the deadline")
	assert.Less(t, elapsed, 5*time.Second,
		"server should respond shortly after the deadline")

	// After the error, server should close the connection. serve()
	// returns the original deadline-exceeded error.
	require.Eventually(t, func() bool {
		select {
		case err := <-serverErrCh:
			return err != nil
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
}

// TestAuthenticationTimeout_PartialSCRAM verifies that a client which begins
// the SCRAM exchange but stalls partway through is disconnected after the
// timeout. The server-first message has already been sent, so the deadline
// must apply to the SASLResponse read, not just the initial packet.
func TestAuthenticationTimeout_PartialSCRAM(t *testing.T) {
	const timeout = 200 * time.Millisecond
	clientConn, serverErrCh, cleanup := timeoutTestServer(t, timeout)
	defer cleanup()

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))

	// Send a valid StartupMessage to advance past the first read.
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber,
		map[string]string{"user": "stalluser", "database": "stalldb"})

	// Read AuthenticationSASL — server is now waiting for SASLInitialResponse.
	msgType, body := readMessage(t, clientConn)
	require.Equal(t, byte(protocol.MsgAuthenticationRequest), msgType)
	authType := binary.BigEndian.Uint32(body[:4])
	require.Equal(t, uint32(protocol.AuthSASL), authType)

	// Stall: never send SASLInitialResponse. Server must time out.
	start := time.Now()
	msgType, body = readMessage(t, clientConn)
	elapsed := time.Since(start)

	assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)
	assert.True(t, containsErrField(body, 'S', "FATAL"))
	assert.True(t, containsErrField(body, 'C', "08006"))
	assert.True(t, containsErrField(body, 'M', "canceling authentication due to timeout"))
	assert.GreaterOrEqual(t, elapsed, timeout-100*time.Millisecond,
		"server should not respond before the deadline")

	require.Eventually(t, func() bool {
		select {
		case err := <-serverErrCh:
			return err != nil
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
}

// TestAuthenticationTimeout_SuccessfulAuthClearsDeadline verifies that the
// startup deadline is removed once authentication completes — an
// authenticated but idle connection must not be killed by the auth timeout.
func TestAuthenticationTimeout_SuccessfulAuthClearsDeadline(t *testing.T) {
	const timeout = 300 * time.Millisecond
	clientConn, serverErrCh, cleanup := timeoutTestServer(t, timeout)
	defer cleanup()

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(10*time.Second)))

	// Complete the startup + SCRAM handshake.
	writeStartupPacketToPipe(t, clientConn, protocol.ProtocolVersionNumber,
		map[string]string{"user": "okuser", "database": "okdb"})
	scramClientHelper(t, clientConn, "okuser", "postgres")

	// Idle longer than the auth timeout. If the deadline weren't cleared,
	// the server would kill the connection here.
	time.Sleep(timeout * 2)

	// Connection must still be alive: send a Terminate ('X') and expect
	// the server to read it and exit cleanly without an ErrorResponse.
	header := make([]byte, 5)
	header[0] = byte(protocol.MsgTerminate)
	binary.BigEndian.PutUint32(header[1:5], 4)
	_, err := clientConn.Write(header)
	require.NoError(t, err)

	// Server should return cleanly (Terminate is signaled as io.EOF
	// inside handleMessage and serve() returns nil).
	select {
	case serveErr := <-serverErrCh:
		// nil is the success signal; io.EOF can also surface if the
		// client side closes first under the test cleanup.
		if serveErr != nil && !errors.Is(serveErr, io.EOF) {
			t.Fatalf("expected clean exit after Terminate, got %v", serveErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit after Terminate; deadline may not have been cleared")
	}
}

// TestAuthenticationTimeout_ConfigDefault verifies that a zero
// AuthenticationTimeout in ListenerConfig falls back to the protocol
// default of 60s (matching PostgreSQL).
func TestAuthenticationTimeout_ConfigDefault(t *testing.T) {
	listener, err := NewListener(ListenerConfig{
		Address:            "127.0.0.1:0",
		Handler:            &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"),
		Logger:             testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	assert.Equal(t, DefaultAuthenticationTimeout, listener.authenticationTimeout)
	assert.Equal(t, 60*time.Second, listener.authenticationTimeout)
}

// TestAuthenticationTimeout_PendingTLSHandshakeSkipsErrorReply verifies the
// post-SSL-accept-pre-TLS-handshake state: the server has answered 'S' to
// SSLRequest but the client hasn't completed the TLS handshake. The
// underlying conn is still plaintext while the client expects encrypted
// bytes — writing an unencrypted ErrorResponse would surface as garbage
// on the client. The server must close cleanly without the unencrypted
// reply.
func TestAuthenticationTimeout_PendingTLSHandshakeSkipsErrorReply(t *testing.T) {
	tlsConfig, _ := generateTestTLSConfig(t)

	const timeout = 200 * time.Millisecond
	clientConn, serverErrCh, cleanup := timeoutTestServer(t, timeout, tlsConfig)
	defer cleanup()

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))

	// Send SSLRequest, read 'S' acceptance, then stall — never start
	// the TLS handshake.
	writeSSLRequest(t, clientConn)
	resp := readSingleByte(t, clientConn)
	require.Equal(t, byte('S'), resp)

	// Read until the server closes. We must not see anything that
	// looks like a PG ErrorResponse byte ('E') here — that would mean
	// the server wrote unencrypted bytes after promising TLS.
	buf := make([]byte, 64)
	n, readErr := clientConn.Read(buf)

	if n > 0 {
		assert.NotEqual(t, byte(protocol.MsgErrorResponse), buf[0],
			"server must not send unencrypted ErrorResponse after accepting SSL")
	}
	// EOF (or connection-reset) is the expected outcome — server
	// detected the timeout, skipped the unencrypted reply, and closed.
	require.Error(t, readErr)

	require.Eventually(t, func() bool {
		select {
		case err := <-serverErrCh:
			return err != nil
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
}

// TestAuthenticationTimeout_CancelRequestNoSpuriousErrors verifies that a
// CancelRequest received during the startup phase does not produce
// spurious Error logs from the deadline-clear path. handleCancelRequest
// closes the connection and returns nil; serve() must detect that and
// bail out without trying to clear the deadline on the now-closed fd.
func TestAuthenticationTimeout_CancelRequestNoSpuriousErrors(t *testing.T) {
	const timeout = 30 * time.Second
	clientConn, serverErrCh, cleanup := timeoutTestServer(t, timeout)
	defer cleanup()

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(2*time.Second)))

	// Send a CancelRequest startup packet: length(16) + code + pid + secret.
	pkt := make([]byte, 16)
	binary.BigEndian.PutUint32(pkt[0:4], 16)
	binary.BigEndian.PutUint32(pkt[4:8], protocol.CancelRequestCode)
	binary.BigEndian.PutUint32(pkt[8:12], 12345)
	binary.BigEndian.PutUint32(pkt[12:16], 67890)
	_, err := clientConn.Write(pkt)
	require.NoError(t, err)

	// Server closes the cancel connection per protocol. serve() must
	// return nil — no error path triggered.
	select {
	case serveErr := <-serverErrCh:
		require.NoError(t, serveErr,
			"cancel request must not produce a serve() error (would be a regression on the deadline-clear path)")
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit after CancelRequest")
	}
}

// TestAuthenticationTimeout_NegativeDisables verifies that a negative
// AuthenticationTimeout disables the deadline entirely (escape hatch for
// operators who want to opt out of the default).
func TestAuthenticationTimeout_NegativeDisables(t *testing.T) {
	listener, err := NewListener(ListenerConfig{
		Address:               "127.0.0.1:0",
		Handler:               &mockHandler{},
		CredentialProvider:    newMockCredentialProvider("postgres"),
		AuthenticationTimeout: -1,
		Logger:                testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	assert.Less(t, listener.authenticationTimeout, time.Duration(0),
		"negative value should be preserved (disables the deadline)")
}
