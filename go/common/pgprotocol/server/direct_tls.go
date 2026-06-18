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

// Direct TLS (PostgreSQL 17 "sslnegotiation=direct") server-side support.
//
// PostgreSQL 17 added a TLS-first connection mode: instead of the classic
// SSLRequest → 'S' → handshake dance, the client opens the TCP connection
// and immediately sends a TLS ClientHello. The server disambiguates by
// looking at the first byte — 0x16 is the TLS "handshake" record type and
// can never start a legitimate startup packet (it would imply a length of
// hundreds of megabytes, far past MaxStartupPacketLength).
//
// This mirrors ProcessSSLStartup in PostgreSQL's
// src/backend/tcop/backend_startup.c, with the same hard requirements:
//
//   - Direct TLS is accepted whenever TLS is configured; there is no
//     opt-in knob (PG 17 likewise has no GUC for it).
//   - ALPN ("postgresql", RFC 7301) is MANDATORY for direct connections.
//     A client that completes the handshake without negotiating it is
//     rejected as a protocol violation. This is PG's cross-protocol-attack
//     hardening: without ALPN, a generic TLS client (e.g. an HTTPS client
//     coaxed into connecting here) could be replayed against the database.
//     Negotiated (SSLRequest) connections keep ALPN optional, matching PG's
//     compatibility posture for pre-17 clients.
//   - When TLS is not configured, the connection is closed without writing
//     anything: the peer is speaking TLS, so a plaintext ErrorResponse
//     would be unintelligible garbage. PostgreSQL also rejects silently.
//
// This is the server half of the Envoy SSLRequest-offload model (MINT-37):
// an edge proxy can answer the client's SSLRequest itself and forward the
// raw TLS byte stream, which the gateway then sees as a direct TLS
// connection.

package server

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// directTLSFirstByte is the first byte of every TLS record carrying a
// handshake message (ContentType handshake = 22 = 0x16). PostgreSQL 17 uses
// the same single-byte disambiguation between a TLS ClientHello and a
// startup packet length word.
const directTLSFirstByte = 0x16

// maybeHandleDirectTLS peeks the first byte of a fresh connection and, when
// it announces a TLS handshake, upgrades the connection via handleDirectTLS.
// For anything else it returns nil without consuming bytes so the classic
// startup-packet flow proceeds untouched.
//
// Runs exactly once per connection, before the first startup packet read —
// the same point PostgreSQL calls ProcessSSLStartup. The peek blocks until
// the client sends its first byte, covered by the same authentication
// deadline that bounds the rest of the startup phase.
func (c *Conn) maybeHandleDirectTLS() error {
	firstByte, err := c.bufferedReader.Peek(1)
	if err != nil {
		// io.EOF here means the client connected and left without sending
		// anything (port scanners, health checks). serve() recognizes the
		// wrapped EOF and treats it as a clean disconnect, matching PG's
		// "don't clutter the log" behavior.
		return fmt.Errorf("failed to read first startup byte: %w", err)
	}
	if firstByte[0] != directTLSFirstByte {
		return nil
	}
	return c.handleDirectTLS()
}

// handleDirectTLS performs the server-side direct TLS upgrade: handshake
// first (replaying any ClientHello bytes the peek pulled into the buffered
// reader), mandatory ALPN check second. On success c.conn is the *tls.Conn
// and the caller reads the startup packet (or cancel request) over the
// encrypted channel.
func (c *Conn) handleDirectTLS() error {
	c.logger.Debug("client initiated direct TLS handshake")

	if c.tlsConfig == nil {
		// PG parity: reject without writing anything. The client is
		// mid-ClientHello; any plaintext bytes we emit would corrupt its
		// TLS state machine rather than convey an error.
		c.directTLSFailed = true
		c.metrics().RecordDirectTLSRejected(c.ctx, DirectTLSRejectedReasonTLSDisabled)
		c.logger.Warn("rejecting direct TLS connection: TLS is not configured",
			"remote_addr", c.RemoteAddr())
		return errors.New("direct TLS connection rejected: TLS is not configured")
	}

	// The single-byte peek may have pulled an arbitrary prefix of the
	// ClientHello into the buffered reader (bufio reads as much as is
	// available). Hand those bytes back to the TLS engine by replaying
	// them ahead of the raw connection.
	transport, err := c.replayBufferedBytes()
	if err != nil {
		c.directTLSFailed = true
		return fmt.Errorf("direct TLS: %w", err)
	}

	tlsConn, err := c.completeTLSHandshake(transport, c.directTLSConfig(), TLSNegotiationDirect)
	if err != nil {
		// crypto/tls already sent the appropriate TLS alert; nothing
		// intelligible can be written in plaintext after a TLS failure.
		c.directTLSFailed = true
		return fmt.Errorf("direct TLS: %w", err)
	}

	// ALPN is mandatory for direct TLS connections (PG 17 parity; see the
	// package comment). A client that offered a non-"postgresql" ALPN list
	// already failed the handshake above with a no_application_protocol
	// alert; this branch catches clients that offered no ALPN at all.
	//
	// Divergence from PostgreSQL, deliberately: PG logs a COMMERROR and
	// closes without telling the client anything. We have a fully
	// established TLS channel at this point, so we return the FATAL
	// PgDiagnostic (same wording, SQLSTATE 08P01 protocol_violation) and
	// let serve()'s startup-error path deliver it through the tunnel —
	// strictly more debuggable for the operator of a misconfigured client,
	// and wire-safe because the bytes are encrypted.
	if tlsConn.ConnectionState().NegotiatedProtocol != protocol.ALPNProtocol {
		c.metrics().RecordDirectTLSRejected(c.ctx, DirectTLSRejectedReasonNoALPN)
		c.logger.Warn("rejecting direct TLS connection: no ALPN protocol negotiated",
			"remote_addr", c.RemoteAddr())
		return mterrors.NewPgError("FATAL", mterrors.PgSSProtocolViolation,
			"received direct SSL connection request without ALPN protocol negotiation extension", "")
	}

	return nil
}

// directTLSConfig returns the TLS config to use for a direct handshake,
// guaranteeing the "postgresql" ALPN protocol is offered. ALPN is mandatory
// for direct connections, so a listener config without NextProtos must not
// cause a well-behaved libpq (which always offers ALPN in direct mode) to be
// rejected just because Go ignored its offer. Configs that already advertise
// the protocol — like multigateway's — are used as-is.
func (c *Conn) directTLSConfig() *tls.Config {
	if slices.Contains(c.tlsConfig.NextProtos, protocol.ALPNProtocol) {
		return c.tlsConfig
	}
	cfg := c.tlsConfig.Clone()
	cfg.NextProtos = append(cfg.NextProtos, protocol.ALPNProtocol)
	return cfg
}

// replayBufferedBytes drains whatever the startup reader has buffered and
// returns a net.Conn that replays those bytes before reading from the
// underlying connection. crypto/tls must see the complete record stream from
// its first byte, and the disambiguation peek may have buffered part (or
// all) of the ClientHello.
func (c *Conn) replayBufferedBytes() (net.Conn, error) {
	buffered := c.bufferedReader.Buffered()
	prefix := make([]byte, buffered)
	if _, err := io.ReadFull(c.bufferedReader, prefix); err != nil {
		return nil, fmt.Errorf("failed to drain buffered startup bytes: %w", err)
	}
	return &prefixConn{
		Conn:   c.conn,
		reader: io.MultiReader(bytes.NewReader(prefix), c.conn),
	}, nil
}

// prefixConn is a net.Conn whose reads are served from a replay reader (the
// buffered prefix followed by the underlying connection). Writes, deadlines,
// addresses, and Close delegate to the embedded connection, so deadline
// handling in serve() keeps working after the *tls.Conn wraps this.
type prefixConn struct {
	net.Conn
	reader io.Reader
}

func (p *prefixConn) Read(b []byte) (int, error) { return p.reader.Read(b) }
