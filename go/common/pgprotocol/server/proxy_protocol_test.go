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
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"os"
	"testing"
	"testing/iotest"
	"time"

	"github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

const (
	// Version 3 with the PROXY command. Only version 2 is valid here.
	proxyTestInvalidVersion proxyproto.ProtocolVersionAndCommand = 0x31
	// Version 2 with unassigned command 2.
	proxyTestInvalidCommand proxyproto.ProtocolVersionAndCommand = 0x22
	// Unassigned address family and transport protocol.
	proxyTestUndefinedFamily proxyproto.AddressFamilyAndProtocol = 0xff
)

// proxyTestHeader builds wire fixtures without formatter validation so tests can
// send malformed headers. Pires provides the signature and valid field constants.
// The command byte combines version and command: PROXY (0x21) or LOCAL (0x20).
// The family byte combines address family and transport: TCPv4 (0x11),
// TCPv6 (0x21), UDPv4 (0x12), UnixStream (0x31), or UNSPEC (0x00).
func proxyTestHeader(command proxyproto.ProtocolVersionAndCommand, family proxyproto.AddressFamilyAndProtocol, payload []byte) []byte {
	header := bytes.Clone(proxyproto.SIGV2)
	header = append(header, byte(command), byte(family))
	header = binary.BigEndian.AppendUint16(header, uint16(len(payload)))
	return append(header, payload...)
}

func proxyTestAddress(ipv6 bool) []byte {
	var data []byte
	if ipv6 {
		data = append(data, net.ParseIP("2001:db8::1").To16()...)
		data = append(data, net.ParseIP("2001:db8::2").To16()...)
	} else {
		data = append(data, 192, 0, 2, 10, 192, 0, 2, 20)
	}
	data = binary.BigEndian.AppendUint16(data, 45678)
	return binary.BigEndian.AppendUint16(data, 5432)
}

func proxyTestTLV(length int) []byte {
	data := binary.BigEndian.AppendUint16([]byte{byte(proxyproto.PP2_TYPE_NOOP)}, uint16(length))
	return append(data, make([]byte, length)...)
}

// TestProxyProtocolParsing covers Multigateway's adapter contract.
//
// Wire fixtures follow section 2.2 of the PROXY protocol specification:
// https://github.com/haproxy/haproxy/blob/master/doc/proxy-protocol.txt
// The 4 KiB limit and strict LOCAL/UNSPEC expectations follow our pinned parser:
// https://github.com/pires/go-proxyproto/blob/v0.15.0/v2.go
// https://github.com/pires/go-proxyproto/blob/v0.15.0/fuzz_test.go
//
// Optional headers, proxy trust, TCP-only addresses, ALPN eligibility, and
// preservation of the following TLS bytes are Multigateway-specific assertions.
func TestProxyProtocolParsing(t *testing.T) {
	v4 := proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */))
	v6 := proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv6, proxyTestAddress(true /* ipv6 */))
	for _, tt := range []struct {
		name      string
		header    []byte
		proxyCIDR string
		remote    string
		wantError bool
	}{
		{"IPv4", v4, "127.0.0.1/32", "192.0.2.10:45678", false /* wantError */},
		{"IPv6", v6, "127.0.0.1/32", "[2001:db8::1]:45678", false /* wantError */},
		{"unrestricted IPv4", v4, "", "192.0.2.10:45678", false /* wantError */},
		{"unrestricted IPv6", v6, "", "[2001:db8::1]:45678", false /* wantError */},
		{"TLV", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, append(proxyTestAddress(false /* ipv6 */), byte(proxyproto.PP2_TYPE_NOOP), 0, 2, 1, 2)), "127.0.0.1/32", "192.0.2.10:45678", false /* wantError */},
		{"empty TLV", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, append(proxyTestAddress(false /* ipv6 */), proxyTestTLV(0)...)), "", "192.0.2.10:45678", false /* wantError */},
		{"multiple TLVs", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, append(append(proxyTestAddress(false /* ipv6 */), proxyTestTLV(2)...), proxyTestTLV(3)...)), "", "192.0.2.10:45678", false /* wantError */},
		{"maximum payload", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, append(proxyTestAddress(false /* ipv6 */), proxyTestTLV(4096-12-3)...)), "", "192.0.2.10:45678", false /* wantError */},
		{"oversized payload", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, append(proxyTestAddress(false /* ipv6 */), proxyTestTLV(4097-12-3)...)), "", "", true /* wantError */},
		{"headerless", nil, "127.0.0.1/32", "127.0.0.1:1234", false /* wantError */},
		{"unrestricted headerless", nil, "", "127.0.0.1:1234", false /* wantError */},
		{"headerless outside CIDRs", nil, "192.0.2.0/24", "127.0.0.1:1234", false /* wantError */},
		{"untrusted", v4, "192.0.2.0/24", "", true /* wantError */},
		{"LOCAL", proxyTestHeader(proxyproto.LOCAL, proxyproto.UNSPEC, nil), "127.0.0.1/32", "127.0.0.1:1234", false /* wantError */},
		{"LOCAL payload", proxyTestHeader(proxyproto.LOCAL, proxyproto.UNSPEC, []byte{1, 2, 3}), "", "127.0.0.1:1234", false /* wantError */},
		{"LOCAL address", proxyTestHeader(proxyproto.LOCAL, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */)), "", "127.0.0.1:1234", false /* wantError */},
		{"LOCAL empty address", proxyTestHeader(proxyproto.LOCAL, proxyproto.TCPv4, nil), "", "127.0.0.1:1234", false /* wantError */},
		{"LOCAL undefined family", proxyTestHeader(proxyproto.LOCAL, proxyTestUndefinedFamily, []byte{1, 2, 3}), "", "", true /* wantError */},
		{"UNSPEC", proxyTestHeader(proxyproto.PROXY, proxyproto.UNSPEC, nil), "127.0.0.1/32", "", true /* wantError */},
		{"version", proxyTestHeader(proxyTestInvalidVersion, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */)), "", "", true /* wantError */},
		{"command", proxyTestHeader(proxyTestInvalidCommand, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */)), "", "", true /* wantError */},
		{"UDP", proxyTestHeader(proxyproto.PROXY, proxyproto.UDPv4, proxyTestAddress(false /* ipv6 */)), "", "", true /* wantError */},
		{"UNIX", proxyTestHeader(proxyproto.PROXY, proxyproto.UnixStream, make([]byte, 216)), "", "", true /* wantError */},
		{"short address", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, []byte{1}), "", "", true /* wantError */},
		{"short TLV", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, append(proxyTestAddress(false /* ipv6 */), byte(proxyproto.PP2_TYPE_ALPN))), "", "", true /* wantError */},
		{"long TLV", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, append(proxyTestAddress(false /* ipv6 */), byte(proxyproto.PP2_TYPE_ALPN), 0, 8)), "", "", true /* wantError */},
		{"signature", []byte("\rxxxxxxxxxxxxxxx"), "", "", true /* wantError */},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, fragmented := range []bool{false, true} {
				// The next bytes can be coalesced with the header or arrive one at a time.
				wire := append(append([]byte(nil), tt.header...), 0x16, 3, 1)
				var reader io.Reader = bytes.NewReader(wire)
				if fragmented {
					reader = iotest.OneByteReader(reader)
				}
				l := &Listener{}
				if tt.proxyCIDR != "" {
					l.trustedProxyCIDRs = []netip.Prefix{netip.MustParsePrefix(tt.proxyCIDR)}
				}
				c := &Conn{
					listener: l, logger: testLogger(t), bufferedReader: bufio.NewReader(reader),
					conn: &proxyAddressConn{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}},
				}
				err := c.maybeHandleProxyProtocol()
				if tt.wantError {
					require.Error(t, err)
					require.True(t, c.proxyProtocolFailed)
					require.False(t, c.proxyClientAddress)
					require.False(t, c.canSendPlaintextStartupError())
					continue
				}
				require.NoError(t, err)
				require.Equal(t, tt.remote, c.RemoteAddr().String())
				require.Equal(t, tt.remote != "127.0.0.1:1234", c.proxyClientAddress)
				if c.proxyClientAddress {
					if tt.remote == "[2001:db8::1]:45678" {
						require.Equal(t, "[2001:db8::2]:5432", c.conn.LocalAddr().String())
					} else {
						require.Equal(t, "192.0.2.20:5432", c.conn.LocalAddr().String())
					}
				}
				tail, err := io.ReadAll(c.bufferedReader)
				require.NoError(t, err)
				require.Equal(t, []byte{0x16, 3, 1}, tail)
			}
		})
	}
	for n := 1; n < len(v4); n++ {
		c := &Conn{
			listener:       &Listener{trustedProxyCIDRs: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}},
			bufferedReader: bufio.NewReader(bytes.NewReader(v4[:n])),
			conn:           &proxyAddressConn{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}},
		}
		require.Error(t, c.maybeHandleProxyProtocol(), "truncated at %d", n)
	}
}

func TestProxyProtocolStartupErrors(t *testing.T) {
	for _, tt := range []struct {
		name      string
		reader    io.Reader
		proxyCIDR string
		wantCode  mtrpcpb.Code
		wantCause error
	}{
		{
			name: "untrusted peer", reader: bytes.NewReader(proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */))),
			proxyCIDR: "192.0.2.0/24", wantCode: mtrpcpb.Code_PERMISSION_DENIED,
		},
		{
			name: "unsupported transport", reader: bytes.NewReader(proxyTestHeader(proxyproto.PROXY, proxyproto.UDPv4, proxyTestAddress(false /* ipv6 */))),
			wantCode: mtrpcpb.Code_INVALID_ARGUMENT,
		},
		{
			name: "library header error", reader: bytes.NewReader(proxyTestHeader(proxyTestInvalidVersion, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */))),
			wantCode: mtrpcpb.Code_UNKNOWN, wantCause: proxyproto.ErrUnsupportedProtocolVersionAndCommand,
		},
		{
			name: "library TLV error", reader: bytes.NewReader(proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, append(proxyTestAddress(false /* ipv6 */), byte(proxyproto.PP2_TYPE_ALPN)))),
			wantCode: mtrpcpb.Code_UNKNOWN, wantCause: proxyproto.ErrTruncatedTLV,
		},
		{
			name: "disconnect", reader: iotest.ErrReader(io.EOF),
			wantCode: mtrpcpb.Code_UNKNOWN, wantCause: io.EOF,
		},
		{
			name: "startup deadline", reader: iotest.ErrReader(os.ErrDeadlineExceeded),
			wantCode: mtrpcpb.Code_UNKNOWN, wantCause: os.ErrDeadlineExceeded,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := &Listener{}
			if tt.proxyCIDR != "" {
				l.trustedProxyCIDRs = []netip.Prefix{netip.MustParsePrefix(tt.proxyCIDR)}
			}
			c := &Conn{
				listener: l, bufferedReader: bufio.NewReader(tt.reader),
				conn: &proxyAddressConn{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}},
			}
			err := c.handleStartup()
			require.ErrorContains(t, err, "PROXY protocol")
			require.Equal(t, tt.wantCode, mterrors.Code(err))
			if tt.wantCause != nil {
				require.ErrorIs(t, err, tt.wantCause)
			}
		})
	}
}

func TestProxyProtocolTLSOffload(t *testing.T) {
	v4 := proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */))
	v6 := proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv6, proxyTestAddress(true /* ipv6 */))
	for _, tt := range []struct {
		name                            string
		offload, classic, restrictProxy bool
		header                          []byte
		alpn                            []string
		success                         bool
	}{
		{"offloaded with CIDRs", true /* offload */, false /* classic */, true /* restrictProxy */, v4, nil, true /* success */},
		{"offloaded without CIDRs", true /* offload */, false /* classic */, false /* restrictProxy */, v4, nil, true /* success */},
		{"offloaded IPv6", true /* offload */, false /* classic */, false /* restrictProxy */, v6, nil, true /* success */},
		{"offloaded ALPN", true /* offload */, false /* classic */, false /* restrictProxy */, v4, []string{protocol.ALPNProtocol}, true /* success */},
		{"offload disabled", false /* offload */, false /* classic */, false /* restrictProxy */, v4, nil, false /* success */},
		{"direct no header", true /* offload */, false /* classic */, false /* restrictProxy */, nil, nil, false /* success */},
		{"direct ALPN", true /* offload */, false /* classic */, false /* restrictProxy */, nil, []string{protocol.ALPNProtocol}, true /* success */},
		{"classic direct", true /* offload */, true /* classic */, false /* restrictProxy */, nil, nil, true /* success */},
		{"classic proxied", true /* offload */, true /* classic */, false /* restrictProxy */, v4, nil, true /* success */},
		{"wrong negotiated ALPN", true /* offload */, false /* classic */, false /* restrictProxy */, v4, []string{"h2"}, false /* success */},
		{"LOCAL requires ALPN", true /* offload */, false /* classic */, false /* restrictProxy */, proxyTestHeader(proxyproto.LOCAL, proxyproto.UNSPEC, nil), nil, false /* success */},
		{"LOCAL address requires ALPN", true /* offload */, false /* classic */, false /* restrictProxy */, proxyTestHeader(proxyproto.LOCAL, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */)), nil, false /* success */},
		{"LOCAL ALPN", true /* offload */, false /* classic */, false /* restrictProxy */, proxyTestHeader(proxyproto.LOCAL, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */)), []string{protocol.ALPNProtocol}, true /* success */},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tlsConfig, ca := generateTestTLSConfig(t)
			tlsConfig.NextProtos = []string{protocol.ALPNProtocol, "h2"}
			var serverConn *Conn
			ln, errCh := startDirectTLSServer(t, tlsConfig, func(c *Conn) {
				serverConn = c
				c.listener.tlsOffload = tt.offload
				if tt.restrictProxy {
					c.listener.trustedProxyCIDRs = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
				}
			})
			raw, err := net.Dial("tcp", ln.Addr().String())
			require.NoError(t, err)
			defer raw.Close()
			require.NoError(t, raw.SetDeadline(time.Now().Add(5*time.Second)))
			if tt.header != nil {
				_, err = raw.Write(tt.header)
				require.NoError(t, err)
			}
			if tt.classic {
				writeSSLRequest(t, raw)
				require.Equal(t, byte('S'), readSingleByte(t, raw))
			}
			conn := tls.Client(raw, &tls.Config{RootCAs: ca, ServerName: "localhost", MinVersion: tls.VersionTLS12, NextProtos: tt.alpn})
			require.NoError(t, conn.Handshake())
			if tt.success {
				writeStartupPacketToPipe(t, conn, protocol.ProtocolVersionNumber, map[string]string{"user": "proxy_user", "database": "proxy_db"})
				scramClientHelper(t, conn, "proxy_user", "postgres")
				require.NoError(t, <-errCh)
				if bytes.Equal(tt.header, v4) {
					require.Equal(t, "192.0.2.10:45678", serverConn.RemoteAddr().String())
					require.Equal(t, "192.0.2.20:5432", serverConn.conn.LocalAddr().String())
				} else if bytes.Equal(tt.header, v6) {
					require.Equal(t, "[2001:db8::1]:45678", serverConn.RemoteAddr().String())
					require.Equal(t, "[2001:db8::2]:5432", serverConn.conn.LocalAddr().String())
				} else {
					require.Equal(t, raw.LocalAddr().String(), serverConn.RemoteAddr().String())
					require.False(t, serverConn.proxyClientAddress)
				}
			} else {
				require.ErrorContains(t, <-errCh, "without ALPN")
			}
		})
	}
}

func TestProxyProtocolTrustedPeers(t *testing.T) {
	for _, tt := range []struct {
		name    string
		cidrs   []string
		allowed []string
		denied  []string
	}{
		{
			name:    "IPv4 prefix",
			cidrs:   []string{"192.0.2.0/24"},
			allowed: []string{"192.0.2.10", "::ffff:192.0.2.10"},
			denied:  []string{"192.0.3.10", "2001:db8::1"},
		},
		{
			name:    "mapped IPv4 prefix with host bits",
			cidrs:   []string{"::ffff:192.0.2.129/120"},
			allowed: []string{"192.0.2.0", "192.0.2.255", "::ffff:192.0.2.10"},
			denied:  []string{"192.0.1.255", "192.0.3.0", "2001:db8::1"},
		},
		{
			name:    "mapped IPv4 host",
			cidrs:   []string{"::ffff:192.0.2.10/128"},
			allowed: []string{"192.0.2.10", "::ffff:192.0.2.10"},
			denied:  []string{"192.0.2.9", "192.0.2.11", "2001:db8::1"},
		},
		{
			name:    "entire mapped IPv4 range",
			cidrs:   []string{"::ffff:0:0/96"},
			allowed: []string{"0.0.0.0", "255.255.255.255", "::ffff:192.0.2.10"},
			denied:  []string{"::1", "2001:db8::1"},
		},
		{
			name:    "broader prefix stays IPv6",
			cidrs:   []string{"::ffff:192.0.2.10/95"},
			allowed: []string{"::fffe:0:1"},
			denied:  []string{"192.0.2.10", "::ffff:192.0.2.10", "::1"},
		},
		{
			name:    "native IPv6 prefix",
			cidrs:   []string{"2001:db8::1234/64"},
			allowed: []string{"2001:db8::1", "2001:db8::ffff"},
			denied:  []string{"2001:db8:0:1::1", "192.0.2.10", "::ffff:192.0.2.10"},
		},
		{
			name:    "mixed prefixes",
			cidrs:   []string{"198.51.100.0/24", "::ffff:192.0.2.0/120", "2001:db8::/64"},
			allowed: []string{"198.51.100.10", "192.0.2.10", "::ffff:192.0.2.10", "2001:db8::1"},
			denied:  []string{"203.0.113.10", "2001:db8:0:1::1"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l, err := NewListener(ListenerConfig{
				Address: "127.0.0.1:0", Handler: &mockHandler{},
				CredentialProvider: newMockCredentialProvider("postgres"),
				TrustedProxyCIDRs:  tt.cidrs,
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, l.Close()) })
			for _, peer := range tt.allowed {
				t.Run("allowed/"+peer, func(t *testing.T) {
					require.True(t, l.trustsProxy(netip.MustParseAddr(peer)))
				})
			}
			for _, peer := range tt.denied {
				t.Run("denied/"+peer, func(t *testing.T) {
					require.False(t, l.trustsProxy(netip.MustParseAddr(peer)))
				})
			}
		})
	}
}

func TestProxyProtocolConfiguration(t *testing.T) {
	for _, cfg := range []ListenerConfig{
		{TrustedProxyCIDRs: []string{"invalid"}},
		{TLSOffload: true},
		{TLSOffload: true, TLSConfig: &tls.Config{}, TrustedProxyCIDRs: []string{"invalid"}},
		{TLSOffload: true, TrustedProxyCIDRs: []string{"127.0.0.1/32"}},
	} {
		cfg.Handler = &mockHandler{}
		cfg.CredentialProvider = newMockCredentialProvider("postgres")
		_, err := NewListener(cfg)
		require.Error(t, err)
	}
	l, err := NewListener(ListenerConfig{
		Address: "127.0.0.1:0", Handler: &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"),
		TLSOffload:         true, TLSConfig: &tls.Config{},
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())
}

func TestProxyProtocolStartupDeadline(t *testing.T) {
	l, err := NewListener(ListenerConfig{
		Address: "127.0.0.1:0", Handler: &mockHandler{},
		CredentialProvider: newMockCredentialProvider("postgres"), AuthenticationTimeout: 100 * time.Millisecond,
		TrustedProxyCIDRs: []string{"127.0.0.1/32"}, Logger: testLogger(t),
	})
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() { serveErr <- l.Serve() }()
	t.Cleanup(func() {
		require.NoError(t, l.Close())
		require.NoError(t, <-serveErr)
	})
	for _, tt := range []struct {
		name           string
		header         []byte
		wantErrorReply bool
	}{
		{"incomplete header", []byte("\r\n"), false /* wantErrorReply */},
		{"complete header then stalled startup", proxyTestHeader(proxyproto.PROXY, proxyproto.TCPv4, proxyTestAddress(false /* ipv6 */)), true /* wantErrorReply */},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := net.Dial("tcp", l.Addr().String())
			require.NoError(t, err)
			defer raw.Close()
			require.NoError(t, raw.SetDeadline(time.Now().Add(3*time.Second)))
			_, err = raw.Write(tt.header)
			require.NoError(t, err)
			if tt.wantErrorReply {
				// A complete header returns to normal PostgreSQL startup. The
				// existing deadline must still terminate a stalled client.
				msgType, body := readMessage(t, raw)
				require.Equal(t, byte(protocol.MsgErrorResponse), msgType)
				require.True(t, containsErrField(body, 'C', "08006"))
				require.True(t, containsErrField(body, 'M', "canceling authentication due to timeout"))
			}
			var reply [1]byte
			n, err := raw.Read(reply[:])
			require.Zero(t, n, "must close without an additional reply")
			require.ErrorIs(t, err, io.EOF)
		})
	}
}
