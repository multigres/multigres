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
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

var proxyV2Signature = []byte("\r\n\r\n\x00\r\nQUIT\n")

func proxyTestHeader(command, family byte, payload []byte) []byte {
	header := append([]byte(nil), proxyV2Signature...)
	header = append(header, command, family, byte(len(payload)>>8), byte(len(payload)))
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
	data := binary.BigEndian.AppendUint16([]byte{4}, uint16(length))
	return append(data, make([]byte, length)...)
}

func TestProxyProtocolParsing(t *testing.T) {
	v4 := proxyTestHeader(0x21, 0x11, proxyTestAddress(false))
	v6 := proxyTestHeader(0x21, 0x21, proxyTestAddress(true))
	for _, tt := range []struct {
		name      string
		header    []byte
		proxyCIDR string
		remote    string
		wantError bool
	}{
		{"IPv4", v4, "127.0.0.1/32", "192.0.2.10:45678", false},
		{"IPv6", v6, "127.0.0.1/32", "[2001:db8::1]:45678", false},
		{"unrestricted IPv4", v4, "", "192.0.2.10:45678", false},
		{"unrestricted IPv6", v6, "", "[2001:db8::1]:45678", false},
		{"TLV", proxyTestHeader(0x21, 0x11, append(proxyTestAddress(false), 4, 0, 2, 1, 2)), "127.0.0.1/32", "192.0.2.10:45678", false},
		{"empty TLV", proxyTestHeader(0x21, 0x11, append(proxyTestAddress(false), proxyTestTLV(0)...)), "", "192.0.2.10:45678", false},
		{"multiple TLVs", proxyTestHeader(0x21, 0x11, append(append(proxyTestAddress(false), proxyTestTLV(2)...), proxyTestTLV(3)...)), "", "192.0.2.10:45678", false},
		{"maximum payload", proxyTestHeader(0x21, 0x11, append(proxyTestAddress(false), proxyTestTLV(4096-12-3)...)), "", "192.0.2.10:45678", false},
		{"oversized payload", proxyTestHeader(0x21, 0x11, append(proxyTestAddress(false), proxyTestTLV(4097-12-3)...)), "", "", true},
		{"headerless", nil, "127.0.0.1/32", "127.0.0.1:1234", false},
		{"unrestricted headerless", nil, "", "127.0.0.1:1234", false},
		{"headerless outside CIDRs", nil, "192.0.2.0/24", "127.0.0.1:1234", false},
		{"untrusted", v4, "192.0.2.0/24", "", true},
		{"LOCAL", proxyTestHeader(0x20, 0, nil), "127.0.0.1/32", "127.0.0.1:1234", false},
		{"LOCAL payload", proxyTestHeader(0x20, 0, []byte{1, 2, 3}), "", "127.0.0.1:1234", false},
		{"LOCAL address", proxyTestHeader(0x20, 0x11, proxyTestAddress(false)), "", "127.0.0.1:1234", false},
		{"LOCAL empty address", proxyTestHeader(0x20, 0x11, nil), "", "127.0.0.1:1234", false},
		{"LOCAL undefined family", proxyTestHeader(0x20, 0xff, []byte{1, 2, 3}), "", "", true},
		{"UNSPEC", proxyTestHeader(0x21, 0, nil), "127.0.0.1/32", "", true},
		{"version", proxyTestHeader(0x31, 0x11, proxyTestAddress(false)), "", "", true},
		{"command", proxyTestHeader(0x22, 0x11, proxyTestAddress(false)), "", "", true},
		{"UDP", proxyTestHeader(0x21, 0x12, proxyTestAddress(false)), "", "", true},
		{"UNIX", proxyTestHeader(0x21, 0x31, make([]byte, 216)), "", "", true},
		{"short address", proxyTestHeader(0x21, 0x11, []byte{1}), "", "", true},
		{"short TLV", proxyTestHeader(0x21, 0x11, append(proxyTestAddress(false), 1)), "", "", true},
		{"long TLV", proxyTestHeader(0x21, 0x11, append(proxyTestAddress(false), 1, 0, 8)), "", "", true},
		{"signature", []byte("\rxxxxxxxxxxxxxxx"), "", "", true},
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

func TestProxyProtocolTLSOffload(t *testing.T) {
	v4 := proxyTestHeader(0x21, 0x11, proxyTestAddress(false))
	v6 := proxyTestHeader(0x21, 0x21, proxyTestAddress(true))
	for _, tt := range []struct {
		name                            string
		offload, classic, restrictProxy bool
		header                          []byte
		alpn                            []string
		success                         bool
	}{
		{"offloaded with CIDRs", true, false, true, v4, nil, true},
		{"offloaded without CIDRs", true, false, false, v4, nil, true},
		{"offloaded IPv6", true, false, false, v6, nil, true},
		{"offloaded ALPN", true, false, false, v4, []string{protocol.ALPNProtocol}, true},
		{"offload disabled", false, false, false, v4, nil, false},
		{"direct no header", true, false, false, nil, nil, false},
		{"direct ALPN", true, false, false, nil, []string{protocol.ALPNProtocol}, true},
		{"classic direct", true, true, false, nil, nil, true},
		{"classic proxied", true, true, false, v4, nil, true},
		{"wrong negotiated ALPN", true, false, false, v4, []string{"h2"}, false},
		{"LOCAL requires ALPN", true, false, false, proxyTestHeader(0x20, 0, nil), nil, false},
		{"LOCAL address requires ALPN", true, false, false, proxyTestHeader(0x20, 0x11, proxyTestAddress(false)), nil, false},
		{"LOCAL ALPN", true, false, false, proxyTestHeader(0x20, 0x11, proxyTestAddress(false)), []string{protocol.ALPNProtocol}, true},
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
		{"incomplete header", []byte("\r\n"), false},
		{"complete header then stalled startup", proxyTestHeader(0x21, 0x11, proxyTestAddress(false)), true},
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
