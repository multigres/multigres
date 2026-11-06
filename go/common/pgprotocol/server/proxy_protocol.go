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
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
)

var proxyV2Signature = []byte("\r\n\r\n\x00\r\nQUIT\n")

// maybeHandleProxyProtocol runs once, before TLS detection, under the startup
// deadline. Peeking one byte preserves headerless clients and avoids waiting
// for a full signature when a client sends an eight-byte SSLRequest.
func (c *Conn) maybeHandleProxyProtocol() (err error) {
	first, err := c.bufferedReader.Peek(1)
	if err != nil {
		return err
	}
	if first[0] != proxyV2Signature[0] {
		return nil
	}
	defer func() { c.proxyProtocolFailed = err != nil }()
	peer, ok := c.conn.RemoteAddr().(*net.TCPAddr)
	if !ok || !c.listener.trustsProxy(peer.AddrPort().Addr()) {
		return errors.New("header from untrusted peer")
	}
	var header [16]byte
	if _, err := io.ReadFull(c.bufferedReader, header[:]); err != nil {
		return err
	}
	if !bytes.Equal(header[:12], proxyV2Signature) || header[12]>>4 != 2 {
		return errors.New("invalid v2 header")
	}
	length := int(binary.BigEndian.Uint16(header[14:16]))
	switch header[12] & 0x0f {
	case 0: // LOCAL carries no authoritative address or offload permission.
		_, err := c.bufferedReader.Discard(length)
		return err
	case 1: // PROXY
	default:
		return errors.New("invalid v2 command")
	}
	var ipLength int
	switch header[13] {
	case 0x00: // UNSPEC has no client address.
		_, err := c.bufferedReader.Discard(length)
		return err
	case 0x11: // TCP over IPv4
		ipLength = net.IPv4len
	case 0x21: // TCP over IPv6
		ipLength = net.IPv6len
	default:
		return errors.New("unsupported v2 address family or transport")
	}
	addressLength := 2*ipLength + 4
	if length < addressLength {
		return errors.New("truncated v2 address")
	}
	var address [36]byte
	if _, err := io.ReadFull(c.bufferedReader, address[:addressLength]); err != nil {
		return err
	}
	// Skip bounded TLVs without allocating the declared payload length. Reject
	// truncated TLVs instead of consuming bytes from the following TLS record.
	remaining := length - addressLength
	for remaining > 0 {
		if remaining < 3 {
			return errors.New("truncated v2 TLV")
		}
		var tlv [3]byte
		if _, err := io.ReadFull(c.bufferedReader, tlv[:]); err != nil {
			return err
		}
		n := int(binary.BigEndian.Uint16(tlv[1:]))
		remaining -= 3
		if n > remaining {
			return errors.New("invalid v2 TLV length")
		}
		if _, err := c.bufferedReader.Discard(n); err != nil {
			return err
		}
		remaining -= n
	}
	remote := &net.TCPAddr{IP: append(net.IP(nil), address[:ipLength]...), Port: int(binary.BigEndian.Uint16(address[2*ipLength:]))}
	local := &net.TCPAddr{IP: append(net.IP(nil), address[ipLength:2*ipLength]...), Port: int(binary.BigEndian.Uint16(address[2*ipLength+2:]))}
	c.conn = &proxyAddressConn{Conn: c.conn, remote: remote, local: local}
	c.proxyClientAddress = true
	c.logger.Debug("accepted PROXY v2 client address", "proxy_addr", peer, "remote_addr", remote)
	return nil
}

func (l *Listener) trustsProxy(addr netip.Addr) bool {
	for _, prefix := range l.trustedProxyCIDRs {
		if prefix.Contains(addr.Unmap()) {
			return true
		}
	}
	return false
}

// proxyAddressConn preserves addresses through the subsequent TLS wrapper.
type proxyAddressConn struct {
	net.Conn
	remote, local net.Addr
}

func (c *proxyAddressConn) RemoteAddr() net.Addr { return c.remote }
func (c *proxyAddressConn) LocalAddr() net.Addr  { return c.local }
