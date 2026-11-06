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
	"net"
	"net/netip"

	"github.com/pires/go-proxyproto"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// maybeHandleProxyProtocol runs once, before TLS detection, under the startup
// deadline. Peeking one byte preserves headerless clients and avoids waiting
// for a full signature when a client sends an eight-byte SSLRequest.
func (c *Conn) maybeHandleProxyProtocol() (err error) {
	first, err := c.bufferedReader.Peek(1)
	if err != nil {
		return err
	}
	if first[0] != proxyproto.SIGV2[0] {
		return nil
	}
	defer func() { c.proxyProtocolFailed = err != nil }()
	peer, ok := c.conn.RemoteAddr().(*net.TCPAddr)
	if !ok || !c.listener.trustsProxy(peer.AddrPort().Addr()) {
		return mterrors.New(mtrpcpb.Code_PERMISSION_DENIED, "header from untrusted peer")
	}
	// Read uses the existing buffer and authentication deadline. The listener
	// wrapper's lazy parsing and separate header timeout do not fit this startup
	// sequence. The library owns framing, length limits, and address decoding.
	header, err := proxyproto.Read(c.bufferedReader)
	if err != nil {
		return err
	}
	if header.Command == proxyproto.LOCAL {
		// LOCAL carries no authoritative address or offload permission.
		return nil
	}
	remote, local, ok := header.TCPAddrs()
	if !ok {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "unsupported v2 address family or transport")
	}
	// Read retains optional metadata as raw bytes. Validate its framing before
	// accepting the address, without using metadata for authentication.
	if _, err := header.TLVs(); err != nil {
		return err
	}
	c.conn = &proxyAddressConn{Conn: c.conn, remote: remote, local: local}
	c.proxyClientAddress = true
	c.logger.Debug("accepted PROXY v2 client address", "proxy_addr", peer, "remote_addr", remote)
	return nil
}

func (l *Listener) trustsProxy(addr netip.Addr) bool {
	if len(l.trustedProxyCIDRs) == 0 {
		return true
	}
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
