// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package latencydiag

import (
	"crypto/tls"
	"maps"
	"net"
	"time"
)

// SocketSample is a periodic snapshot, not packet timestamps. Endpoint identity
// is essential when the immediate peer is an Envoy/load balancer, not a driver.
type socketKey struct {
	role string
	id   uint32
}

type SocketSample struct {
	Role       string `json:"role"`
	TimeNS     int64  `json:"time_ns"`
	Connection uint32 `json:"connection"`
	Local      string `json:"local"`
	Peer       string `json:"peer"`
	Info       any    `json:"tcp_info,omitempty"`
	Status     string `json:"status"`
}

// ObserveSocket registers a sampled frontend connection, capped at 256 per
// capture. All TCP_INFO syscalls run in the collector, never the query path.
func ObserveSocket(s *Span, connection uint32, conn net.Conn) {
	observeSocket(s, socketKey{"frontend", connection}, conn)
}

func ObserveBackendSocket(s *Span, connection uint32, conn net.Conn) {
	observeSocket(s, socketKey{"postgres", connection}, conn)
}

func observeSocket(s *Span, key socketKey, conn net.Conn) {
	if s == nil || conn == nil || !s.capture.opts.Sockets {
		return
	}
	c := s.capture
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if c.sockets == nil {
		c.sockets = make(map[socketKey]net.Conn)
	}
	if len(c.sockets) >= 256 {
		if _, ok := c.sockets[key]; !ok {
			c.socketOmitted++
			return
		}
	}
	c.sockets[key] = conn
}

func (c *Capture) socketSamples() []SocketSample {
	c.mu.Lock()
	conns := maps.Clone(c.sockets)
	c.mu.Unlock()
	samples := make([]SocketSample, 0, len(conns))
	for id, conn := range conns {
		sample := SocketSample{TimeNS: time.Since(c.start).Nanoseconds(), Connection: id.id, Role: id.role, Local: conn.LocalAddr().String(), Peer: conn.RemoteAddr().String()}
		if tlsConn, ok := conn.(*tls.Conn); ok {
			conn = tlsConn.NetConn()
		}
		sample.Info, sample.Status = tcpInfo(conn)
		samples = append(samples, sample)
	}
	return samples
}
