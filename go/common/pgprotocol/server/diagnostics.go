// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"

	"github.com/multigres/multigres/go/tools/latencydiag"
)

func (c *Conn) diagnosticContext() context.Context {
	if s := c.diagnostic.Load(); s != nil {
		return s.Context()
	}
	if c.ctx == nil {
		return context.TODO()
	}
	return c.ctx
}

// diagnosticTransport sits below bufio and above TLS (when enabled). It
// includes implicit flushes on buffer overflow. A successful Write only means
// the local transport accepted the bytes, not that the peer received them.
type diagnosticTransport struct{ owner *Conn }

func (t *diagnosticTransport) Read(p []byte) (int, error) {
	if t.owner.diagnostic.Load() == nil {
		return t.owner.conn.Read(p)
	}
	_, s := latencydiag.Start(t.owner.diagnosticContext(), "frontend.transport_read")
	n, err := t.owner.conn.Read(p)
	s.End(err, n)
	return n, err
}

func (t *diagnosticTransport) Write(p []byte) (int, error) {
	if t.owner.diagnostic.Load() == nil {
		return t.owner.conn.Write(p)
	}
	_, s := latencydiag.Start(t.owner.diagnosticContext(), "frontend.transport_write")
	n, err := t.owner.conn.Write(p)
	s.End(err, n)
	return n, err
}

func (c *Conn) ReadMessageType() (byte, error) {
	if c.diagnostic.Load() == nil {
		return c.bufferedReader.ReadByte()
	}
	return c.readMessageTypeDiagnostic()
}

func (c *Conn) readMessageTypeDiagnostic() (byte, error) {
	_, s := latencydiag.Start(c.diagnosticContext(), "frontend.read_type")
	b, err := c.readMessageTypeUntraced()
	s.End(err, 0)
	return b, err
}

func (c *Conn) ReadMessageLength() (int, error) {
	if c.diagnostic.Load() == nil {
		return c.readMessageLengthUntraced()
	}
	return c.readMessageLengthDiagnostic()
}

func (c *Conn) readMessageLengthDiagnostic() (int, error) {
	_, s := latencydiag.Start(c.diagnosticContext(), "frontend.read_length")
	n, err := c.readMessageLengthUntraced()
	s.End(err, 0)
	return n, err
}

func (c *Conn) readMessageBody(length int) ([]byte, error) {
	if c.diagnostic.Load() == nil {
		return c.readMessageBodyUntraced(length)
	}
	return c.readMessageBodyDiagnostic(length)
}

func (c *Conn) readMessageBodyDiagnostic(length int) ([]byte, error) {
	_, s := latencydiag.Start(c.diagnosticContext(), "frontend.read_body")
	b, err := c.readMessageBodyUntraced(length)
	s.End(err, len(b))
	return b, err
}

func (c *Conn) flush() error {
	if c.diagnostic.Load() == nil {
		return c.flushUntraced()
	}
	return c.flushDiagnostic()
}

func (c *Conn) flushDiagnostic() error {
	_, s := latencydiag.Start(c.diagnosticContext(), "frontend.flush")
	err := c.flushUntraced()
	s.End(err, 0)
	return err
}

func (c *Conn) endWriterBuffering() error {
	if c.diagnostic.Load() == nil {
		return c.endWriterBufferingUntraced()
	}
	return c.endWriterBufferingDiagnostic()
}

func (c *Conn) endWriterBufferingDiagnostic() error {
	_, s := latencydiag.Start(c.diagnosticContext(), "frontend.flush_release")
	err := c.endWriterBufferingUntraced()
	s.End(err, 0)
	return err
}

func (c *Conn) handleMessage(kind byte) error {
	if c.diagnostic.Load() == nil {
		return c.handleMessageUntraced(kind)
	}
	return c.handleMessageDiagnostic(kind)
}

func (c *Conn) handleMessageDiagnostic(kind byte) error {
	parent := c.diagnostic.Load()
	_, s := latencydiag.Start(c.diagnosticContext(), "frontend.dispatch")
	if s != nil {
		c.diagnostic.Store(s)
		defer c.diagnostic.Store(parent)
	}
	err := c.handleMessageUntraced(kind)
	s.End(err, 0)
	return err
}
