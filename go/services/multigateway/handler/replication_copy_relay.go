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

package handler

import (
	"errors"
	"io"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// errReplicationSessionResumed is returned by copyModeDownstreamReader once
// a ReadyForQuery message has been fully relayed to the client: postgres has
// returned to normal command mode after the COPY session ended. Multigres
// does not support resuming command mode on a replication connection — a
// survey of the most widely used open-source software that consumes
// PostgreSQL logical replication found none that do this either — so
// seeing this signals the caller to tear the connection down rather than
// continue relaying.
var errReplicationSessionResumed = errors.New("replication session returned to command mode; connection reuse is not supported")

// copyModeDownstreamReader relays postgres's responses to the client during
// an active COPY-BOTH replication session. Its next() method matches the
// RecvFunc shape (commonrepl.RecvFunc) so it plugs directly into the
// existing, unmodified commonrepl.Tunnel — see replication_stream.go.
//
// CopyData bodies are never accumulated further than what's already been
// delivered: logical replication's CopyData frames are not size-bounded (a
// single large row change can be one multi-megabyte frame), so waiting for
// a whole frame before forwarding any of it would spike memory and add real
// latency versus today's incremental byte-blind relay. A next() call
// returns whatever's already buffered for
// the current frame (header plus as much of the body as has already
// arrived — all of it, if it arrived together) and never blocks on
// recv() for more just to fill out the rest of the body; any remaining
// body bytes are relayed as later recv() calls deliver them. Every other
// message type is small (control messages: CommandComplete, ErrorResponse,
// NoticeResponse, ParameterStatus, ReadyForQuery, ...) and is buffered
// whole, same as pgMsgReader already does for the preamble.
type copyModeDownstreamReader struct {
	recv func() ([]byte, error)
	buf  []byte // unconsumed bytes not yet classified into a message

	passthroughRemaining int // bytes left in the in-flight CopyData body to relay verbatim
}

// next returns the next chunk to forward to the client. Matches
// commonrepl.RecvFunc: a non-nil error means no more chunks will come, but
// any returned bytes must still be forwarded first (mirrors io.Reader's
// "process n>0 before considering err" contract, which the underlying
// recv()/net.Conn read already honors).
func (r *copyModeDownstreamReader) next() ([]byte, error) {
	if r.passthroughRemaining > 0 {
		return r.readPassthrough()
	}
	return r.readNextMessage()
}

// readPassthrough returns up to passthroughRemaining bytes of an in-flight
// CopyData body, pulling more from recv() only once buf is empty. Never
// accumulates the whole body — just relays whatever's already available,
// bounded by what's left to deliver for this message. If recv() returns
// bytes alongside a terminal error (e.g. io.EOF), those bytes are used now;
// the error resurfaces on a later call once recv() is asked again and
// returns nothing.
func (r *copyModeDownstreamReader) readPassthrough() ([]byte, error) {
	if len(r.buf) == 0 {
		chunk, err := r.recv()
		if len(chunk) == 0 {
			return nil, err
		}
		r.buf = chunk
	}
	n := min(len(r.buf), r.passthroughRemaining)
	out := r.buf[:n]
	r.buf = r.buf[n:]
	r.passthroughRemaining -= n
	return out, nil
}

// readNextMessage reassembles the next message's 5-byte header, then either
// returns the CopyData header plus whatever body bytes are already
// buffered (deferring any remainder to readPassthrough as it arrives), or
// fully buffers a control message's small body and classifies it.
func (r *copyModeDownstreamReader) readNextMessage() ([]byte, error) {
	if err := r.ensureBuffered(5); err != nil {
		return nil, err
	}
	msgType, bodyLen, err := parsePgMessageHeader(r.buf[:5], "backend")
	if err != nil {
		return nil, err
	}

	if msgType == protocol.MsgCopyData {
		// Forward the header plus however much of the body is already
		// sitting in buf (possibly none, possibly all of it) in a single
		// chunk — no reason to artificially split bytes we already have.
		// Whatever's left (if any) is relayed via readPassthrough as
		// later recv() calls deliver it.
		bodyAvail := min(bodyLen, len(r.buf)-5)
		total := 5 + bodyAvail
		raw := append([]byte(nil), r.buf[:total]...)
		r.buf = r.buf[total:]
		r.passthroughRemaining = bodyLen - bodyAvail
		return raw, nil
	}

	total := 5 + bodyLen
	if err := r.ensureBuffered(total); err != nil {
		return nil, err
	}
	raw := append([]byte(nil), r.buf[:total]...)
	r.buf = r.buf[total:]
	if msgType == protocol.MsgReadyForQuery {
		return raw, errReplicationSessionResumed
	}
	return raw, nil
}

// ensureBuffered pulls chunks from recv() until r.buf holds at least n
// bytes. See the package-level ensureBuffered for the byte-before-error
// contract this honors.
func (r *copyModeDownstreamReader) ensureBuffered(n int) error {
	return ensureBuffered(&r.buf, r.recv, n)
}

// errReplicationSessionEndedByClient is returned by copyModeUpstreamReader
// once a non-CopyData message (CopyDone, CopyFail, or — defensively —
// anything else) has been fully relayed: the client is exiting the COPY
// session. Multigres forwards that message (postgres needs to see it to end
// its own WalSndLoop cleanly) but never relays anything the client sends
// afterward, closing the pipelining loophole where a client sends a
// follow-up command in the same write as CopyDone without waiting for a
// reply.
var errReplicationSessionEndedByClient = errors.New("client ended the replication COPY session; connection reuse is not supported")

// copyModeUpstreamReader wraps the detached client socket and implements
// io.Reader, forwarding CopyData/CopyDone/CopyFail messages to the pooler
// stream during an active COPY-BOTH replication session. Client-sent
// messages are always small (StandbyStatusUpdate, HotStandbyFeedback,
// CopyDone, CopyFail — never large payloads), so unlike
// copyModeDownstreamReader this always buffers a whole message before
// returning it; no streaming passthrough is needed on this side.
type copyModeUpstreamReader struct {
	src     io.Reader
	buf     []byte     // unread bytes from src, not yet classified into a message
	pending []byte     // current message's bytes not yet copied out via Read
	err     error      // sticky terminal error, delivered once pending is drained
	scratch [4096]byte // reused read buffer for ensureBuffered; contents never need to survive past a single call
}

func newCopyModeUpstreamReader(src io.Reader) *copyModeUpstreamReader {
	return &copyModeUpstreamReader{src: src}
}

// Read implements io.Reader. Once a non-CopyData message has been fully
// delivered, every subsequent call returns errReplicationSessionEndedByClient
// without reading anything further from src — so bytes already sitting in
// src's buffer (a pipelined follow-up command) are never even inspected,
// let alone forwarded.
func (r *copyModeUpstreamReader) Read(p []byte) (int, error) {
	if len(r.pending) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if err := r.fillPending(); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	if len(r.pending) == 0 && r.err != nil {
		return n, r.err
	}
	return n, nil
}

// fillPending reassembles exactly one complete frontend message into
// r.pending, and sets r.err (delivered once r.pending is fully copied out)
// if that message signals the end of the COPY session.
func (r *copyModeUpstreamReader) fillPending() error {
	if err := r.ensureBuffered(5); err != nil {
		return err
	}
	msgType, bodyLen, err := parsePgMessageHeader(r.buf[:5], "frontend")
	if err != nil {
		return err
	}
	total := 5 + bodyLen
	if err := r.ensureBuffered(total); err != nil {
		return err
	}
	r.pending = append([]byte(nil), r.buf[:total]...)
	r.buf = r.buf[total:]
	if msgType != protocol.MsgCopyData {
		r.err = errReplicationSessionEndedByClient
	}
	return nil
}

// ensureBuffered pulls from src until r.buf holds at least n bytes. See the
// package-level ensureBuffered for the byte-before-error contract this
// honors; src.Read is adapted to that shape via scratch, which is reused
// across calls rather than allocated fresh each time.
func (r *copyModeUpstreamReader) ensureBuffered(n int) error {
	return ensureBuffered(&r.buf, func() ([]byte, error) {
		rn, err := r.src.Read(r.scratch[:])
		return r.scratch[:rn], err
	}, n)
}
