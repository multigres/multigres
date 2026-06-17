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

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// replStreamState tracks where a connection is in the copy-both replication
// lifecycle. It guards the write path: a Standby Status Update may only be
// sent while the stream is live.
type replStreamState int

const (
	replStreamIdle      replStreamState = iota // not in copy-both
	replStreamStreaming                        // CopyBothResponse received; bytes flow both ways
	replStreamDead                             // server ErrorResponse or CopyDone observed; no more sends
)

// StartReplication issues a START_REPLICATION command and reads until the
// server sends CopyBothResponse ('W'), at which point the connection is in
// copy-both streaming mode. Interleaved NoticeResponse diagnostics are
// collected and returned to the caller (never silently dropped — they carry
// operational signal such as slot/WAL warnings); ParameterStatus updates are
// recorded into the connection's server params. On ErrorResponse (e.g. no such
// slot, max_wal_senders exhausted) the PG error is parsed, the trailing
// ReadyForQuery is drained so the socket stays reusable, and the error is
// returned alongside any notices seen before the failure.
//
// cmd is passed through verbatim — the caller is responsible for its contents.
// Multigres does not parse or validate the replication command.
//
// Call with a deadline-bearing or cancelable ctx: the read of the server's
// response is bounded by ctx (an absolute deadline applied across the whole
// handshake, so a server that stalls mid-response is still bounded). An
// unbounded ctx (context.Background with no deadline) will block indefinitely
// against an unhealthy server that accepts the command but never replies.
func (c *Conn) StartReplication(ctx context.Context, cmd string) ([]*mterrors.PgDiagnostic, error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	var notices []*mterrors.PgDiagnostic

	if err := c.writeQueryMessage(cmd); err != nil {
		return notices, fmt.Errorf("send START_REPLICATION: %w", err)
	}

	stop := c.makeReadsCancelable(ctx)
	defer stop()

	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return notices, ctxErr
			}
			return notices, fmt.Errorf("read START_REPLICATION response: %w", err)
		}

		switch msgType {
		case protocol.MsgCopyBothResponse:
			// Body is format byte + column formats; opaque to this layer.
			c.replState = replStreamStreaming
			return notices, nil

		case protocol.MsgErrorResponse:
			// The PG error (pgErr) is the meaningful failure we return. We then
			// drain the trailing ReadyForQuery best-effort so the socket is left
			// clean and reusable; its return is intentionally ignored — a drain
			// failure means the conn is broken, which the caller discovers on
			// next use, and pgErr remains the more informative error to surface.
			pgErr := c.parseError(body)
			_ = c.waitForReadyForQuery(ctx)
			return notices, pgErr

		case protocol.MsgNoticeResponse:
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgParameterStatus:
			// A parse failure here means a malformed ParameterStatus body —
			// real postgres never sends one, so it signals wire corruption.
			// Surface it rather than silently failing to record the param.
			if err := c.handleParameterStatus(body); err != nil {
				return notices, fmt.Errorf("malformed ParameterStatus during START_REPLICATION: %w", err)
			}

		default:
			return notices, fmt.Errorf("unexpected message during START_REPLICATION: '%c' (0x%02x)", msgType, msgType)
		}
	}
}

// ReplicationMessage is a single message read from a copy-both stream.
// Exactly one of Data or Notice is set on a nil-error return:
//   - Data holds the raw CopyData payload — including the leading replication
//     sub-type byte ('w'/'k'/etc.), which is NOT interpreted here; it is
//     forwarded verbatim (byte-level passthrough).
//   - Notice holds a NoticeResponse diagnostic the server interleaved with the
//     stream. These are surfaced rather than dropped: postgres can emit
//     warnings (slot/WAL retention, plugin messages) mid-stream and the caller
//     decides what to do with them.
type ReplicationMessage struct {
	Data   []byte
	Notice *mterrors.PgDiagnostic
}

// ReadReplicationMessage reads the next message from a copy-both stream.
//
// Returns a ReplicationMessage carrying either a CopyData payload or a
// NoticeResponse diagnostic. ParameterStatus frames are recorded into the
// connection's server params and reading continues (they remain observable via
// ServerParams(), so nothing is silently dropped). Returns io.EOF when the
// server sends CopyDone. Returns the parsed PG error (after draining to
// ReadyForQuery) on ErrorResponse. Both CopyDone and ErrorResponse terminate
// the stream in both directions: no CopyDone must be sent afterward, and the
// stream is marked dead so WriteReplicationData will refuse further sends.
//
// ctx is honored: if it has a deadline or is cancelled, a blocked read unblocks
// and returns a context error. This is the teardown primitive used to abort
// long-lived streams (e.g. on leader change).
func (c *Conn) ReadReplicationMessage(ctx context.Context) (msg ReplicationMessage, err error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	stop := c.makeReadsCancelable(ctx)
	defer stop()

	// Any non-context error (transport failure, server CopyDone/ErrorResponse,
	// malformed or unexpected frame) terminates the stream in both directions:
	// mark it dead so WriteReplicationData refuses further sends. A context
	// error (cancel/deadline) is excluded — the socket is intact and the caller
	// may resume reading or send an ack before reading again (walreceiver-style),
	// so a deadline-driven wakeup must not kill the stream.
	defer func() {
		if err != nil && ctx.Err() == nil {
			c.replState = replStreamDead
		}
	}()

	for {
		msgType, body, readErr := c.readMessage()
		if readErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ReplicationMessage{}, ctxErr
			}
			return ReplicationMessage{}, fmt.Errorf("read replication message: %w", readErr)
		}

		switch msgType {
		case protocol.MsgCopyData:
			return ReplicationMessage{Data: body}, nil

		case protocol.MsgNoticeResponse:
			return ReplicationMessage{Notice: c.parseNotice(body)}, nil

		case protocol.MsgParameterStatus:
			// Recorded into serverParams; not a stream-terminating event
			// under normal operation. A parse failure means a malformed body
			// (wire corruption), which we surface rather than swallow.
			if psErr := c.handleParameterStatus(body); psErr != nil {
				return ReplicationMessage{}, fmt.Errorf("malformed ParameterStatus in copy-both stream: %w", psErr)
			}

		case protocol.MsgCopyDone:
			return ReplicationMessage{}, io.EOF

		case protocol.MsgErrorResponse:
			// Return the PG error; drain the trailing ReadyForQuery best-effort
			// (its return is ignored — pgErr is the error worth surfacing).
			pgErr := c.parseError(body)
			_ = c.waitForReadyForQuery(ctx)
			return ReplicationMessage{}, pgErr

		default:
			return ReplicationMessage{}, fmt.Errorf("unexpected message in copy-both stream: '%c' (0x%02x)", msgType, msgType)
		}
	}
}

// WriteReplicationData sends a client→server CopyData payload (e.g. a Standby
// Status Update, leading byte 'r') on a live copy-both stream. The payload is
// opaque: multigres does not build or validate it — the consumer does. Refuses
// to send once the stream is dead (server sent ErrorResponse or CopyDone),
// since per the protocol no further frames (including CopyDone) may follow.
//
// Single-owner contract: like the rest of *Conn, the replication stream is
// single-owner — ReadReplicationMessage holds bufMu across its (possibly
// blocking) read, so a write from a second goroutine would block until that
// read returns. Drive the stream from one goroutine: read with a deadline (see
// ReadReplicationMessage's ctx) and send acks between reads, walreceiver-style.
// Do NOT run a concurrent read goroutine and ack goroutine on the same Conn.
func (c *Conn) WriteReplicationData(payload []byte) error {
	c.bufMu.Lock()
	notStreaming := c.replState != replStreamStreaming
	c.bufMu.Unlock()
	if notStreaming {
		return errors.New("replication stream is not streaming; cannot send CopyData")
	}
	return c.WriteCopyData(payload)
}

// FinishReplication completes a client-initiated copy-both shutdown. The caller
// must have sent CopyDone (via WriteCopyDone) first. The server may still send
// in-flight CopyData before its own CopyDone (copy-both -> copy-out), so this
// drains CopyData, then consumes the server CopyDone, then reads until
// ReadyForQuery (tolerating more than one CommandComplete, as the physical
// START_REPLICATION format the docs reference can emit two). NoticeResponse
// diagnostics seen during the drain are collected and returned rather than
// dropped. Returns the last command tag seen and any notices.
//
// Call with a deadline-bearing or cancelable ctx: the drain reads are bounded
// by ctx (an absolute deadline across the whole drain). An unbounded ctx will
// block indefinitely if an unhealthy server never completes the shutdown.
func (c *Conn) FinishReplication(ctx context.Context) (string, []*mterrors.PgDiagnostic, error) {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	stop := c.makeReadsCancelable(ctx)
	defer stop()

	var (
		tag     string
		notices []*mterrors.PgDiagnostic
	)
	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return tag, notices, ctxErr
			}
			return tag, notices, fmt.Errorf("drain replication shutdown: %w", err)
		}
		switch msgType {
		case protocol.MsgCopyData, protocol.MsgCopyDone:
			// In-flight data or the server's CopyDone — discard and continue.
		case protocol.MsgCommandComplete:
			t := string(body)
			if len(t) > 0 && t[len(t)-1] == 0 {
				t = t[:len(t)-1]
			}
			tag = t
		case protocol.MsgNoticeResponse:
			notices = append(notices, c.parseNotice(body))
		case protocol.MsgErrorResponse:
			// Return the PG error; drain the trailing ReadyForQuery best-effort
			// (its return is ignored — pgErr is the error worth surfacing).
			pgErr := c.parseError(body)
			_ = c.waitForReadyForQuery(ctx)
			c.replState = replStreamDead
			return tag, notices, pgErr
		case protocol.MsgReadyForQuery:
			if len(body) > 0 {
				c.txnStatus = protocol.TransactionStatus(body[0])
			}
			c.replState = replStreamDead
			return tag, notices, nil
		default:
			return tag, notices, fmt.Errorf("unexpected message during replication shutdown: '%c'", msgType)
		}
	}
}

// makeReadsCancelable wires ctx cancellation/deadline to the socket read
// deadline so a blocked readMessage returns promptly when ctx is cancelled or
// its deadline passes. It returns a stop func that clears the deadline and
// tears down the watcher. If ctx can never be cancelled (context.Background
// with no deadline), it is a no-op.
//
// stop blocks until the watcher goroutine has fully exited before clearing the
// deadline. This is load-bearing: if stop merely closed done and cleared the
// deadline, a watcher that had already committed to the ctx.Done() branch could
// re-arm a now() deadline after the clear, leaving the connection with a stale
// past deadline that poisons the next read. Joining the goroutine first
// guarantees the clear is the last SetReadDeadline call.
func (c *Conn) makeReadsCancelable(ctx context.Context) (stop func()) {
	if ctx.Done() == nil {
		return func() {}
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetReadDeadline(dl)
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-ctx.Done():
			// Force the in-flight Read to return immediately.
			_ = c.SetReadDeadline(time.Now())
		case <-done:
		}
	}()
	return func() {
		close(done)
		<-finished                         // wait for the watcher to exit (no late SetReadDeadline)
		_ = c.SetReadDeadline(time.Time{}) // clear deadline
	}
}
