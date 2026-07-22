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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// pgMsgReader reassembles complete PostgreSQL protocol messages (frontend
// or backend — both share the same 1-byte type + 4-byte length + body wire
// framing) from the opaque byte chunks a chunk-returning source produces.
// Those chunks are NOT message-aligned — a chunk can hold a partial
// message, a whole message, or several.
type pgMsgReader struct {
	recv func() ([]byte, error)
	buf  []byte // leftover bytes from prior recv() calls, not yet consumed
}

// next reads one complete backend message and returns its type byte and the
// exact raw wire bytes (header + body), so the caller can forward them to
// the client verbatim without re-serializing.
func (r *pgMsgReader) next() (msgType byte, raw []byte, err error) {
	if err := ensureBuffered(&r.buf, r.recv, 5); err != nil {
		return 0, nil, err
	}
	msgType, bodyLen, err := parsePgMessageHeader(r.buf[:5], "backend")
	if err != nil {
		return 0, nil, err
	}
	total := 5 + bodyLen
	if err := ensureBuffered(&r.buf, r.recv, total); err != nil {
		return 0, nil, err
	}
	raw = append([]byte(nil), r.buf[:total]...)
	r.buf = r.buf[total:]
	return msgType, raw, nil
}

// rawClientMessage reconstructs the exact wire bytes (5-byte header + body)
// of a frontend message whose type/length/body have already been read off
// conn individually.
func rawClientMessage(msgType byte, bodyLen int, body []byte) []byte {
	raw := make([]byte, 5+bodyLen)
	raw[0] = msgType
	binary.BigEndian.PutUint32(raw[1:5], uint32(bodyLen+4))
	copy(raw[5:], body)
	return raw
}

// parseNullTerminatedString extracts a null-terminated string from a Query
// message body, PostgreSQL wire-protocol convention. Returns false if body
// is not null-terminated (malformed message — let the pooler/postgres reject
// it rather than guessing).
func parseNullTerminatedString(body []byte) (string, bool) {
	if len(body) == 0 || body[len(body)-1] != 0 {
		return "", false
	}
	return string(body[:len(body)-1]), true
}

// runReplicationPreamble relays the replication-protocol command/response
// cycles (IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, START_REPLICATION, ...)
// message-by-message between the client and the pooler stream, inspecting
// only CREATE_REPLICATION_SLOT to reject non-TEMPORARY slot requests before
// they ever reach the pooler/postgres. Once streaming begins (a
// CopyBothResponse is observed), it returns so the caller can hand off to the
// byte-blind commonrepl.Tunnel for the remainder of the connection's life.
//
// Real replication clients block waiting for each command's reply before
// deciding what to send next (e.g. IDENTIFY_SYSTEM before
// CREATE_REPLICATION_SLOT), so this cannot just peek at the first message —
// it must relay full request/response cycles until it affirmatively sees
// streaming begin.
//
// Only the simple query protocol is supported here: a replication=database
// connection technically allows arbitrary SQL in addition to the replication
// commands, and such SQL could in principle use the extended query protocol
// (Parse/Bind/Describe/Execute/Close/Flush/Sync). But a survey of the most
// widely used open-source software that consumes PostgreSQL logical
// replication — spanning independent implementations across several
// languages — found none that use the extended query protocol on this
// connection; several document this as a Postgres-imposed limitation, not
// just their own choice. Supporting the extended protocol here would mean
// mirroring postgres's own multi-message-per-command, ignore-till-sync state
// machine for a case nothing in practice exercises. Anything other than
// Query is rejected outright instead.
func runReplicationPreamble(
	ctx context.Context,
	conn *server.Conn,
	stream multipoolerservice.MultipoolerService_StreamReplicationClient,
) (streaming bool, leftover []byte, err error) {
	for {
		// 1. Read one full frontend message off the still-attached client
		// socket. EOF/Terminate here just means the client left before
		// streaming started — not an error.
		msgType, rerr := conn.ReadMessageType()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return false, nil, nil
			}
			return false, nil, rerr
		}
		if msgType == protocol.MsgTerminate {
			return false, nil, nil
		}

		length, rerr := conn.ReadMessageLength()
		if rerr != nil {
			return false, nil, rerr
		}
		body, rerr := conn.ReadMessageBody(length)
		if rerr != nil {
			return false, nil, rerr
		}

		// 2. Gate: only simple Query is allowed here (see doc comment above).
		if msgType != protocol.MsgQuery {
			rejectErr := unsupportedPreambleMessageError(msgType)
			if werr := conn.WriteError(rejectErr); werr != nil {
				return false, nil, werr
			}
			return false, nil, rejectErr
		}

		// 3. Inspect the query text for a non-TEMPORARY CREATE_REPLICATION_SLOT
		// and reject it before the pooler/postgres ever see it.
		if cmd, ok := parseNullTerminatedString(body); ok {
			if rejectErr := nonTemporaryCreateReplicationSlotError(cmd); rejectErr != nil {
				// The pooler/postgres never see the rejected command.
				if werr := conn.WriteError(rejectErr); werr != nil {
					return false, nil, werr
				}
				return false, nil, rejectErr
			}
		}

		// 4. Forward the (accepted) command to the pooler's replication stream
		// verbatim.
		raw := rawClientMessage(msgType, length, body)
		if serr := stream.Send(&multipoolerservice.StreamReplicationRequest{
			Msg: &multipoolerservice.StreamReplicationRequest_Data{Data: raw},
		}); serr != nil {
			return false, nil, serr
		}

		// 5. Relay the pooler's response(s) back to the client message-by-
		// message until the command cycle resolves.
		reader := &pgMsgReader{recv: func() ([]byte, error) {
			resp, rerr := stream.Recv()
			if rerr != nil {
				return nil, rerr
			}
			if e := resp.GetError(); e != nil {
				if diag := e.GetDiagnostic(); diag != nil {
					return nil, mterrors.PgDiagnosticFromProto(diag)
				}
				return nil, mterrors.New(mtrpcpb.Code_INTERNAL, "replication stream returned an error without a diagnostic")
			}
			return resp.GetData(), nil
		}}

		for {
			respType, respRaw, nerr := reader.next()
			if nerr != nil {
				return false, nil, nerr
			}
			if werr := conn.WriteRawMessage(respRaw); werr != nil {
				return false, nil, werr
			}
			if werr := conn.Flush(); werr != nil {
				return false, nil, werr
			}
			// 6a. Streaming has begun: hand off to the tunnel, carrying over
			// any bytes already read past this response.
			if respType == protocol.MsgCopyBothResponse {
				return true, reader.buf, nil
			}
			// 6b. Command cycle done with no streaming yet: go back to step 1
			// for the client's next command.
			if respType == protocol.MsgReadyForQuery {
				break
			}
		}
	}
}

// unsupportedPreambleMessageError rejects any frontend message other than a
// simple Query (Terminate is handled separately, unconditionally) arriving
// before streaming begins. See runReplicationPreamble's doc comment for why
// the extended query protocol isn't supported here.
func unsupportedPreambleMessageError(msgType byte) error {
	return mterrors.NewFeatureNotSupported(
		fmt.Sprintf("message type %q is not supported on a replication connection before streaming begins: only the simple query protocol is supported here", string(msgType)))
}

// nonTemporaryCreateReplicationSlotError returns a rejection error if cmd is
// a CREATE_REPLICATION_SLOT command that does not request a TEMPORARY slot,
// or nil if cmd is anything else / already requests TEMPORARY. Multigres
// cannot yet transition a replication slot's position across a primary
// failover, so only ephemeral (client-lifetime) slots are safe.
//
// No grammar exists in this codebase for the replication protocol's command
// language (see go/common/parser/ast/replication.go — the AST nodes exist
// but nothing constructs them), so this is deliberately a lightweight
// tokenizer, not a parser: CREATE_REPLICATION_SLOT slot_name [TEMPORARY]
// {PHYSICAL|LOGICAL ...} — TEMPORARY, if present, appears between the slot
// name and the PHYSICAL/LOGICAL keyword, so stop scanning there.
func nonTemporaryCreateReplicationSlotError(cmd string) error {
	fields := strings.Fields(cmd)
	if len(fields) == 0 || !strings.EqualFold(fields[0], "CREATE_REPLICATION_SLOT") {
		return nil
	}
	if len(fields) < 2 {
		return mterrors.NewNonTemporaryReplicationSlotError("CREATE_REPLICATION_SLOT", "TEMPORARY")
	}
	// fields[1] is the slot name — skip it, or a client naming its slot
	// "temporary" would have that name misread as the keyword.
	for _, f := range fields[2:] {
		if strings.EqualFold(f, "TEMPORARY") {
			return nil
		}
		if strings.EqualFold(f, "PHYSICAL") || strings.EqualFold(f, "LOGICAL") {
			break
		}
	}
	return mterrors.NewNonTemporaryReplicationSlotError("CREATE_REPLICATION_SLOT", "TEMPORARY")
}
