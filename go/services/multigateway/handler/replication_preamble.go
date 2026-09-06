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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
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
	conn *server.Conn,
	stream multipoolerservice.MultipoolerService_StreamReplicationClient,
	admitFailoverSlots bool,
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
		// (the replication-protocol command form) or a
		// pg_create_*_replication_slot(...) call with temporary != true (the
		// SQL form — postgres falls through to its normal SQL executor for
		// any query on this connection that isn't a recognized replication
		// command, so this connection can reach that function same as any
		// other), and reject either before the pooler/postgres ever see it.
		if cmd, ok := parseNullTerminatedString(body); ok {
			if rejectErr := nonTemporaryCreateReplicationSlotError(cmd, admitFailoverSlots); rejectErr != nil {
				// The pooler/postgres never see the rejected command.
				if werr := conn.WriteError(rejectErr); werr != nil {
					return false, nil, werr
				}
				return false, nil, rejectErr
			}
			if rejectErr := nonTemporaryReplicationSlotSQLFuncError(cmd, admitFailoverSlots); rejectErr != nil {
				if werr := conn.WriteError(rejectErr); werr != nil {
					return false, nil, werr
				}
				return false, nil, rejectErr
			}

			// 3b. Enforce failover: with the feature on, refuse an
			// ALTER_REPLICATION_SLOT that would turn FAILOVER off. Multigres
			// auto-marks every non-temporary logical slot as a failover slot
			// (step 3c), so it will not let a client silently opt back out and
			// lose HA. Enabling failover or changing only TWO_PHASE passes
			// through.
			if admitFailoverSlots && alterReplicationSlotDisablesFailover(cmd) {
				rejectErr := mterrors.NewFeatureNotSupported(
					"ALTER_REPLICATION_SLOT cannot disable FAILOVER: Multigres keeps every non-temporary logical slot registered for failover",
				)
				if werr := conn.WriteError(rejectErr); werr != nil {
					return false, nil, werr
				}
				return false, nil, rejectErr
			}

			// 3c. Auto-mark: with the feature on, inject FAILOVER into a
			// non-temporary logical CREATE_REPLICATION_SLOT that omits it, so
			// the slot is created as a failover slot without client opt-in.
			// The rewritten command replaces the body forwarded to the pooler
			// below; length stays in sync (== len(body)). A NOTICE tells the
			// client its slot was registered for failover — advisory, and valid
			// before the command's own result (postgres emits notices mid-command
			// the same way). For a real subscriber it surfaces in that server's
			// log via its notice processor.
			if admitFailoverSlots {
				if rewritten, changed := rewriteCreateReplicationSlotAddFailover(cmd); changed {
					body = append([]byte(rewritten), 0)
					length = len(body)
					if werr := conn.WriteNotice(autoFailoverNotice); werr != nil {
						return false, nil, werr
					}
				}
			}
		}

		// 4. Forward the (accepted, possibly rewritten) command to the pooler's
		// replication stream.
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
		fmt.Sprintf("message type %q is not supported on a replication connection before streaming begins: only the simple query protocol is supported here", string(msgType)),
	)
}

// nonTemporaryCreateReplicationSlotError returns a rejection error if cmd is a
// CREATE_REPLICATION_SLOT command that does not request a TEMPORARY slot, or
// nil if cmd is anything else / already requests TEMPORARY. A non-temporary
// slot could not previously be carried across a primary failover, so only
// ephemeral (client-lifetime) slots were safe.
//
// When admitFailoverSlots is true (the slot-based-replication feature is on)
// every non-temporary LOGICAL slot is admitted, not just one that already
// requests failover: a slot registered for failover (the command a real
// PostgreSQL 17 subscriber sends for CREATE SUBSCRIPTION ... WITH (failover =
// true), verified in go/test/endtoend/subscriptionwire) is admitted as-is, and
// one that omits FAILOVER is auto-marked by
// rewriteCreateReplicationSlotAddFailover before it reaches postgres so it too
// becomes a failover slot. Only a deliberate FAILOVER false is an opt-out and
// stays rejected. Because the FAILOVER option rides in the
// CREATE_REPLICATION_SLOT command itself, no ALTER_REPLICATION_SLOT follow-up
// needs to be tracked.
//
// No grammar exists in this codebase for the replication protocol's command
// language (see go/common/parser/ast/replication.go — the AST nodes exist but
// nothing constructs them), so this is deliberately a lightweight tokenizer,
// not a parser: CREATE_REPLICATION_SLOT slot_name [TEMPORARY] {PHYSICAL |
// LOGICAL output_plugin [ ( option [, ...] ) ]}. TEMPORARY, if present, appears
// between the slot name and the PHYSICAL/LOGICAL keyword; the FAILOVER option
// appears in the parenthesized list after the plugin.
func nonTemporaryCreateReplicationSlotError(cmd string, admitFailoverSlots bool) error {
	fields := strings.Fields(cmd)
	if len(fields) == 0 || !strings.EqualFold(fields[0], "CREATE_REPLICATION_SLOT") {
		return nil
	}
	if len(fields) < 2 {
		return mterrors.NewNonTemporaryReplicationSlotError("CREATE_REPLICATION_SLOT", "TEMPORARY")
	}
	// fields[1] is the slot name — skip it, or a client naming its slot
	// "temporary" would have that name misread as the keyword. TEMPORARY, if
	// present, appears before the PHYSICAL/LOGICAL keyword.
	kindIdx := -1
	for i := 2; i < len(fields); i++ {
		if strings.EqualFold(fields[i], "TEMPORARY") {
			return nil
		}
		if strings.EqualFold(fields[i], "PHYSICAL") || strings.EqualFold(fields[i], "LOGICAL") {
			kindIdx = i
			break
		}
	}
	// Non-temporary. With the feature enabled, admit any LOGICAL slot except an
	// explicit opt-out: one that already requests failover is a failover slot
	// as-is, and one that omits FAILOVER is auto-marked — the option is injected
	// before the command reaches postgres (rewriteCreateReplicationSlotAddFailover).
	// A deliberate FAILOVER false stays rejected, as do PHYSICAL slots, which
	// cannot be failover slots.
	if admitFailoverSlots && kindIdx >= 0 && strings.EqualFold(fields[kindIdx], "LOGICAL") &&
		createReplicationSlotFailover(fields[kindIdx+1:]) != failoverDisabled {
		return nil
	}
	return mterrors.NewNonTemporaryReplicationSlotError("CREATE_REPLICATION_SLOT", "TEMPORARY")
}

// failoverOption is the state of the FAILOVER option in the parenthesized
// option list of a CREATE_REPLICATION_SLOT ... LOGICAL command.
type failoverOption int

const (
	failoverAbsent   failoverOption = iota // no FAILOVER option present
	failoverEnabled                        // FAILOVER, or FAILOVER <true-ish>
	failoverDisabled                       // FAILOVER <false-ish>
)

// slotOptionTokens splits a replication-command option list into individual
// option tokens. Parentheses and commas are normalized to spaces so an option
// glued to a paren or comma — e.g. the glued form `pgoutput(FAILOVER)` or
// `(FAILOVER,SNAPSHOT ...)` — still separates into its own token, then the
// result is whitespace-split.
func slotOptionTokens(tokens []string) []string {
	normalized := strings.Map(func(r rune) rune {
		if r == '(' || r == ')' || r == ',' {
			return ' '
		}
		return r
	}, strings.Join(tokens, " "))
	return strings.Fields(normalized)
}

// failoverFromOptions classifies the FAILOVER option within an already-split
// option list (no output plugin among the tokens). A bare FAILOVER means
// enabled; an explicit boolean value (FAILOVER false/off/no/0/...) is
// interpreted with sqltypes.ParseBool, which mirrors PostgreSQL's own
// parse_bool_with_len. A value it can't parse leaves the option enabled (the
// conservative direction — never silently disables). The three states let a
// caller tell an absent option (auto-marked) apart from a deliberate opt-out
// (rejected).
func failoverFromOptions(opts []string) failoverOption {
	for i, opt := range opts {
		if !strings.EqualFold(opt, "FAILOVER") {
			continue
		}
		if i+1 < len(opts) {
			if v, ok := sqltypes.ParseBool(strings.Trim(opts[i+1], "'\"")); ok {
				if v {
					return failoverEnabled
				}
				return failoverDisabled
			}
		}
		return failoverEnabled
	}
	return failoverAbsent
}

// createReplicationSlotFailover classifies the FAILOVER option in the tokens
// following the LOGICAL keyword of a CREATE_REPLICATION_SLOT command, e.g.
// `pgoutput (FAILOVER, SNAPSHOT 'nothing')` or the glued form
// `pgoutput(FAILOVER)`. The first option token is the required output_plugin
// argument, not an option, so it is dropped before scanning — otherwise a
// plugin literally named "failover" (client-controlled) would be misread as an
// already-enabled option and wrongly suppress auto-marking. Dropping the plugin
// after normalization handles both the spaced and glued forms.
func createReplicationSlotFailover(tokens []string) failoverOption {
	opts := slotOptionTokens(tokens)
	if len(opts) == 0 {
		return failoverAbsent
	}
	return failoverFromOptions(opts[1:]) // opts[0] is the output_plugin
}

// createReplicationSlotHasFailover reports whether the option list requests
// failover (present and not set false). Retained as the predicate the
// command-form guard and its tests read.
func createReplicationSlotHasFailover(tokens []string) bool {
	return createReplicationSlotFailover(tokens) == failoverEnabled
}

// autoFailoverNotice is the advisory NOTICE the gateway sends a client when it
// auto-marks the client's logical slot as a failover slot (preamble step 3c),
// so the change is observable — in a PostgreSQL subscriber's server log via its
// notice processor, or in any client's notice handler. The content is static,
// and NoticeResponse serialization does not mutate the diagnostic, so a single
// shared instance is safe.
var autoFailoverNotice = mterrors.NewPgNotice(
	"NOTICE", mterrors.PgSSSuccessfulCompletion,
	"multigres registered this replication slot for failover so it survives primary failover",
	"",
)

// rewriteCreateReplicationSlotAddFailover injects a FAILOVER option into a
// non-temporary LOGICAL CREATE_REPLICATION_SLOT command that omits one, so the
// slot is created as a failover slot without the client having to ask for it —
// the auto-marking behavior enabled alongside slot-based replication. It
// returns the rewritten command and true when a rewrite
// applies, or ("", false) otherwise: for a temporary slot, a physical slot, a
// slot that already carries FAILOVER (enabled or explicitly disabled), or any
// command that is not CREATE_REPLICATION_SLOT.
//
// The grammar is the one nonTemporaryCreateReplicationSlotError tokenizes:
// CREATE_REPLICATION_SLOT slot_name [TEMPORARY]
// {PHYSICAL | LOGICAL output_plugin [ ( option [, ...] ) ]}. FAILOVER is
// inserted as the first option of the parenthesized list (`(FAILOVER, ...)`),
// or a fresh `(FAILOVER)` list is appended when the plugin has no option list.
// A slot name and output plugin are PostgreSQL identifiers restricted to
// [a-z0-9_], so the first '(' in the command is unambiguously the option
// list's opening paren.
func rewriteCreateReplicationSlotAddFailover(cmd string) (string, bool) {
	fields := strings.Fields(cmd)
	if len(fields) < 4 || !strings.EqualFold(fields[0], "CREATE_REPLICATION_SLOT") {
		return "", false
	}
	kindIdx := -1
	for i := 2; i < len(fields); i++ {
		if strings.EqualFold(fields[i], "TEMPORARY") || strings.EqualFold(fields[i], "PHYSICAL") {
			return "", false
		}
		if strings.EqualFold(fields[i], "LOGICAL") {
			kindIdx = i
			break
		}
	}
	if kindIdx < 0 || kindIdx+1 >= len(fields) {
		return "", false // no output plugin
	}
	// Only inject when FAILOVER is entirely absent; a client-supplied value
	// (enabled or a deliberate false) is left untouched.
	if createReplicationSlotFailover(fields[kindIdx+1:]) != failoverAbsent {
		return "", false
	}
	if open := strings.IndexByte(cmd, '('); open >= 0 {
		rest := cmd[open+1:]
		// An empty option list `()` must not become `(FAILOVER, )`.
		if strings.HasPrefix(strings.TrimLeft(rest, " \t"), ")") {
			return cmd[:open+1] + "FAILOVER" + rest, true
		}
		return cmd[:open+1] + "FAILOVER, " + rest, true
	}
	return strings.TrimRight(cmd, " \t") + " (FAILOVER)", true
}

// alterReplicationSlotDisablesFailover reports whether cmd is an
// ALTER_REPLICATION_SLOT command that sets FAILOVER to a false value. PostgreSQL
// emits exactly this for ALTER SUBSCRIPTION ... SET (failover = false):
// `ALTER_REPLICATION_SLOT "slot" ( FAILOVER false );` (see libpqrcv_alter_slot;
// the option always carries an explicit true/false, and TWO_PHASE may ride
// alongside it). It is the only way to change a slot's failover after creation —
// there is no pg_alter_replication_slot SQL function — so intercepting this
// command fully enforces that an auto-marked slot stays a failover slot.
// Enabling failover, or changing only TWO_PHASE, is left alone.
//
// The option list follows the slot name (fields[1]) and — unlike the CREATE
// form — has no output_plugin, so it is classified directly with
// failoverFromOptions (not createReplicationSlotFailover, which would drop the
// first real option as if it were a plugin).
func alterReplicationSlotDisablesFailover(cmd string) bool {
	fields := strings.Fields(cmd)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "ALTER_REPLICATION_SLOT") {
		return false
	}
	return failoverFromOptions(slotOptionTokens(fields[2:])) == failoverDisabled
}

// nonTemporaryReplicationSlotSQLFuncError returns a rejection error if cmd
// parses as SQL containing a pg_create_physical_replication_slot/
// pg_create_logical_replication_slot(...) call whose temporary argument
// isn't a literal true, or nil otherwise (including when cmd isn't valid
// SQL at all — e.g. a replication-protocol command like IDENTIFY_SYSTEM or
// START_REPLICATION, which nonTemporaryCreateReplicationSlotError already
// handles above and which never parses as SQL).
//
// When admitFailoverSlots is true, a non-temporary logical slot created with
// failover => true is also admitted, mirroring the command-form guard — for the
// case where the slot is created by a plain `SELECT
// pg_create_logical_replication_slot(...)` rather than the walsender command.
//
// This exists because postgres's walsender falls through to the normal SQL
// executor for any query on a replication=database connection that isn't a
// recognized replication command (exec_replication_command returning false
// in walsender.c) — so a plain `SELECT pg_create_physical_replication_slot(...)`
// reaches the same function the planner already guards on ordinary
// connections (go/services/multigateway/planner/unsafe_funccall.go), and
// must be guarded here too.
func nonTemporaryReplicationSlotSQLFuncError(cmd string, admitFailoverSlots bool) error {
	// A parse error means cmd isn't SQL at all — most likely one of the
	// replication-protocol commands nonTemporaryCreateReplicationSlotError
	// already handled above. Nothing to check.
	stmts, err := parser.ParseSQL(cmd)
	if err == nil {
		for _, stmt := range stmts {
			var name string
			var found bool
			if admitFailoverSlots {
				name, found = ast.FindNonTemporaryNonFailoverReplicationSlotCall(stmt)
			} else {
				name, found = ast.FindNonTemporaryReplicationSlotCall(stmt)
			}
			if found {
				return mterrors.NewNonTemporaryReplicationSlotError(name, "temporary=true")
			}
		}
	}
	return nil
}
