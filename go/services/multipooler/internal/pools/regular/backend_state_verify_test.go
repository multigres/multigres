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

package regular

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/pb/query"
)

// trackedQuery is the body every tracked test statement was prepared with.
const trackedQuery = "SELECT 1"

// scriptPreparedProbe scripts the probe to list the given statements, each
// with trackedQuery as its body unless the name is followed by "=" and a
// different body (e.g. "ppstmt1=SELECT 2").
func scriptPreparedProbe(server *fakepgserver.Server, entries ...string) {
	rows := make([][]any, 0, len(entries))
	for _, e := range entries {
		name, body, ok := strings.Cut(e, "=")
		if !ok {
			body = trackedQuery
		}
		rows = append(rows, []any{name, body})
	}
	server.AddQuery(constants.PreparedStatementsProbeSQL,
		fakepgserver.MakeResult([]string{"name", "statement"}, rows))
}

func trackPrepared(conn *Conn, names ...string) {
	for _, n := range names {
		conn.State().StorePreparedStatement(&query.PreparedStatement{Name: n, Query: trackedQuery})
	}
}

func TestVerifyPreparedStatementsInSync(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptPreparedProbe(server, "ppstmt1", "ppstmt2")

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	// The unnamed statement is tracked but never listed by the backend.
	trackPrepared(conn, "ppstmt1", "ppstmt2", "")

	div, err := PreparedStatementChecker{}.Check(context.Background(), conn)
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifyPreparedStatementsUntrackedAndPhantom(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptPreparedProbe(server, "ppstmt1", "rogue")

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	trackPrepared(conn, "ppstmt1", "ppstmt9")

	div, err := conn.VerifyPreparedStatements(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{foreignPreparedStatementName}, div.Untracked, "untracked names are redacted")
	assert.Equal(t, []string{"ppstmt9"}, div.Phantom)
	assert.Empty(t, div.Mismatched)
}

func TestVerifyPreparedStatementsRedefinedBody(t *testing.T) {
	// A hidden DEALLOCATE plus re-PREPARE keeps the tracked name but swaps
	// the body (a SQL PREPARE also stores the full PREPARE text). The name
	// alone would look clean; the body comparison must flag it, or
	// ensurePrepared hands the redefined statement to the next borrower.
	server := fakepgserver.New(t)
	defer server.Close()
	scriptPreparedProbe(server, "ppstmt1", "ppstmt2=PREPARE ppstmt2 AS SELECT 2 AS evil")

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	trackPrepared(conn, "ppstmt1", "ppstmt2")

	div, err := conn.VerifyPreparedStatements(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"ppstmt2"}, div.Mismatched)
	assert.Empty(t, div.Untracked)
	assert.Empty(t, div.Phantom)
}

func TestVerifyPreparedStatementsRedactsAllUntrackedNames(t *testing.T) {
	// Every untracked name may embed client data — a PREPARE hidden in a
	// routine body chooses the name, and the consolidator's ppstmt<N> shape
	// is reachable too (ppstmt1234567890 with an account number). None may
	// reach the Divergence that flows into logs. Counts survive: one entry
	// per statement.
	server := fakepgserver.New(t)
	defer server.Close()
	scriptPreparedProbe(server, "ppstmt1", "ppstmt1234567890", `stmt_ssn_123-45-6789`, "get_user_by_email")

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	trackPrepared(conn, "ppstmt1")

	div, err := conn.VerifyPreparedStatements(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"foreign_name", "foreign_name", "foreign_name"}, div.Untracked)
	assert.Empty(t, div.Phantom)
	assert.Empty(t, div.Mismatched)
}

func TestVerifyPreparedStatementsProbeError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.AddRejectedQuery(constants.PreparedStatementsProbeSQL, errors.New("backend on fire"))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	_, err := conn.VerifyPreparedStatements(context.Background())
	require.Error(t, err)
}

func scriptAdvisoryProbe(server *fakepgserver.Server, held any) {
	server.AddQuery(constants.PgLocksAdvisoryProbeSQL,
		fakepgserver.MakeResult([]string{"exists"}, [][]any{{held}}))
}

func TestVerifyAdvisoryLocksNoneHeld(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptAdvisoryProbe(server, "f")

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := AdvisoryLockChecker{}.Check(context.Background(), conn)
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifyAdvisoryLocksHeld(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptAdvisoryProbe(server, "t")

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifyAdvisoryLocks(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{advisoryLockDivergenceName}, div.Untracked)
}

func TestVerifyAdvisoryLocksMalformed(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptAdvisoryProbe(server, "maybe")

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	_, err := conn.VerifyAdvisoryLocks(context.Background())
	require.Error(t, err, "a non-boolean probe result must be a probe failure, not a clean verdict")
}

func scriptCursorProbe(server *fakepgserver.Server, names ...string) {
	rows := make([][]any, 0, len(names))
	for _, n := range names {
		rows = append(rows, []any{n})
	}
	server.AddQuery(constants.HoldableCursorsProbeSQL,
		fakepgserver.MakeResult([]string{"name"}, rows))
}

func TestVerifyHoldableCursorsNone(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptCursorProbe(server)

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := HoldableCursorChecker{}.Check(context.Background(), conn)
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifyHoldableCursorsReportsCountNotNames(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptCursorProbe(server, "cur_tenant_42", "hidden_hold")

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifyHoldableCursors(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{holdableCursorDivergenceName, holdableCursorDivergenceName}, div.Untracked)
}

func scriptTempProbe(server *fakepgserver.Server, codes ...string) {
	rows := make([][]any, 0, len(codes))
	for _, c := range codes {
		rows = append(rows, []any{c})
	}
	server.AddQuery(constants.TempObjectsProbeSQL,
		fakepgserver.MakeResult([]string{"relkind"}, rows))
}

func TestVerifyTempObjectsEmpty(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	scriptTempProbe(server)

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := TempObjectChecker{}.Check(context.Background(), conn)
	require.NoError(t, err)
	assert.False(t, div.IsDiverged())
}

func TestVerifyTempObjectsReportsKindsNotNames(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	// Two tables, their toast tables, an index, a function, a domain, an
	// enum, a range with its multirange, one of each other catalog's tag,
	// and an unknown code: kinds are deduplicated and unknown codes stay
	// bounded.
	scriptTempProbe(server, "r", "r", "t", "t", "i", "function", "type:d", "type:e", "type:r", "type:m",
		"operator", "collation", "statistics", "operator_class", "operator_family", "conversion",
		"ts_parser", "ts_dictionary", "ts_template", "ts_config", "z")

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifyTempObjects(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{
		"collation", "conversion", "domain", "enum", "function", "index", "multirange",
		"operator", "operator_class", "operator_family", "range", "statistics", "table",
		"toast_table", "ts_config", "ts_dictionary", "ts_parser", "ts_template", "unknown_z",
	}, div.Untracked)
	assert.Empty(t, div.Phantom)
}
