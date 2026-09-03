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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/pb/query"
)

func scriptPreparedProbe(server *fakepgserver.Server, names ...string) {
	rows := make([][]any, 0, len(names))
	for _, n := range names {
		rows = append(rows, []any{n})
	}
	server.AddQuery(constants.PreparedStatementsProbeSQL,
		fakepgserver.MakeResult([]string{"name"}, rows))
}

func trackPrepared(conn *Conn, names ...string) {
	for _, n := range names {
		conn.State().StorePreparedStatement(&query.PreparedStatement{Name: n, Query: "SELECT 1"})
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
	assert.Equal(t, []string{foreignPreparedStatementName}, div.Untracked, "a non-consolidator name is redacted")
	assert.Equal(t, []string{"ppstmt9"}, div.Phantom)
	assert.Empty(t, div.Mismatched)
}

func TestVerifyPreparedStatementsRedactsForeignNames(t *testing.T) {
	// An untracked name that is not consolidator-shaped may embed client
	// data (a PREPARE hidden in a routine body); it must never reach the
	// Divergence that flows into logs. Counts survive: one entry each.
	server := fakepgserver.New(t)
	defer server.Close()
	scriptPreparedProbe(server, "ppstmt1", "ppstmt7", `stmt_ssn_123-45-6789`, "get_user_by_email")

	conn := newTestDirectConn(t, server)
	defer conn.Close()
	trackPrepared(conn, "ppstmt1")

	div, err := conn.VerifyPreparedStatements(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"foreign_name", "foreign_name", "ppstmt7"}, div.Untracked)
	assert.Empty(t, div.Phantom)
	for _, name := range div.Untracked {
		assert.NotContains(t, name, "ssn")
		assert.NotContains(t, name, "email")
	}
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
	// enum, a range with its multirange, and an unknown code: kinds are
	// deduplicated and unknown codes stay bounded.
	scriptTempProbe(server, "r", "r", "t", "t", "i", "function", "type:d", "type:e", "type:r", "type:m", "z")

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	div, err := conn.VerifyTempObjects(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"domain", "enum", "function", "index", "multirange", "range", "table", "toast_table", "unknown_z"}, div.Untracked)
	assert.Empty(t, div.Phantom)
}
