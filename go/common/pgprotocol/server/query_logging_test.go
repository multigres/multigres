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
	"context"
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

func TestQueryLoggingOmitsSQLAndErrorText(t *testing.T) {
	const secret = "query-password-must-not-be-logged"
	const sql = "ALTER ROLE app PASSWORD '" + secret + "'"
	syntaxError := mterrors.NewPgError("ERROR", mterrors.PgSSSyntaxError,
		"syntax error at or near \""+secret+"\"", "")
	permissionError := mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege,
		"permission denied", "")

	for _, tc := range []struct {
		name  string
		query string
		err   *mterrors.PgDiagnostic
		level slog.Level
	}{
		{name: "success", query: sql, level: slog.LevelDebug},
		{name: "permission error", query: sql, err: permissionError, level: slog.LevelDebug},
		{name: "error-level logging", query: sql, err: permissionError, level: slog.LevelError},
		{name: "malformed SQL", query: "ALTER ROLE app '" + secret + "'", err: syntaxError, level: slog.LevelDebug},
		{name: "batch", query: "SELECT 1; " + sql + "; SELECT 2", level: slog.LevelDebug},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var input, output, logs bytes.Buffer
			require.NoError(t, binary.Write(&input, binary.BigEndian, int32(4+len(tc.query)+1)))
			input.WriteString(tc.query)
			input.WriteByte(0)
			h := &testHandler{queryFunc: func(_ context.Context, _ *Conn, got string, callback func(context.Context, *sqltypes.Result) error) error {
				require.Equal(t, tc.query, got, "execution must receive the original SQL")
				if tc.err != nil {
					return tc.err
				}
				return callback(context.Background(), &sqltypes.Result{CommandTag: "ALTER ROLE"})
			}}
			conn := createExtendedQueryTestConn(t, &input, &output, h)
			conn.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: tc.level}))
			conn.startWriterBuffering()
			require.NoError(t, conn.handleQuery())
			require.NoError(t, conn.endWriterBuffering())

			require.NotEmpty(t, logs.String())
			require.NotContains(t, logs.String(), secret)
			if tc.err != nil {
				require.Contains(t, logs.String(), "sqlstate="+tc.err.Code)
				require.Contains(t, output.String(), tc.err.Code)
				require.Contains(t, output.String(), tc.err.Message, "client diagnostics must retain their original message")
			}
		})
	}
}

func TestParseLoggingOmitsSQL(t *testing.T) {
	const secret = "parse-password-must-not-be-logged"
	for _, reject := range []bool{false, true} {
		name := "success"
		if reject {
			name = "syntax error"
		}
		t.Run(name, func(t *testing.T) {
			var body, input, output, logs bytes.Buffer
			body.WriteString("stmt\x00ALTER ROLE app PASSWORD '" + secret + "'\x00")
			require.NoError(t, binary.Write(&body, binary.BigEndian, uint16(0)))
			require.NoError(t, binary.Write(&input, binary.BigEndian, int32(body.Len()+4)))
			input.Write(body.Bytes())
			h := &testHandler{parseFunc: func(_ context.Context, _ *Conn, _ string, sql string, _ []uint32) error {
				require.Contains(t, sql, secret, "execution must receive the original SQL")
				if reject {
					return mterrors.NewPgError("ERROR", mterrors.PgSSSyntaxError,
						"syntax error at or near \""+secret+"\"", "")
				}
				return nil
			}}
			conn := createExtendedQueryTestConn(t, &input, &output, h)
			conn.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			conn.startWriterBuffering()
			require.NoError(t, conn.handleParse())
			require.NoError(t, conn.endWriterBuffering())

			require.Contains(t, logs.String(), "msg=parse")
			require.Contains(t, logs.String(), "param_count=0")
			require.NotContains(t, logs.String(), secret)
			if reject {
				require.Equal(t, byte(protocol.MsgErrorResponse), output.Bytes()[0])
				require.Contains(t, output.String(), secret, "client diagnostics must retain their original message")
			} else {
				require.Equal(t, byte(protocol.MsgParseComplete), output.Bytes()[0])
			}
		})
	}
}
