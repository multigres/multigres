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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// buildFatalErrorResponse returns a serialized ErrorResponse carrying a FATAL
// severity, as PostgreSQL sends right before closing the connection (e.g.
// pg_terminate_backend, crash recovery, immediate shutdown).
func buildFatalErrorResponse(sqlstate, message string) []byte {
	var body bytes.Buffer
	body.WriteByte('S')
	body.WriteString("FATAL")
	body.WriteByte(0)
	body.WriteByte('C')
	body.WriteString(sqlstate)
	body.WriteByte(0)
	body.WriteByte('M')
	body.WriteString(message)
	body.WriteByte(0)
	body.WriteByte(0)

	var out bytes.Buffer
	writeRawMessage(&out, protocol.MsgErrorResponse, body.Bytes())
	return out.Bytes()
}

// A FATAL ErrorResponse is PostgreSQL's last message before it closes the
// socket — no ReadyForQuery follows. The read loops must surface the captured
// diagnostic, not the EOF that follows it, while keeping the I/O error in the
// chain so the pool still classifies the connection as dead.

func TestProcessQueryResponses_FatalDiagnosticSurvivesConnClose(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildFatalErrorResponse("57P01", "terminating connection due to administrator command"))
	// No ReadyForQuery: the backend closed the connection (reader hits EOF).

	c := newTestReadOnlyConn(input.Bytes())
	err := c.processQueryResponses(context.Background(), nil)
	require.Error(t, err)

	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag, "the backend's FATAL diagnostic must survive the connection loss")
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
	assert.Equal(t, "terminating connection due to administrator command", diag.Message)
	assert.True(t, mterrors.IsConnectionError(err),
		"the read failure must stay in the chain so the pool discards the dead conn")
}

func TestProcessExecuteResponses_FatalDiagnosticSurvivesConnClose(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildFatalErrorResponse("57P01", "terminating connection due to administrator command"))

	c := newTestReadOnlyConn(input.Bytes())
	_, err := c.processExecuteResponses(context.Background(), nil)
	require.Error(t, err)

	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag, "extended-protocol read loop must also surface the FATAL")
	assert.Equal(t, "FATAL", diag.Severity)
	assert.Equal(t, "57P01", diag.Code)
	assert.True(t, mterrors.IsConnectionError(err))
}

func TestProcessQueryResponses_PlainEOFStaysAnIOError(t *testing.T) {
	// Connection dies with no prior ErrorResponse: the error must remain the
	// plain read failure, with no diagnostic conjured out of thin air.
	c := newTestReadOnlyConn(nil)
	err := c.processQueryResponses(context.Background(), nil)
	require.Error(t, err)

	var diag *mterrors.PgDiagnostic
	assert.False(t, errors.As(err, &diag), "no PgDiagnostic expected on a bare EOF")
	assert.True(t, mterrors.IsConnectionError(err))
}
