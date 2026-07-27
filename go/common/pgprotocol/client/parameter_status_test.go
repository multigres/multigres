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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// buildCommandComplete and buildParameterStatus are shared test helpers defined
// in copy_done_response_test.go and replication_test.go.

// TestProcessQueryResponses_ForwardsParameterStatus verifies the streaming read
// loop surfaces a backend ParameterStatus as its own Result (so a routed
// SET/RESET's GUC change reaches the caller) and records it on the connection.
func TestProcessQueryResponses_ForwardsParameterStatus(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildCommandComplete("SET"))
	input.Write(buildParameterStatus("client_encoding", "LATIN1"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

	c := newTestReadOnlyConn(input.Bytes())

	var forwarded []string
	err := c.processQueryResponses(context.Background(), func(_ context.Context, r *sqltypes.Result) error {
		if v, ok := r.ParameterStatus["client_encoding"]; ok {
			forwarded = append(forwarded, v)
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"LATIN1"}, forwarded, "ParameterStatus must be forwarded to the callback")
	assert.Equal(t, "LATIN1", c.serverParams["client_encoding"], "and recorded on the connection")
}

// TestQuery_AttachesTrailingParameterStatus verifies the buffered Query path
// attaches a ParameterStatus that arrives *after* CommandComplete (as ROLLBACK's
// revert does) to the finalized result rather than orphaning it.
func TestQuery_AttachesTrailingParameterStatus(t *testing.T) {
	var input bytes.Buffer
	input.Write(buildCommandComplete("ROLLBACK"))
	input.Write(buildParameterStatus("client_encoding", "LATIN1"))
	input.Write(buildReadyForQuery(protocol.TxnStatusIdle))

	c := newTestReadOnlyConn(input.Bytes())

	results, err := c.Query(context.Background(), "ROLLBACK")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "ROLLBACK", results[0].CommandTag)
	assert.Equal(t, map[string]string{"client_encoding": "LATIN1"}, results[0].ParameterStatus,
		"a post-CommandComplete ParameterStatus must attach to the finalized result")
}
