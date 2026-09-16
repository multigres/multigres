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

package engine

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// TestEnableUnsafeConnection confirms the primitive latches unsafe connection on
// the connection and replies with a bare "SET" CommandComplete, touching no backend.
func TestEnableUnsafeConnection(t *testing.T) {
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	require.False(t, conn.UnsafeConnection())

	prim := NewEnableUnsafeConnection("SET multigres.unsafe_connection = on")

	var got []*sqltypes.Result
	err := prim.StreamExecute(context.Background(), nil, conn, nil, nil, PlanExecInfo{},
		func(_ context.Context, r *sqltypes.Result) error {
			got = append(got, r)
			return nil
		})
	require.NoError(t, err)

	assert.True(t, conn.UnsafeConnection(), "connection must be latched into unsafe connection")
	require.Len(t, got, 1)
	assert.Equal(t, "SET", got[0].CommandTag)
}
