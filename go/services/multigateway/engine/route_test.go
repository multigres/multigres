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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func TestRoutePortalPreservesPreparedQuery(t *testing.T) {
	const original = "SELECT  set_config('application_name', $1, false)"
	psi, err := preparedstatement.NewPreparedStatementInfo(&query.PreparedStatement{Name: "stmt", Query: original})
	require.NoError(t, err)
	portal := preparedstatement.NewPortalInfo(psi, &query.Portal{Name: "p"})
	state := handler.NewMultigatewayConnectionState()
	state.MarkReparsePending(psi.Name)
	exec := &mockIExecute{}

	// Formatting-only normalization must reuse the already-prepared SQL.
	route := NewRoute("default", "0", psi.AstStmt().SqlString(), nil)
	require.NoError(t, route.PortalStreamExecute(t.Context(), exec, nil, state, portal, 0, false, PlanExecInfo{}, nil))
	require.Same(t, portal, exec.lastPortalInfo)
	require.False(t, exec.lastPortalInfo.GetForceReparse())

	// A semantic rewrite still reaches the backend and refreshes its own cache
	// entry once. Reusing the original above must not consume this signal.
	const rewritten = "SELECT set_config('application_name', $1, true)"
	route = NewRoute("default", "0", rewritten, nil)
	require.NoError(t, route.PortalStreamExecute(t.Context(), exec, nil, state, portal, 0, false, PlanExecInfo{}, nil))
	require.Equal(t, rewritten, exec.lastPortalInfo.GetQuery())
	require.True(t, exec.lastPortalInfo.GetForceReparse())
	require.Same(t, portal.Portal, exec.lastPortalInfo.Portal)
	require.False(t, psi.GetForceReparse(), "shared statement metadata must not be mutated")
	require.NoError(t, route.PortalStreamExecute(t.Context(), exec, nil, state, portal, 0, false, PlanExecInfo{}, nil))
	require.False(t, exec.lastPortalInfo.GetForceReparse())
}
