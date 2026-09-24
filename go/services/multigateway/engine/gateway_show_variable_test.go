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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func TestGatewayShowVariableProtocolFields(t *testing.T) {
	state := handler.NewMultigatewayConnectionState()
	state.SetStatementTimeout(5 * time.Second)
	prim := NewGatewayShowVariable("SHOW statement_timeout", "statement_timeout")

	check := func(t *testing.T, result *sqltypes.Result, wantFields bool) {
		t.Helper()
		assert.Equal(t, "SHOW", result.CommandTag)
		require.Len(t, result.Rows, 1)
		require.Len(t, result.Rows[0].Values, 1)
		assert.Equal(t, "5s", string(result.Rows[0].Values[0]))
		if !wantFields {
			assert.Nil(t, result.Fields, "Execute must not send RowDescription without a folded Describe")
			return
		}
		require.Len(t, result.Fields, 1)
		assert.Equal(t, "statement_timeout", result.Fields[0].Name)
		assert.Equal(t, uint32(25), result.Fields[0].DataTypeOid)
	}

	t.Run("simple query includes fields", func(t *testing.T) {
		results := collectResults(t, func(cb func(context.Context, *sqltypes.Result) error) error {
			return prim.StreamExecute(context.Background(), nil, nil, state, nil, PlanExecInfo{}, cb)
		})
		require.Len(t, results, 1)
		check(t, results[0], true)
	})

	for _, tc := range []struct {
		name            string
		includeDescribe bool
	}{
		{"extended execute without folded describe", false},
		{"extended execute with folded describe", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results := collectResults(t, func(cb func(context.Context, *sqltypes.Result) error) error {
				return prim.PortalStreamExecute(context.Background(), nil, nil, state, nil, 0, tc.includeDescribe, PlanExecInfo{}, cb)
			})
			require.Len(t, results, 1)
			check(t, results[0], tc.includeDescribe)
		})
	}
}
