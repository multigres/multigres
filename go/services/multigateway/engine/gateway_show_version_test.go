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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/sqltypes"
)

func collectResults(t *testing.T, run func(func(context.Context, *sqltypes.Result) error) error) []*sqltypes.Result {
	t.Helper()
	var results []*sqltypes.Result
	err := run(func(_ context.Context, r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	})
	require.NoError(t, err)
	return results
}

// TestGatewayShowVersion_StreamExecute checks the GUC surface: a single text
// column named multigres_version carrying the short release version.
func TestGatewayShowVersion_StreamExecute(t *testing.T) {
	prim := NewGatewayShowVersion("SHOW multigres_version")

	results := collectResults(t, func(cb func(context.Context, *sqltypes.Result) error) error {
		return prim.StreamExecute(context.Background(), nil, nil, nil, nil, PlanExecInfo{}, cb)
	})

	require.Len(t, results, 1)
	require.Len(t, results[0].Fields, 1)
	assert.Equal(t, "multigres_version", results[0].Fields[0].Name)
	assert.Equal(t, uint32(ast.TEXTOID), results[0].Fields[0].DataTypeOid)
	assert.Equal(t, "SHOW", results[0].CommandTag)
	require.Len(t, results[0].Rows, 1)
	require.Len(t, results[0].Rows[0].Values, 1)
	assert.Equal(t, servenv.Version(), string(results[0].Rows[0].Values[0]))
}

// TestGatewayShowVersion_PortalStreamExecute pins the extended-protocol contract:
// RowDescription is delivered by Describe, so Execute must attach Fields only
// when a Describe('P') was folded into it (includeDescribe). Attaching Fields
// otherwise would emit an illegal second RowDescription mid-Execute.
func TestGatewayShowVersion_PortalStreamExecute(t *testing.T) {
	prim := NewGatewayShowVersion("SHOW multigres_version")

	run := func(includeDescribe bool) *sqltypes.Result {
		results := collectResults(t, func(cb func(context.Context, *sqltypes.Result) error) error {
			return prim.PortalStreamExecute(context.Background(), nil, nil, nil, nil, 0, includeDescribe, PlanExecInfo{}, cb)
		})
		require.Len(t, results, 1)
		return results[0]
	}

	t.Run("without folded describe omits fields", func(t *testing.T) {
		res := run(false)
		assert.Nil(t, res.Fields, "Execute must not carry Fields when RowDescription came from a separate Describe")
		require.Len(t, res.Rows, 1)
	})

	t.Run("with folded describe carries fields", func(t *testing.T) {
		res := run(true)
		require.Len(t, res.Fields, 1)
		assert.Equal(t, "multigres_version", res.Fields[0].Name)
		require.Len(t, res.Rows, 1)
	})
}

// TestIsMultigresVersionShow covers the SHOW matcher.
func TestIsMultigresVersionShow(t *testing.T) {
	assert.True(t, IsMultigresVersionShow(&ast.VariableShowStmt{Name: "multigres_version"}))
	assert.True(t, IsMultigresVersionShow(&ast.VariableShowStmt{Name: "Multigres_Version"}))
	assert.False(t, IsMultigresVersionShow(&ast.VariableShowStmt{Name: "statement_timeout"}))
	assert.False(t, IsMultigresVersionShow(&ast.SelectStmt{}))
	assert.False(t, IsMultigresVersionShow(nil))
}

// TestMultigresVersionShowDescription verifies the synthetic Describe response:
// no bind parameters and a single text column, so the extended-protocol Describe
// can be answered without a backend round-trip.
func TestMultigresVersionShowDescription(t *testing.T) {
	desc := MultigresVersionShowDescription()
	require.NotNil(t, desc.Parameters)
	assert.Empty(t, desc.Parameters, "SHOW takes no bind parameters")
	require.Len(t, desc.Fields, 1)
	assert.Equal(t, "multigres_version", desc.Fields[0].Name)
	assert.Equal(t, uint32(ast.TEXTOID), desc.Fields[0].DataTypeOid)
	assert.True(t, desc.HasFields)
}
