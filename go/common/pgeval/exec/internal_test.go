// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	_ "github.com/multigres/multigres/go/common/pgeval/funcs"
)

// A Go panic inside a function body is a bug, not a SQL error: Eval must
// let it propagate instead of turning it into a diagnostic.
func TestGoPanicIsNotRecovered(t *testing.T) {
	int4pl := pgcatalog.ProcsByName("int4pl")[0].Oid
	args := ast.NewNodeList(
		ast.NewConst(pgoid.INT4OID, datum.Int32GetDatum(1), false),
		ast.NewConst(pgoid.INT4OID, datum.Int32GetDatum(2), false))
	state, err := Prepare(ast.NewFuncExpr(int4pl, pgoid.INT4OID, args))
	require.NoError(t, err)

	fn := state.root.(*funcStep)
	fn.fcinfo.Flinfo = &fmgr.FmgrInfo{FnAddr: func(fmgr.FunctionCallInfo) datum.Datum { panic("boom") }}
	require.PanicsWithValue(t, "boom", func() { _, _ = state.Eval(nil) })
}

// Constant arguments are preloaded into the call frame at prepare time;
// only the others have steps.
func TestConstantArgumentsArePreloaded(t *testing.T) {
	int4pl := pgcatalog.ProcsByName("int4pl")[0].Oid
	args := ast.NewNodeList(
		ast.NewConst(pgoid.INT4OID, datum.Int32GetDatum(1), false),
		ast.NewParam(ast.PARAM_EXTERN, 1, pgoid.INT4OID))
	state, err := Prepare(ast.NewFuncExpr(int4pl, pgoid.INT4OID, args))
	require.NoError(t, err)

	fn := state.root.(*funcStep)
	require.Nil(t, fn.args[0])
	require.Equal(t, int32(1), datum.DatumGetInt32(fn.fcinfo.Args[0].Value))
	require.IsType(t, &paramStep{}, fn.args[1])
}
