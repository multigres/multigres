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

package plpgsqlast

// PLpgSQL_function is the root of a parsed PL/pgSQL function body.
// Ported from postgres/src/pl/plpgsql/src/plpgsql.h (PLpgSQL_function struct).
//
// As in PG, the body is a single top-level block (PG's function->action). The
// many execution-engine fields of PG's struct (datum array, resolution flags,
// etc.) are intentionally omitted — this is a parse tree for static analysis.
type PLpgSQL_function struct {
	BaseNode
	Action *PLpgSQL_stmt_block `json:"action,omitempty"` // the function body block
}

func (n *PLpgSQL_function) String() string {
	return "PLpgSQL_function"
}

func (n *PLpgSQL_function) SqlString() string {
	if n.Action == nil {
		return ""
	}
	return n.Action.SqlString()
}

func NewPLpgSQL_function() *PLpgSQL_function {
	return &PLpgSQL_function{
		BaseNode: BaseNode{Tag: T_PLpgSQL_function, Loc: -1},
	}
}
