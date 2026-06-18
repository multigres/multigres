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

package plpgsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
)

// parseBlock parses a body and returns its top-level block.
func parseBlock(t *testing.T, body string) *plpgsqlast.PLpgSQL_stmt_block {
	t.Helper()
	fn, err := ParsePLpgSQL(body)
	require.NoError(t, err)
	require.NotNil(t, fn.Action)
	return fn.Action
}

func TestParseBlock(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantLabel string
		wantBody  int // number of statements in the block body
	}{
		{"empty block", "BEGIN END", "", 0},
		{"empty block trailing semi", "BEGIN END;", "", 0},
		{"null only", "BEGIN NULL; END;", "", 0},            // NULL is discarded, like PG
		{"multiple nulls", "BEGIN NULL; NULL; END;", "", 0}, // both discarded
		{"labeled", "<<outer>> BEGIN END outer;", "outer", 0},
		{"labeled no end label", "<<outer>> BEGIN END;", "outer", 0},
		{"nested block", "BEGIN BEGIN NULL; END; END;", "", 1}, // one nested block
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := parseBlock(t, tt.body)
			assert.Equal(t, plpgsqlast.T_PLpgSQL_stmt_block, block.NodeTag())
			assert.Equal(t, tt.wantLabel, block.Label)
			assert.Len(t, block.Body, tt.wantBody)
		})
	}
}

// An unreserved keyword may be used as an identifier (here, a block label),
// matching PG's any_identifier: T_WORD | unreserved_keyword.
func TestParseUnreservedKeywordLabel(t *testing.T) {
	block := parseBlock(t, "<<query>> BEGIN END query;")
	assert.Equal(t, "query", block.Label, "the keyword text must flow through as the label")
}

// A nested block sits in the outer block's body as a PLpgSQL_stmt_block.
func TestParseNestedBlock(t *testing.T) {
	outer := parseBlock(t, "<<o>> BEGIN <<i>> BEGIN NULL; END i; END o;")
	assert.Equal(t, "o", outer.Label)
	require.Len(t, outer.Body, 1)

	inner, ok := outer.Body[0].(*plpgsqlast.PLpgSQL_stmt_block)
	require.True(t, ok, "nested statement should be a block")
	assert.Equal(t, "i", inner.Label)
	assert.Empty(t, inner.Body)
}

func TestParseBlockLabelErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"end label on unlabeled block", "BEGIN END x;"},
		{"mismatched labels", "<<a>> BEGIN END b;"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePLpgSQL(tt.body)
			require.Error(t, err)
		})
	}
}

func TestCheckLabels(t *testing.T) {
	assert.NoError(t, checkLabels("", ""))
	assert.NoError(t, checkLabels("a", ""))
	assert.NoError(t, checkLabels("a", "a"))
	assert.Error(t, checkLabels("", "x"))  // end label on unlabeled block
	assert.Error(t, checkLabels("a", "b")) // mismatch
}
