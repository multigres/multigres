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

// A body must be a block; empty input is a parse error (matching PG).
func TestParsePLpgSQL_EmptyIsError(t *testing.T) {
	_, err := ParsePLpgSQL("")
	require.Error(t, err)
}

// The simplest valid body: an empty block becomes the function's Action.
func TestParsePLpgSQL_EmptyBlock(t *testing.T) {
	fn, err := ParsePLpgSQL("BEGIN END")
	require.NoError(t, err)
	require.NotNil(t, fn)
	assert.Equal(t, plpgsqlast.T_PLpgSQL_function, fn.NodeTag())
	require.NotNil(t, fn.Action)
	assert.Equal(t, plpgsqlast.T_PLpgSQL_stmt_block, fn.Action.NodeTag())
	assert.Empty(t, fn.Action.Body)
}
