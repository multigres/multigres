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

// The grammar currently only accepts an empty body; real statement parsing is
// added as the grammar is ported. These tests just cover the scaffolding.

func TestParsePLpgSQL_Empty(t *testing.T) {
	fn, err := ParsePLpgSQL("")

	require.NoError(t, err)
	require.NotNil(t, fn)
	assert.Equal(t, plpgsqlast.T_PLpgSQL_function, fn.NodeTag())
	assert.Nil(t, fn.Action)
}
