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

package preparedstatement

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
)

// A syntactically invalid prepared statement must surface as a 42601
// syntax_error diagnostic (via mterrors.NewParseError), so clients see the same
// SQLSTATE PostgreSQL would send rather than an opaque internal error.
func TestConsolidator_AddPreparedStatement_SyntaxError(t *testing.T) {
	c := NewConsolidator()
	_, err := c.AddPreparedStatement(1, "bad", "SELECT FROM WHERE", nil)
	require.Error(t, err)
	require.Equal(t, mterrors.PgSSSyntaxError, mterrors.ExtractSQLSTATE(err))
}
