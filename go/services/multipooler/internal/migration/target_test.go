// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDropTablesSQL(t *testing.T) {
	// No tables → no statement (DropTables no-ops on this).
	require.Equal(t, "", dropTablesSQL(nil))
	require.Equal(t, "", dropTablesSQL([]string{}))

	// Safe lowercase identifiers are not quoted; the drop is one IF EXISTS …
	// CASCADE over all tables so a re-run over pre-existing target tables (and any
	// dependents) succeeds.
	require.Equal(t, "DROP TABLE IF EXISTS public.orders CASCADE",
		dropTablesSQL([]string{"public.orders"}))
	require.Equal(t, "DROP TABLE IF EXISTS public.orders, app.items CASCADE",
		dropTablesSQL([]string{"public.orders", "app.items"}))

	// Identifiers needing quoting (mixed case / reserved) are quoted per part.
	got := dropTablesSQL([]string{"public.Order"})
	require.Contains(t, got, `"Order"`)
	require.True(t, strings.HasPrefix(got, "DROP TABLE IF EXISTS "))
	require.True(t, strings.HasSuffix(got, " CASCADE"))
}
