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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarkersToSelection(t *testing.T) {
	// "*" -> all_tables; "schema.*" -> a schema object; "schema.table" -> a table object.
	all, objs := markersToSelection([]string{"*", "sales.*", "public.orders"})
	require.True(t, all)
	require.Len(t, objs, 2)
	require.Equal(t, "sales", objs[0].GetSchema())
	require.Equal(t, "public.orders", objs[1].GetTable().GetQualifiedName())

	// No "*": all_tables stays false.
	all, objs = markersToSelection([]string{"public.orders"})
	require.False(t, all)
	require.Len(t, objs, 1)
	require.Equal(t, "public.orders", objs[0].GetTable().GetQualifiedName())

	// Empty input.
	all, objs = markersToSelection(nil)
	require.False(t, all)
	require.Empty(t, objs)
}
