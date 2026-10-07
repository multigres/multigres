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

package multiadmin

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

func TestSetDatabaseReadOnly(t *testing.T) {
	srv := newTestServer(t)
	ctx := t.Context()
	require.NoError(t, srv.ts.CreateDatabase(ctx, "app", &clustermetadatapb.Database{Name: "app", Cells: []string{"cell1"}}))

	get := func() *clustermetadatapb.Database {
		db, err := srv.ts.GetDatabase(ctx, "app")
		require.NoError(t, err)
		return db
	}

	_, err := srv.SetDatabaseReadOnly(ctx, &multiadminpb.SetDatabaseReadOnlyRequest{Database: "app", ReadOnly: true})
	require.NoError(t, err)
	db := get()
	require.True(t, db.GetReadOnly())
	require.False(t, db.GetReadOnlyForce())
	require.Equal(t, []string{"cell1"}, db.GetCells(), "other fields are preserved")

	_, err = srv.SetDatabaseReadOnly(ctx, &multiadminpb.SetDatabaseReadOnlyRequest{Database: "app", ReadOnly: true, Force: true})
	require.NoError(t, err)
	require.True(t, get().GetReadOnlyForce())

	// Repeating the same request is a no-op, not an error.
	_, err = srv.SetDatabaseReadOnly(ctx, &multiadminpb.SetDatabaseReadOnlyRequest{Database: "app", ReadOnly: true, Force: true})
	require.NoError(t, err)

	// Lifting clears force too, even if the caller passes it.
	_, err = srv.SetDatabaseReadOnly(ctx, &multiadminpb.SetDatabaseReadOnlyRequest{Database: "app", ReadOnly: false, Force: true})
	require.NoError(t, err)
	db = get()
	require.False(t, db.GetReadOnly())
	require.False(t, db.GetReadOnlyForce())

	_, err = srv.SetDatabaseReadOnly(ctx, &multiadminpb.SetDatabaseReadOnlyRequest{Database: "missing", ReadOnly: true})
	require.Equal(t, codes.NotFound, status.Code(err))

	_, err = srv.SetDatabaseReadOnly(ctx, &multiadminpb.SetDatabaseReadOnlyRequest{ReadOnly: true})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
