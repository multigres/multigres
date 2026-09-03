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

package test

import (
	"context"
	"errors"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkPutEphemeral tests the backend-agnostic semantics of PutEphemeral:
// create, overwrite, delete, and re-create. Liveness behavior (expiry after
// process death, re-creation after lease loss) is backend-specific and
// covered by the etcd end-to-end tests.
func checkPutEphemeral(t *testing.T, ctx context.Context, ts topoclient.Store) {
	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
	require.NoError(t, err, "ConnForCell(global) failed")

	filePath := "ephemeral/test/File"

	// Put creates the file.
	err = conn.PutEphemeral(ctx, filePath, []byte("v1"))
	require.NoError(t, err, "PutEphemeral(create) failed")
	contents, _, err := conn.Get(ctx, filePath)
	require.NoError(t, err, "Get after PutEphemeral failed")
	assert.Equal(t, []byte("v1"), contents)

	// Put overwrites unconditionally.
	err = conn.PutEphemeral(ctx, filePath, []byte("v2"))
	require.NoError(t, err, "PutEphemeral(overwrite) failed")
	contents, _, err = conn.Get(ctx, filePath)
	require.NoError(t, err, "Get after overwrite failed")
	assert.Equal(t, []byte("v2"), contents)

	// Delete removes it.
	err = conn.Delete(ctx, filePath, nil)
	require.NoError(t, err, "Delete of ephemeral file failed")
	_, _, err = conn.Get(ctx, filePath)
	assert.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}),
		"Get after Delete should return NoNode, got: %v", err)

	// Re-create after delete works.
	err = conn.PutEphemeral(ctx, filePath, []byte("v3"))
	require.NoError(t, err, "PutEphemeral(re-create) failed")
	contents, _, err = conn.Get(ctx, filePath)
	require.NoError(t, err, "Get after re-create failed")
	assert.Equal(t, []byte("v3"), contents)
}

// checkClaimEphemeral tests the backend-agnostic semantics of
// ClaimEphemeral: atomic claim when absent, refresh when held by the same
// owner, NodeExists when held by another, and claimable again after delete.
func checkClaimEphemeral(t *testing.T, ctx context.Context, ts topoclient.Store) {
	conn, err := ts.ConnForCell(ctx, topoclient.GlobalCell)
	require.NoError(t, err, "ConnForCell(global) failed")

	filePath := "ephemeral/claims/7"

	// Absent: first claimant wins.
	err = conn.ClaimEphemeral(ctx, filePath, []byte("owner-a"))
	require.NoError(t, err, "ClaimEphemeral(absent) failed")

	// Held by another: rejected, and the holder's contents are untouched.
	err = conn.ClaimEphemeral(ctx, filePath, []byte("owner-b"))
	assert.True(t, errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}),
		"ClaimEphemeral(held by another) should return NodeExists, got: %v", err)
	contents, _, err := conn.Get(ctx, filePath)
	require.NoError(t, err)
	assert.Equal(t, []byte("owner-a"), contents, "a losing claim must not overwrite the holder")

	// Held by the same owner: refresh succeeds.
	err = conn.ClaimEphemeral(ctx, filePath, []byte("owner-a"))
	require.NoError(t, err, "ClaimEphemeral(refresh own claim) failed")

	// Released: claimable by the next owner.
	require.NoError(t, conn.Delete(ctx, filePath, nil))
	err = conn.ClaimEphemeral(ctx, filePath, []byte("owner-b"))
	require.NoError(t, err, "ClaimEphemeral after release failed")
}
