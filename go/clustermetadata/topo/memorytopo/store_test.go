// Copyright 2025 Supabase, Inc.
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

package memorytopo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/test"
)

func TestMemoryTopo(t *testing.T) {
	// Run the TopoServerTestSuite tests.
	ctx := t.Context()
	test.TopoServerTestSuite(t, ctx, func() topo.Store {
		return NewServer(ctx, test.LocalCellName)
	})
}

func TestLockWithTTLExpiration(t *testing.T) {
	ctx := context.Background()
	ts := NewServer(ctx, test.LocalCellName)
	defer ts.Close()

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err)

	// Create a node to lock
	_, err = conn.Create(ctx, "test_ttl_node", []byte("data"))
	require.NoError(t, err)

	// Lock with a short TTL
	ttl := 100 * time.Millisecond
	lockDesc, err := conn.LockWithTTL(ctx, "test_ttl_node", "holder1", ttl)
	require.NoError(t, err)

	// Immediately trying to lock again should fail (lock is held)
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = conn.Lock(ctx2, "test_ttl_node", "holder2")
	require.Error(t, err, "Lock should fail while TTL lock is held")

	// Wait for TTL to expire and verify we can acquire the lock
	var lockDesc2 topo.LockDescriptor
	require.Eventually(t, func() bool {
		ctx3, cancel3 := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel3()
		lockDesc2, err = conn.Lock(ctx3, "test_ttl_node", "holder2")
		return err == nil
	}, 500*time.Millisecond, 20*time.Millisecond, "Lock should succeed after TTL expiration")

	err = lockDesc2.Unlock(ctx)
	require.NoError(t, err)

	// Original unlock should fail (lock was already expired)
	err = lockDesc.Unlock(ctx)
	require.Error(t, err, "Unlock should fail after TTL expiration")
}

func TestLockNameWithTTLExpiration(t *testing.T) {
	ctx := context.Background()
	ts := NewServer(ctx, test.LocalCellName)
	defer ts.Close()

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	require.NoError(t, err)

	// LockNameWithTTL should work on a non-existent path
	ttl := 100 * time.Millisecond
	lockDesc, err := conn.LockNameWithTTL(ctx, "test_named_ttl", "holder1", ttl)
	require.NoError(t, err)

	// Wait for TTL to expire and verify we can acquire the lock
	var lockDesc2 topo.LockDescriptor
	require.Eventually(t, func() bool {
		ctx2, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		lockDesc2, err = conn.LockNameWithTTL(ctx2, "test_named_ttl", "holder2", ttl)
		return err == nil
	}, 500*time.Millisecond, 20*time.Millisecond, "LockNameWithTTL should succeed after TTL expiration")

	err = lockDesc2.Unlock(ctx)
	require.NoError(t, err)

	// Original unlock should fail (lock was already expired)
	err = lockDesc.Unlock(ctx)
	require.Error(t, err, "Unlock should fail after TTL expiration")
}
