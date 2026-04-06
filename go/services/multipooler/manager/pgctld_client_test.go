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

package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// TestProtectedPgctldClient_StateChangingOperationsRequireLock verifies that all
// state-changing operations (Start, Stop, Restart, InitDataDir, PgRewind, ReloadConfig)
// require the action lock to be held.
func TestProtectedPgctldClient_StateChangingOperationsRequireLock(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_STOPPED,
		},
	}
	protected := NewProtectedPgctldClient(mockClient)

	t.Run("StartAsStandby requires lock", func(t *testing.T) {
		_, err := protected.StartAsStandby(ctx, &pgctldpb.StartAsStandbyRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "action lock")
	})

	t.Run("Stop requires lock", func(t *testing.T) {
		_, err := protected.Stop(ctx, &pgctldpb.StopRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "action lock")
	})

	t.Run("Restart requires lock", func(t *testing.T) {
		_, err := protected.Restart(ctx, &pgctldpb.RestartRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "action lock")
	})

	t.Run("InitDataDir requires lock", func(t *testing.T) {
		_, err := protected.InitDataDir(ctx, &pgctldpb.InitDataDirRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "action lock")
	})

	t.Run("PgRewind requires lock", func(t *testing.T) {
		_, err := protected.PgRewind(ctx, &pgctldpb.PgRewindRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "action lock")
	})

	t.Run("ReloadConfig requires lock", func(t *testing.T) {
		_, err := protected.ReloadConfig(ctx, &pgctldpb.ReloadConfigRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "action lock")
	})
}

// TestProtectedPgctldClient_ReadOnlyOperationsNoLockRequired verifies that read-only
// operations (Status) can be called without holding the action lock.
func TestProtectedPgctldClient_ReadOnlyOperationsNoLockRequired(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
		},
	}
	protected := NewProtectedPgctldClient(mockClient)

	t.Run("Status does not require lock", func(t *testing.T) {
		resp, err := protected.Status(ctx, &pgctldpb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, pgctldpb.ServerStatus_RUNNING, resp.Status)
	})
}

// TestProtectedPgctldClient_WithLockHeld verifies that state-changing operations
// succeed when the action lock is held.
func TestProtectedPgctldClient_WithLockHeld(t *testing.T) {
	ctx := context.Background()
	actionLock := NewActionLock()

	// Acquire the lock
	lockCtx, err := actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer actionLock.Release(lockCtx)

	mockClient := &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
		},
	}
	protected := NewProtectedPgctldClient(mockClient)

	t.Run("StartAsStandby succeeds with lock", func(t *testing.T) {
		resp, err := protected.StartAsStandby(lockCtx, &pgctldpb.StartAsStandbyRequest{})
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.True(t, mockClient.startAsStandbyCalled)
	})

	t.Run("Restart succeeds with lock", func(t *testing.T) {
		resp, err := protected.Restart(lockCtx, &pgctldpb.RestartRequest{})
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.True(t, mockClient.restartCalled)
	})
}
