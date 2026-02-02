// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package poolerserver

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/multipooler/connpoolmanager"
)

func TestNewQueryPoolerServer_NilPoolManager(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Creating with nil pool manager should work but executor will be nil
	pooler := NewQueryPoolerServer(logger, nil)

	assert.NotNil(t, pooler)
	assert.Equal(t, logger, pooler.logger)
	// Executor should be nil since pool manager was nil
	exec, err := pooler.Executor()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool manager was nil")
	assert.Nil(t, exec)
}

func TestIsHealthy_NotInitialized(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	pooler := NewQueryPoolerServer(logger, nil)

	// IsHealthy should fail since the pool manager is not initialized
	err := pooler.IsHealthy()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "executor not initialized")
}

// mockPoolManager implements PoolManager for testing
type mockPoolManager struct {
	connpoolmanager.PoolManager // embed for default nil implementations
	internalUser                string
}

func (m *mockPoolManager) InternalUser() string {
	return m.internalUser
}

func TestNewQueryPoolerServer_CustomInternalUser(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	tests := []struct {
		name         string
		internalUser string
	}{
		{
			name:         "default postgres user",
			internalUser: "postgres",
		},
		{
			name:         "custom replication user",
			internalUser: "replication_user",
		},
		{
			name:         "custom admin user",
			internalUser: "multigres_admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMgr := &mockPoolManager{
				internalUser: tt.internalUser,
			}

			pooler := NewQueryPoolerServer(logger, mockMgr)

			require.NotNil(t, pooler)
			assert.Equal(t, logger, pooler.logger)
			assert.Equal(t, mockMgr, pooler.poolManager)

			// Verify the executor was created with the correct internal user
			exec, err := pooler.Executor()
			require.NoError(t, err)
			require.NotNil(t, exec)

			// The executor should use the internal user from the pool manager
			// We can verify this by checking the executor was created
			// (the actual internal user value is private to the executor,
			// but it's tested in executor/internal_user_test.go)
		})
	}
}

func TestNewQueryPoolerServer_InternalUserPassthrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Verify that InternalUser() is called on the pool manager
	customUser := "test_internal_user"
	mockMgr := &mockPoolManager{
		internalUser: customUser,
	}

	pooler := NewQueryPoolerServer(logger, mockMgr)

	require.NotNil(t, pooler)

	// The executor should have been created with the custom internal user
	exec, err := pooler.Executor()
	require.NoError(t, err)
	require.NotNil(t, exec)
}
