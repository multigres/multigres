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
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
)

func TestNewQueryPoolerServer_NilPoolManager(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Creating with nil pool manager should work but executor will be nil
	pooler := NewQueryPoolerServer(logger, nil, nil, nil)

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
	pooler := NewQueryPoolerServer(logger, nil, nil, nil)

	// IsHealthy should fail since the pool manager is not initialized
	err := pooler.IsHealthy()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "executor not initialized")
}

// mockPoolManager implements PoolManager for testing
type mockPoolManager struct {
	connpoolmanager.PoolManager // embed for default nil implementations
}

func TestNewQueryPoolerServer_WithPoolManager(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	mockMgr := &mockPoolManager{}
	pooler := NewQueryPoolerServer(logger, mockMgr, nil, nil)

	require.NotNil(t, pooler)
	assert.Equal(t, logger, pooler.logger)
	assert.Equal(t, mockMgr, pooler.poolManager)

	exec, err := pooler.Executor()
	require.NoError(t, err)
	require.NotNil(t, exec)
}

func newStartRequestTestServer() *QueryPoolerServer {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &QueryPoolerServer{
		logger:        logger,
		servingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		gracePeriod:   defaultGracePeriod,
	}
}

func TestStartRequest_Serving(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING

	// Both allowOnShutdown=true and false should succeed when serving
	err := s.StartRequest(false)
	require.NoError(t, err)

	err = s.StartRequest(true)
	require.NoError(t, err)
}

func TestStartRequest_NotServing(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING

	// Both should fail when not serving
	err := s.StartRequest(false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotServing)

	err = s.StartRequest(true)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotServing)
}

func TestStartRequest_ShuttingDown_NewRequestRejected(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.shuttingDown = true

	// New requests (allowOnShutdown=false) should be rejected
	err := s.StartRequest(false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrShuttingDown)
}

func TestStartRequest_ShuttingDown_ExistingReservedAllowed(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.shuttingDown = true

	// Existing reserved connections (allowOnShutdown=true) should be allowed
	err := s.StartRequest(true)
	require.NoError(t, err)
}
