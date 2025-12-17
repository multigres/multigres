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
