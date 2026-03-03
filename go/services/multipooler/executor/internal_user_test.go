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

package executor

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewExecutor(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Test that NewExecutor creates an executor without caching internal user
	executor := NewExecutor(logger, nil, nil)

	assert.NotNil(t, executor)
	assert.NotNil(t, executor.logger)
	assert.NotNil(t, executor.consolidator)
}

func TestDefaultInternalUser(t *testing.T) {
	// Verify the default constant hasn't changed unexpectedly
	assert.Equal(t, "postgres", DefaultInternalUser)
}
