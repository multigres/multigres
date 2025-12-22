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

package executor

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewExecutor_ConfigurableInternalUser(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	tests := []struct {
		name         string
		internalUser string
		expected     string
	}{
		{
			name:         "custom user specified",
			internalUser: "custom_user",
			expected:     "custom_user",
		},
		{
			name:         "empty string defaults to default user",
			internalUser: "",
			expected:     DefaultInternalUser,
		},
		{
			name:         "another custom user",
			internalUser: "admin",
			expected:     "admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewExecutor(logger, nil, tt.internalUser)
			assert.Equal(t, tt.expected, executor.internalUser, 
				"Expected internal user to be %s, got %s", tt.expected, executor.internalUser)
		})
	}
}

func TestDefaultInternalUser(t *testing.T) {
	// Verify the default constant hasn't changed unexpectedly
	assert.Equal(t, "postgres", DefaultInternalUser)
}