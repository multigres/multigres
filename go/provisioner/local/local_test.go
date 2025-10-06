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

package local

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateUnixSocketPathLength(t *testing.T) {
	provisioner := &localProvisioner{}

	tests := []struct {
		name        string
		rootDir     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "short path should pass",
			rootDir:     "/tmp/mt",
			expectError: false,
		},
		{
			name:        "medium path should pass",
			rootDir:     "/home/user/multigres",
			expectError: false,
		},
		{
			name:        "very long path should fail",
			rootDir:     "/Users/very/long/path/that/will/definitely/exceed/the/unix/socket/path/limit/for/postgresql/sockets",
			expectError: true,
			errorMsg:    "Unix socket path would exceed system limit",
		},
		{
			name:        "relative path should be converted to absolute",
			rootDir:     "./short",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &LocalProvisionerConfig{
				RootWorkingDir: tt.rootDir,
			}

			err := provisioner.validateUnixSocketPathLength(config)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}

				// For path length errors, verify the error message contains helpful guidance
				if strings.Contains(tt.errorMsg, "exceed system limit") {
					assert.Contains(t, err.Error(), "To fix this issue:")
					assert.Contains(t, err.Error(), "Initialize multigres from a directory with a shorter path")
					assert.Contains(t, err.Error(), "Provide config-path to multigres")
					assert.Contains(t, err.Error(), "Better:  multigres cluster init --config-path /tmp/mt/")
					assert.Contains(t, err.Error(), "This will generate socket paths like:")
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
