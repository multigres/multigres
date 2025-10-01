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

package main

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequiredFlags(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		expectError   bool
		errorContains string
	}{
		{
			name:          "missing database flag",
			args:          []string{"--cell", "testcell", "--table-group", "default"},
			expectError:   true,
			errorContains: "--database flag is required",
		},
		{
			name:          "missing table-group flag",
			args:          []string{"--cell", "testcell", "--database", "testdb"},
			expectError:   true,
			errorContains: "--table-group flag is required",
		},
		{
			name:          "missing both database and table-group flags",
			args:          []string{"--cell", "testcell"},
			expectError:   true,
			errorContains: "--database flag is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fresh command and MultiPooler instance
			cmd, _ := CreateMultiPoolerCommand()

			// Reset flags for reuse
			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				_ = flag.Value.Set(flag.DefValue)
				flag.Changed = false
			})

			// Set args and execute
			cmd.SetArgs(tt.args)
			err := cmd.Execute()

			// All these tests should fail with the expected error message
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}
