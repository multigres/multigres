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

package command

import (
	"testing"
)

// TestPostgresAlreadyRunningPattern verifies the regex pattern matches the actual error
func TestPostgresAlreadyRunningPattern(t *testing.T) {
	testCases := []struct {
		name     string
		output   string
		expected bool
	}{
		{
			name:     "actual postgres error",
			output:   `FATAL:  lock file "postmaster.pid" already exists`,
			expected: true,
		},
		{
			name:     "full error with hint",
			output:   `FATAL:  lock file "postmaster.pid" already exists\nHINT:  Is another postmaster (PID 12345) running in data directory "/data"?`,
			expected: true,
		},
		{
			name:     "other lock file",
			output:   `FATAL:  lock file "other.lock" already exists`,
			expected: true,
		},
		{
			name:     "different error",
			output:   `FATAL:  database system is in recovery mode`,
			expected: false,
		},
		{
			name:     "similar but not exact",
			output:   `lock file is missing`,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := postgresAlreadyRunningPattern.MatchString(tc.output)
			if result != tc.expected {
				t.Errorf("Pattern match = %v, want %v for output: %q",
					result, tc.expected, tc.output)
			}
		})
	}
}
