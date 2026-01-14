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

package regular

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "failed to read message",
			err:      errors.New("failed to read message: EOF"),
			expected: true,
		},
		{
			name:     "failed to read field count",
			err:      errors.New("failed to read field count: unexpected EOF"),
			expected: true,
		},
		{
			name:     "failed to write",
			err:      errors.New("failed to write: broken pipe"),
			expected: true,
		},
		{
			name:     "connection reset",
			err:      errors.New("connection reset by peer"),
			expected: true,
		},
		{
			name:     "broken pipe",
			err:      errors.New("write tcp 127.0.0.1:5432: broken pipe"),
			expected: true,
		},
		{
			name:     "use of closed network connection",
			err:      errors.New("read tcp: use of closed network connection"),
			expected: true,
		},
		{
			name:     "SQL error - not a connection error",
			err:      errors.New("ERROR: relation \"users\" does not exist"),
			expected: false,
		},
		{
			name:     "timeout error - not a connection error",
			err:      errors.New("context deadline exceeded"),
			expected: false,
		},
		{
			name:     "authentication error - not a connection error",
			err:      errors.New("password authentication failed for user \"test\""),
			expected: false,
		},
		{
			name:     "permission denied - not a connection error",
			err:      errors.New("permission denied for table users"),
			expected: false,
		},
		{
			name:     "parse error - not a connection error",
			err:      errors.New("failed to parse SQL: syntax error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConnectionError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
