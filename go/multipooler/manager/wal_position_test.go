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

package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertLSNToIndex(t *testing.T) {
	tests := []struct {
		name     string
		lsn      string
		expected int64
	}{
		{
			name:     "zero LSN",
			lsn:      "0/0",
			expected: 0,
		},
		{
			name:     "simple LSN",
			lsn:      "0/1",
			expected: 1,
		},
		{
			name:     "LSN with high segment",
			lsn:      "1/0",
			expected: 1 << 32,
		},
		{
			name:     "complex LSN",
			lsn:      "0/1A2B3C4D",
			expected: 0x1A2B3C4D,
		},
		{
			name:     "LSN with both segments",
			lsn:      "2/ABCD1234",
			expected: (2 << 32) | 0xABCD1234,
		},
		{
			name:     "invalid LSN format",
			lsn:      "invalid",
			expected: 0,
		},
		{
			name:     "missing slash",
			lsn:      "12345",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertLSNToIndex(tt.lsn)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertLSNToIndex_Monotonicity(t *testing.T) {
	// Test that higher LSNs always produce higher indices
	lsns := []string{
		"0/0",
		"0/1",
		"0/FFFF",
		"0/10000",
		"0/FFFFFFFF",
		"1/0",
		"1/1",
		"2/0",
		"FF/FFFFFFFF",
	}

	var prevIndex int64 = -1
	for _, lsn := range lsns {
		index := ConvertLSNToIndex(lsn)
		assert.Greater(t, index, prevIndex, "LSN %s should produce higher index than previous", lsn)
		prevIndex = index
	}
}
