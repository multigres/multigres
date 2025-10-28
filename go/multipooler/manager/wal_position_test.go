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

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
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

func TestIsWALPositionUpToDate(t *testing.T) {
	tests := []struct {
		name              string
		candidateLogIndex int64
		candidateLogTerm  int64
		ourWAL            *consensusdatapb.WALPosition
		expected          bool
	}{
		{
			name:              "candidate has higher term",
			candidateLogIndex: 100,
			candidateLogTerm:  5,
			ourWAL: &consensusdatapb.WALPosition{
				LogIndex: 200,
				LogTerm:  4,
			},
			expected: true,
		},
		{
			name:              "candidate has lower term",
			candidateLogIndex: 200,
			candidateLogTerm:  3,
			ourWAL: &consensusdatapb.WALPosition{
				LogIndex: 100,
				LogTerm:  4,
			},
			expected: false,
		},
		{
			name:              "same term, candidate has higher index",
			candidateLogIndex: 200,
			candidateLogTerm:  5,
			ourWAL: &consensusdatapb.WALPosition{
				LogIndex: 100,
				LogTerm:  5,
			},
			expected: true,
		},
		{
			name:              "same term, candidate has lower index",
			candidateLogIndex: 100,
			candidateLogTerm:  5,
			ourWAL: &consensusdatapb.WALPosition{
				LogIndex: 200,
				LogTerm:  5,
			},
			expected: false,
		},
		{
			name:              "same term and index",
			candidateLogIndex: 150,
			candidateLogTerm:  5,
			ourWAL: &consensusdatapb.WALPosition{
				LogIndex: 150,
				LogTerm:  5,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsWALPositionUpToDate(tt.candidateLogIndex, tt.candidateLogTerm, tt.ourWAL)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCompareWALPositions(t *testing.T) {
	tests := []struct {
		name     string
		pos1     *consensusdatapb.WALPosition
		pos2     *consensusdatapb.WALPosition
		expected int
	}{
		{
			name:     "pos1 has higher term",
			pos1:     &consensusdatapb.WALPosition{LogTerm: 5, LogIndex: 100},
			pos2:     &consensusdatapb.WALPosition{LogTerm: 4, LogIndex: 200},
			expected: 1,
		},
		{
			name:     "pos1 has lower term",
			pos1:     &consensusdatapb.WALPosition{LogTerm: 3, LogIndex: 200},
			pos2:     &consensusdatapb.WALPosition{LogTerm: 4, LogIndex: 100},
			expected: -1,
		},
		{
			name:     "same term, pos1 has higher index",
			pos1:     &consensusdatapb.WALPosition{LogTerm: 5, LogIndex: 200},
			pos2:     &consensusdatapb.WALPosition{LogTerm: 5, LogIndex: 100},
			expected: 1,
		},
		{
			name:     "same term, pos1 has lower index",
			pos1:     &consensusdatapb.WALPosition{LogTerm: 5, LogIndex: 100},
			pos2:     &consensusdatapb.WALPosition{LogTerm: 5, LogIndex: 200},
			expected: -1,
		},
		{
			name:     "identical positions",
			pos1:     &consensusdatapb.WALPosition{LogTerm: 5, LogIndex: 150},
			pos2:     &consensusdatapb.WALPosition{LogTerm: 5, LogIndex: 150},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareWALPositions(tt.pos1, tt.pos2)
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
