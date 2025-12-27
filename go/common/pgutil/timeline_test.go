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

package pgutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTimelineHistory(t *testing.T) {
	tests := []struct {
		name       string
		timelineID int64
		content    string
		expected   []TimelineForkPoint
	}{
		{
			name:       "empty content",
			timelineID: 2,
			content:    "",
			expected:   nil,
		},
		{
			name:       "single entry",
			timelineID: 2,
			content:    "1\t0/5000000\tbefore 2024-01-01 12:00:00+00\n",
			expected: []TimelineForkPoint{
				{TimelineID: 2, ParentTimelineID: 1, ForkLSN: "0/5000000"},
			},
		},
		{
			name:       "multiple entries",
			timelineID: 4,
			content: `1	0/5000000	before 2024-01-01 12:00:00+00
2	0/A000000	before 2024-01-02 12:00:00+00
3	0/F000000	before 2024-01-03 12:00:00+00
`,
			expected: []TimelineForkPoint{
				{TimelineID: 2, ParentTimelineID: 1, ForkLSN: "0/5000000"},
				{TimelineID: 3, ParentTimelineID: 2, ForkLSN: "0/A000000"},
				{TimelineID: 4, ParentTimelineID: 3, ForkLSN: "0/F000000"},
			},
		},
		{
			name:       "with comments and empty lines",
			timelineID: 2,
			content: `# This is a comment
1	0/5000000	recovery point

# Another comment
`,
			expected: []TimelineForkPoint{
				{TimelineID: 2, ParentTimelineID: 1, ForkLSN: "0/5000000"},
			},
		},
		{
			name:       "malformed line skipped",
			timelineID: 2,
			content: `invalid line without tabs
1	0/5000000	valid entry
also invalid
`,
			expected: []TimelineForkPoint{
				{TimelineID: 2, ParentTimelineID: 1, ForkLSN: "0/5000000"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseTimelineHistory(tt.timelineID, []byte(tt.content))
			assert.Equal(t, tt.expected, result)
		})
	}
}
