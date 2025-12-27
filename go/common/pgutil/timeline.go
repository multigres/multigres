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

// Package pgutil provides PostgreSQL-related utilities.
package pgutil

import (
	"strconv"
	"strings"
)

// TimelineForkPoint represents where a timeline forked from its parent.
type TimelineForkPoint struct {
	// TimelineID is the timeline that was created.
	TimelineID int64
	// ParentTimelineID is the timeline it forked from.
	ParentTimelineID int64
	// ForkLSN is the LSN where the fork occurred.
	ForkLSN string
}

// ParseTimelineHistory parses the content of a PostgreSQL timeline history file.
// The format is one entry per line: <parent_tli>\t<switch_lsn>\t<reason>
// Lines starting with # are comments and are ignored.
// The timelineID parameter is the timeline whose history file this is.
func ParseTimelineHistory(timelineID int64, content []byte) []TimelineForkPoint {
	var points []TimelineForkPoint

	for line := range strings.SplitSeq(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "\t", 3)
		if len(parts) < 2 {
			continue
		}

		parentTLI, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			continue
		}

		forkLSN := strings.TrimSpace(parts[1])

		// The history file for timeline N contains entries for timelines 1 to N-1.
		// Each entry shows where that timeline's successor forked.
		// So parent_tli=1 with fork_lsn=X means timeline 2 forked from timeline 1 at X.
		points = append(points, TimelineForkPoint{
			TimelineID:       parentTLI + 1, // The timeline created by this fork
			ParentTimelineID: parentTLI,
			ForkLSN:          forkLSN,
		})
	}

	return points
}
