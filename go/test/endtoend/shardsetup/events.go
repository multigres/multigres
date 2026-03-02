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

package shardsetup

import (
	"bufio"
	"encoding/json"
	"io"
	"testing"
)

// ParseEvents scans a reader for multigres.event log lines.
// Each line is expected to be a JSON object; non-JSON lines are skipped.
// Returns a slice of attribute maps for lines where msg == "multigres.event".
func ParseEvents(t *testing.T, r io.Reader) []map[string]any {
	t.Helper()
	var events []map[string]any
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue // not JSON, skip
		}
		if m["msg"] == "multigres.event" {
			events = append(events, m)
		}
	}
	return events
}

// HasEvent checks if events contains at least one event with the given
// event_type and outcome values.
func HasEvent(events []map[string]any, eventType, outcome string) bool {
	for _, e := range events {
		if e["event_type"] == eventType && e["outcome"] == outcome {
			return true
		}
	}
	return false
}
