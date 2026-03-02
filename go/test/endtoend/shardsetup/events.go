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
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

// WaitForEvent polls logFile until the given event_type+outcome appears or timeout expires.
// Returns the full set of events found in the file at the time the target event was seen.
// Fails the test (fatally) if the event is not seen within the timeout.
func WaitForEvent(t *testing.T, logFile, eventType, outcome string, timeout time.Duration) []map[string]any {
	t.Helper()
	var events []map[string]any
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(logFile)
		if err != nil {
			return false
		}
		events = ParseEvents(t, bytes.NewReader(data))
		return HasEvent(events, eventType, outcome)
	}, timeout, 500*time.Millisecond,
		"timed out waiting for event_type=%q outcome=%q in %s", eventType, outcome, logFile)
	return events
}
