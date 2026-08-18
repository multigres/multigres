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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// ParseEvents scans a reader for eventlog lifecycle log lines.
// Each line is expected to be a JSON object; non-JSON lines are skipped.
// Event lines are identified by the presence of an "event_type" attribute
// (the record message is the event type itself, not a fixed sentinel).
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
		if _, ok := m["event_type"]; ok {
			events = append(events, m)
		}
	}
	return events
}

// HasEvent checks if events contains at least one event with the given
// event_type and outcome values.
func HasEvent(events []map[string]any, eventType, outcome string) bool {
	return len(FindEvents(events, eventType, outcome)) > 0
}

// FindEvents returns every event with the given event_type and outcome, in the
// order they appear. Use this when a test needs to inspect an event's own
// attributes (e.g. a duration field), not just assert that it occurred.
func FindEvents(events []map[string]any, eventType, outcome string) []map[string]any {
	var matches []map[string]any
	for _, e := range events {
		if e["event_type"] == eventType && e["outcome"] == outcome {
			matches = append(matches, e)
		}
	}
	return matches
}

// WaitForLogLine polls logFile until a single raw line contains ALL of substrs,
// or the timeout expires (failing the test fatally). Unlike WaitForEvent it
// matches any log line, not just structured eventlog JSON (records carrying an
// "event_type" attribute), so it works for plain slog output such as the
// gateway's "routing primary retracted" debug line. Passing several substrs pins
// the match to one specific line (e.g. a message plus the pooler id it concerns).
// Returns the matching line.
func WaitForLogLine(t *testing.T, logFile string, timeout time.Duration, substrs ...string) string {
	t.Helper()
	require.NotEmpty(t, substrs, "WaitForLogLine requires at least one substring")
	timeout = utils.ScaleTimeout(timeout)
	var match string
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(logFile)
		if err != nil {
			return false
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if lineContainsAll(line, substrs) {
				match = line
				return true
			}
		}
		return false
	}, timeout, 200*time.Millisecond,
		"timed out waiting for log line containing all of %q in %s", substrs, logFile)
	return match
}

// lineContainsAll reports whether line contains every substring in substrs.
func lineContainsAll(line string, substrs []string) bool {
	for _, s := range substrs {
		if !strings.Contains(line, s) {
			return false
		}
	}
	return true
}

// WaitForEvent polls logFile until the given event_type+outcome appears or timeout expires.
// Returns the full set of events found in the file at the time the target event was seen.
// Fails the test (fatally) if the event is not seen within the timeout.
func WaitForEvent(t *testing.T, logFile, eventType, outcome string, timeout time.Duration) []map[string]any {
	t.Helper()
	// Events are emitted later under coverage instrumentation (slower restore,
	// promotion, etc.), so widen the wait budget there (see ScaleTimeout).
	timeout = utils.ScaleTimeout(timeout)
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
