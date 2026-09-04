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

package gotestjson

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dornyMissingElapsedGroup mirrors dorny/test-reporter's golang-json parser:
// it groups events by "Package/Test", takes each group's LAST event, and the
// reporter throws "missing elapsed on final test event" when that last event
// has no Elapsed field. This helper returns the key of the first such group in
// first-seen order (matching the reporter's insertion-ordered Map), or "" when
// every group's final event carries Elapsed. It lets these tests assert that a
// stream that WOULD crash the real reporter no longer does after Normalize —
// the local reproduction of the CI flake, and the guard against regressing it.
func dornyMissingElapsedGroup(t *testing.T, jsonl string) string {
	t.Helper()
	type rawEvent struct {
		Package string   `json:"Package"`
		Test    string   `json:"Test"`
		Elapsed *float64 `json:"Elapsed"`
	}
	order := []string{}
	last := map[string]*float64{}
	for line := range strings.SplitSeq(strings.TrimSpace(jsonl), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e rawEvent
		require.NoError(t, json.Unmarshal([]byte(line), &e),
			"dorny would also fail to JSON-parse this line: %q", line)
		if e.Test == "" {
			continue // reporter ignores package-level events
		}
		key := e.Package + "/" + e.Test
		if _, seen := last[key]; !seen {
			order = append(order, key)
		}
		last[key] = e.Elapsed
	}
	for _, key := range order {
		if last[key] == nil {
			return key
		}
	}
	return ""
}

// lastSyntheticEventFor returns the last event in jsonl for the given package
// and test, decoded as a generic map, or nil if none.
func lastSyntheticEventFor(t *testing.T, jsonl, pkg, test string) map[string]any {
	t.Helper()
	var found map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(jsonl), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m))
		if m["Package"] == pkg && m["Test"] == test {
			found = m
		}
	}
	return found
}

func normalizeString(t *testing.T, in string) (string, Stats) {
	t.Helper()
	var out strings.Builder
	stats, err := Normalize(strings.NewReader(in), &out)
	require.NoError(t, err)
	return out.String(), stats
}

// TestStrayOutputAfterPass is the exact CI flake: a test passes, then a stray
// asynchronous output line is attributed to it, so its group's final event has
// no Elapsed and the reporter aborts. Normalize must re-close the group with
// the real pass result and its real duration.
func TestStrayOutputAfterPass(t *testing.T) {
	in := strings.Join([]string{
		`{"Action":"run","Package":"pkg","Test":"TestA"}`,
		`{"Action":"output","Package":"pkg","Test":"TestA","Output":"--- PASS: TestA (0.10s)\n"}`,
		`{"Action":"pass","Package":"pkg","Test":"TestA","Elapsed":0.1}`,
		`{"Action":"output","Package":"pkg","Test":"TestA","Output":"pooler-1 terminated with error: exit status 2\n"}`,
	}, "\n") + "\n"

	// Reproduce: the raw stream would crash the reporter.
	require.Equal(t, "pkg/TestA", dornyMissingElapsedGroup(t, in),
		"expected the raw stream to trip the reporter's missing-elapsed check")

	out, stats := normalizeString(t, in)

	// Fix: after normalization the reporter would parse it cleanly.
	assert.Equal(t, "", dornyMissingElapsedGroup(t, out))
	assert.Equal(t, 1, stats.Repaired)
	assert.Equal(t, 0, stats.Interrupted, "a passed test must not be counted as interrupted")

	last := lastSyntheticEventFor(t, out, "pkg", "TestA")
	require.NotNil(t, last)
	assert.Equal(t, "pass", last["Action"], "the real pass result must be preserved")
	assert.Equal(t, 0.1, last["Elapsed"], "the real elapsed must be preserved")
}

// TestRealFailurePreserved guards the "do not paper over failures" requirement:
// a genuinely failing test whose group ends on stray output must be re-closed
// as a failure, never flipped to a pass.
func TestRealFailurePreserved(t *testing.T) {
	in := strings.Join([]string{
		`{"Action":"run","Package":"pkg","Test":"TestB"}`,
		`{"Action":"output","Package":"pkg","Test":"TestB","Output":"--- FAIL: TestB (0.20s)\n"}`,
		`{"Action":"fail","Package":"pkg","Test":"TestB","Elapsed":0.2}`,
		`{"Action":"output","Package":"pkg","Test":"TestB","Output":"stray line after the failure\n"}`,
	}, "\n") + "\n"

	require.Equal(t, "pkg/TestB", dornyMissingElapsedGroup(t, in))

	out, stats := normalizeString(t, in)

	assert.Equal(t, "", dornyMissingElapsedGroup(t, out))
	assert.Equal(t, 1, stats.Repaired)
	last := lastSyntheticEventFor(t, out, "pkg", "TestB")
	require.NotNil(t, last)
	assert.Equal(t, "fail", last["Action"], "a real failure must stay a failure")
	assert.Equal(t, 0.2, last["Elapsed"])
}

// TestInterruptedTestMarkedFailed covers a stream truncated mid-test (timeout,
// panic, OOM): the test never reached a terminal event. Normalize must surface
// it as a failure rather than dropping it or letting it read as a pass.
func TestInterruptedTestMarkedFailed(t *testing.T) {
	in := strings.Join([]string{
		`{"Action":"run","Package":"pkg","Test":"TestC"}`,
		`{"Action":"output","Package":"pkg","Test":"TestC","Output":"panic: boom\n"}`,
	}, "\n") + "\n"

	require.Equal(t, "pkg/TestC", dornyMissingElapsedGroup(t, in))

	out, stats := normalizeString(t, in)

	assert.Equal(t, "", dornyMissingElapsedGroup(t, out))
	assert.Equal(t, 1, stats.Repaired)
	assert.Equal(t, 1, stats.Interrupted)
	last := lastSyntheticEventFor(t, out, "pkg", "TestC")
	require.NotNil(t, last)
	assert.Equal(t, "fail", last["Action"], "an interrupted test must be surfaced as failed")
}

// TestTruncatedFinalLineDropped covers a process killed mid-write: the last
// line is partial JSON. dorny throws "Invalid JSON" on it; Normalize drops it
// (and still closes out the now-terminal-less group).
func TestTruncatedFinalLineDropped(t *testing.T) {
	in := strings.Join([]string{
		`{"Action":"run","Package":"pkg","Test":"TestD"}`,
		`{"Action":"output","Package":"pkg","Test":"TestD","Output":"working...\n"}`,
		`{"Action":"ru`, // truncated: process killed mid-write
	}, "\n") // deliberately no trailing newline

	out, stats := normalizeString(t, in)

	assert.Equal(t, 1, stats.Dropped)
	assert.Equal(t, 1, stats.Repaired)
	assert.Equal(t, 1, stats.Interrupted)
	// dornyMissingElapsedGroup require.NoError-parses every output line, so its
	// success also proves the truncated partial line did not survive into output.
	assert.Equal(t, "", dornyMissingElapsedGroup(t, out))
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		assert.True(t, json.Valid([]byte(line)), "every output line must be valid JSON, got %q", line)
	}
}

// TestCleanStreamUnchanged: a well-formed all-terminal stream needs no repair
// and passes through unchanged (aside from a normalized trailing newline).
func TestCleanStreamUnchanged(t *testing.T) {
	lines := []string{
		`{"Action":"run","Package":"pkg","Test":"TestOK"}`,
		`{"Action":"output","Package":"pkg","Test":"TestOK","Output":"--- PASS: TestOK (0.01s)\n"}`,
		`{"Action":"pass","Package":"pkg","Test":"TestOK","Elapsed":0.01}`,
		`{"Action":"output","Package":"pkg","Output":"ok  \tpkg\t0.02s\n"}`,
		`{"Action":"pass","Package":"pkg","Elapsed":0.02}`,
	}
	in := strings.Join(lines, "\n") + "\n"

	require.Equal(t, "", dornyMissingElapsedGroup(t, in), "clean stream should already be fine")

	out, stats := normalizeString(t, in)

	assert.Equal(t, 0, stats.Repaired)
	assert.Equal(t, 0, stats.Dropped)
	assert.Equal(t, len(lines), stats.Events)
	assert.Equal(t, in, out, "a clean stream must pass through byte-for-byte")
}

// TestPackageLevelEventsIgnored: events without a Test field are package-level
// and must not spawn synthetic per-test events.
func TestPackageLevelEventsIgnored(t *testing.T) {
	in := strings.Join([]string{
		`{"Action":"start","Package":"pkg"}`,
		`{"Action":"output","Package":"pkg","Output":"some package output\n"}`,
		`{"Action":"pass","Package":"pkg","Elapsed":0.42}`,
	}, "\n") + "\n"

	out, stats := normalizeString(t, in)

	assert.Equal(t, 0, stats.Repaired)
	assert.Equal(t, in, out)
}

// TestMultipleGroupsMixedOutcomes exercises all three group shapes in one
// stream, and checks synthetic events are appended in first-seen order.
func TestMultipleGroupsMixedOutcomes(t *testing.T) {
	in := strings.Join([]string{
		`{"Action":"run","Package":"pkg","Test":"TestPass"}`,
		`{"Action":"pass","Package":"pkg","Test":"TestPass","Elapsed":0.1}`,
		`{"Action":"output","Package":"pkg","Test":"TestPass","Output":"stray\n"}`,
		`{"Action":"run","Package":"pkg","Test":"TestInterrupted"}`,
		`{"Action":"output","Package":"pkg","Test":"TestInterrupted","Output":"hang\n"}`,
	}, "\n") + "\n"

	out, stats := normalizeString(t, in)

	assert.Equal(t, 2, stats.Repaired)
	assert.Equal(t, 1, stats.Interrupted)
	assert.Equal(t, "", dornyMissingElapsedGroup(t, out))

	// Synthetic events are appended after the original stream, TestPass before
	// TestInterrupted (first-seen order).
	idxPass := strings.LastIndex(out, `"Test":"TestPass"`)
	idxInterrupted := strings.LastIndex(out, `"Test":"TestInterrupted"`)
	assert.Less(t, idxPass, idxInterrupted, "synthetic events should follow first-seen order")
}
