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

// Package testtiming is the wire contract for timeout-bounded timing
// measurements that end-to-end tests emit and CI tooling consumes. Keeping the
// emit (Record) and consume (Parse) sides in one package means the JSON
// contract has a single definition and the two halves cannot drift.
//
// A measurement is recorded as a testing.T attribute (Go 1.25+), which surfaces
// as a structured {"Action":"attr","Key":"mg_timing","Value":...} event in the
// go test -json stream. The summary command (go/tools/testtiming/summary)
// parses those events back into Measurements and aggregates them into a
// percentile table.
package testtiming

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"
)

// AttrKey is the testing.T.Attr key under which every measurement is emitted.
const AttrKey = "mg_timing"

// Measurement is the JSON payload carried in a timing attribute's value. The
// durations are time.Duration values, which JSON encodes as int64 nanoseconds.
type Measurement struct {
	Op      string        `json:"op"`
	Elapsed time.Duration `json:"elapsed"`
	Limit   time.Duration `json:"limit"`
}

// Record emits one timeout-bounded measurement as a testing.T attribute: op is
// the operation label, elapsed is how long it took, and limit is the timeout it
// ran against. The elapsed time relative to its limit is the key signal — values
// approaching the limit are at risk on slow CI runners.
func Record(t *testing.T, op string, elapsed, limit time.Duration) {
	t.Helper()
	value, err := json.Marshal(Measurement{Op: op, Elapsed: elapsed, Limit: limit})
	if err != nil {
		// Measurement holds a string and two int64s, so marshaling cannot fail.
		t.Fatalf("marshal timing measurement: %v", err)
	}
	t.Attr(AttrKey, string(value))
}

// event matches the subset of `go test -json` (gotestsum --jsonfile) event
// fields we care about. "attr" events (Go 1.25+) carry Key/Value.
type event struct {
	Action string `json:"Action"`
	Key    string `json:"Key"`
	Value  string `json:"Value"`
}

// Parse streams a go test -json event stream and returns the measurements
// recorded by Record. Lines that are not mg_timing attribute events, and events
// whose value is not a valid Measurement, are skipped — tolerating the
// surrounding test output rather than failing on it.
func Parse(r io.Reader) ([]Measurement, error) {
	var out []Measurement
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024) // some events embed long Output strings
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var ev event
		if err := json.Unmarshal(line, &ev); err != nil {
			continue // tolerate occasional malformed lines rather than abort
		}
		if ev.Action != "attr" || ev.Key != AttrKey {
			continue
		}
		var m Measurement
		if err := json.Unmarshal([]byte(ev.Value), &m); err != nil {
			continue
		}
		out = append(out, m)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan test output: %w", err)
	}
	return out, nil
}
