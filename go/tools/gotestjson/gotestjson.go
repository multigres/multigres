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

// Package gotestjson normalizes a `go test -json` event stream so that a
// downstream test reporter can parse it even when the stream is imperfect.
//
// The immediate motivation is dorny/test-reporter's golang-json parser, which
// groups events by (Package, Test), takes the last event of each group, and
// fails the whole job with "missing elapsed on final test event" if that last
// event has no Elapsed field. Only terminal events (pass / fail / skip) carry
// Elapsed; run / output / pause / cont events do not. So a single test group
// whose last recorded event is, say, an output line makes the reporter abort —
// turning an otherwise-green test run red for a purely cosmetic reason.
//
// Two real situations produce such a group even when every test actually
// passed:
//
//   - Stray asynchronous output. In the integration suite, background
//     goroutines and child processes write to stdout/stderr. test2json wraps
//     that output into an "output" event and attributes it to whichever test
//     was most recently active. If a line lands in the narrow window after a
//     test's terminal event but before the next test starts, that test's group
//     ends on an elapsed-less output event.
//   - A truncated stream. If the test binary is killed (timeout SIGQUIT, panic,
//     OOM) mid-test, the terminal event for the in-flight test never arrives and
//     the final line may even be partial JSON.
//
// Normalize repairs both without hiding real failures: it guarantees every
// output group ends on a terminal event that carries Elapsed, preserving the
// test's actual result when a real terminal was seen, and marking a test that
// never reached a terminal as failed (an interrupted test is a failure, not a
// silent pass). Unparseable lines are dropped so the reporter never chokes on
// partial JSON from a killed process.
package gotestjson

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// event captures the subset of `go test -json` fields Normalize reasons about.
// Elapsed is a pointer so we can distinguish "absent" (run / output / pause /
// cont events) from a real zero value.
type event struct {
	Action  string   `json:"Action"`
	Package string   `json:"Package"`
	Test    string   `json:"Test"`
	Elapsed *float64 `json:"Elapsed"`
}

// synthetic is a minimal terminal event Normalize appends to close out a group
// whose last real event lacked Elapsed. It carries exactly the fields the
// reporter needs: the action determines the reported result and Elapsed
// satisfies the "final event must have elapsed" contract.
type synthetic struct {
	Action  string  `json:"Action"`
	Package string  `json:"Package"`
	Test    string  `json:"Test"`
	Elapsed float64 `json:"Elapsed"`
}

// groupState tracks, for one (Package, Test) pair, what the reporter would see
// as the group's final event and what terminal result (if any) the test
// actually reached.
type groupState struct {
	pkg, test      string
	lastHasElapsed bool    // did the most recent event for this group carry Elapsed?
	termAction     string  // most recent terminal action (pass/fail/skip); "" if none seen
	termElapsed    float64 // Elapsed of that terminal event
}

// Stats reports what Normalize did, for CI logging.
type Stats struct {
	Events      int // parseable event lines passed through unchanged
	Dropped     int // unparseable lines dropped (e.g. a truncated final line)
	Repaired    int // groups that received a synthetic terminal event
	Interrupted int // subset of Repaired that never reached a terminal (marked failed)
}

// isTerminal reports whether an action ends a test: these are the only actions
// go test emits with an Elapsed field.
func isTerminal(action string) bool {
	switch action {
	case "pass", "fail", "skip":
		return true
	default:
		return false
	}
}

// Normalize copies the `go test -json` stream from r to w, passing every
// parseable line through byte-for-byte, then appends a synthetic terminal
// event for any (Package, Test) group whose last event lacked Elapsed. See the
// package doc for the guarantees. It returns after the whole stream is read; an
// I/O error on r or w is returned, but malformed content never is.
func Normalize(r io.Reader, w io.Writer) (Stats, error) {
	var stats Stats

	// Preserve first-seen order so synthetic events are emitted deterministically.
	order := make([]string, 0)
	groups := make(map[string]*groupState)

	br := bufio.NewReader(r)
	bw := bufio.NewWriter(w)

	for {
		line, readErr := br.ReadString('\n')
		trimmed := strings.TrimSpace(line)

		if trimmed != "" {
			var ev event
			if json.Unmarshal([]byte(trimmed), &ev) != nil {
				// Not valid JSON — a truncated final line or stray text. Drop it
				// rather than let the reporter abort on "Invalid JSON".
				stats.Dropped++
			} else {
				// Pass the original line through unchanged so the reporter still
				// sees every field (Output, Time, ...) exactly as go test wrote it.
				if _, err := bw.WriteString(trimmed + "\n"); err != nil {
					return stats, err
				}
				stats.Events++

				if ev.Test != "" {
					key := ev.Package + "\x00" + ev.Test
					g := groups[key]
					if g == nil {
						g = &groupState{pkg: ev.Package, test: ev.Test}
						groups[key] = g
						order = append(order, key)
					}
					g.lastHasElapsed = ev.Elapsed != nil
					if isTerminal(ev.Action) && ev.Elapsed != nil {
						g.termAction = ev.Action
						g.termElapsed = *ev.Elapsed
					}
				}
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return stats, readErr
		}
	}

	// Close out any group whose last event lacked Elapsed.
	for _, key := range order {
		g := groups[key]
		if g.lastHasElapsed {
			continue
		}
		s := synthetic{Package: g.pkg, Test: g.test}
		if g.termAction != "" {
			// The test reached a real terminal earlier; a later event (e.g. stray
			// output) displaced it as the group's last event. Re-emit the real
			// result and duration so the reported outcome is unchanged.
			s.Action = g.termAction
			s.Elapsed = g.termElapsed
		} else {
			// The test never reached a terminal — the stream was interrupted.
			// Surface it as a failure rather than dropping or silently passing it.
			s.Action = "fail"
			s.Elapsed = 0
			stats.Interrupted++
		}
		encoded, err := json.Marshal(s)
		if err != nil {
			return stats, err
		}
		if _, err := bw.Write(append(encoded, '\n')); err != nil {
			return stats, err
		}
		stats.Repaired++
	}

	return stats, bw.Flush()
}

// String renders Stats as a compact one-line summary for CI logs.
func (s Stats) String() string {
	return fmt.Sprintf("events=%d dropped=%d repaired=%d interrupted=%d",
		s.Events, s.Dropped, s.Repaired, s.Interrupted)
}
