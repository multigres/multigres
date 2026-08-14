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

package postgresttests

import (
	"regexp"
	"strconv"
	"strings"
)

// specResult holds the parsed outcome of one spec-suite run against one target.
type specResult struct {
	Target   string // "direct" or "gateway"
	Total    int    // examples run
	Failures int    // failing examples
	Pending  int    // pending/skipped examples
	// Failing holds the full hspec description path of each failing example, as
	// printed by --format=failed-examples (e.g.
	// "Feature.Query.QuerySpec, ...requesting no representation, ...").
	Failing []string
}

// Passed returns the number of passing examples.
func (r *specResult) Passed() int { return r.Total - r.Failures - r.Pending }

var (
	// hspec summary line, e.g. "212 examples, 0 failures" or
	// "1264 examples, 7 failures, 3 pending".
	summaryRE = regexp.MustCompile(`(\d+)\s+examples?,\s+(\d+)\s+failures?(?:,\s+(\d+)\s+pending)?`)
	// failed-examples numbers each failure like "  1) Feature.Query...".
	failingRE = regexp.MustCompile(`^\s*\d+\)\s+(.*\S)\s*$`)
)

// parseHspecOutput extracts counts and failing example descriptions from the
// stdout of an hspec run using --format=failed-examples. That format prints a
// numbered list of failures followed by the summary line, so the parser reads
// both. It is tolerant of ANSI color and surrounding log lines.
func parseHspecOutput(out string) *specResult {
	res := &specResult{}
	out = stripANSI(out)

	if m := summaryRE.FindStringSubmatch(out); m != nil {
		res.Total, _ = strconv.Atoi(m[1])
		res.Failures, _ = strconv.Atoi(m[2])
		if m[3] != "" {
			res.Pending, _ = strconv.Atoi(m[3])
		}
	}

	for line := range strings.SplitSeq(out, "\n") {
		if m := failingRE.FindStringSubmatch(line); m != nil {
			// Skip the "N) Failures:" style headers, which have no path.
			desc := strings.TrimSpace(m[1])
			if desc != "" {
				res.Failing = append(res.Failing, desc)
			}
		}
	}
	return res
}

var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripANSI(s string) string { return ansiRE.ReplaceAllString(s, "") }
