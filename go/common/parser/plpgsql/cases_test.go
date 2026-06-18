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

package plpgsql

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Data-driven parse tests, modelled on the SQL parser's parse_test harness.
//
// To add a test, append a case to a testdata/*_cases.json file — usually just
//
//	{ "comment": "...", "body": "DECLARE i int; BEGIN END" }
//
// then regenerate the expected deparse with:
//
//	PLPGSQL_REWRITE=1 go test ./go/common/parser/plpgsql/ -run TestParseCases
//
// which fills in each case's "deparse". For a case that should fail to parse,
// give an "error" substring instead and omit "deparse".
//
// Each successful case asserts: the body parses, its deparse matches the golden
// "deparse" (or the body when they are equal), and the deparse re-parses to a
// stable deparse (round-trip).
type parseCase struct {
	Comment string `json:"comment,omitempty"`
	Body    string `json:"body"`
	Deparse string `json:"deparse,omitempty"` // expected deparse; empty means equal to Body
	Error   string `json:"error,omitempty"`   // expected parse-error substring
}

var caseFiles = []string{
	"block_cases.json",
	"declare_cases.json",
	"assign_cases.json",
	"control_flow_cases.json",
	"for_cases.json",
	"case_cases.json",
}

func TestParseCases(t *testing.T) {
	rewrite := os.Getenv("PLPGSQL_REWRITE") != ""
	for _, filename := range caseFiles {
		runCaseFile(t, filename, rewrite)
	}
}

func runCaseFile(t *testing.T, filename string, rewrite bool) {
	t.Run(filename, func(t *testing.T) {
		path := filepath.Join("testdata", filename)
		cases := readCases(t, path)

		for i := range cases {
			c := &cases[i]
			name := c.Comment
			if name == "" {
				name = c.Body
			}

			fn, err := ParsePLpgSQL(c.Body)

			if c.Error != "" {
				assert.ErrorContainsf(t, err, c.Error, "case: %s", name)
				continue
			}
			if !assert.NoErrorf(t, err, "case: %s", name) {
				continue
			}

			got := fn.SqlString()
			if rewrite {
				if got == c.Body {
					c.Deparse = ""
				} else {
					c.Deparse = got
				}
			} else {
				want := c.Deparse
				if want == "" {
					want = c.Body
				}
				assert.Equalf(t, want, got, "deparse mismatch, case: %s", name)
			}

			// Round-trip: the deparse must re-parse to the same deparse.
			if fn2, rerr := ParsePLpgSQL(got); assert.NoErrorf(t, rerr, "re-parse failed, case: %s, deparse: %q", name, got) {
				assert.Equalf(t, got, fn2.SqlString(), "deparse not stable, case: %s", name)
			}
		}

		if rewrite {
			writeCases(t, path, cases)
		}
	})
}

func readCases(t *testing.T, path string) []parseCase {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoErrorf(t, err, "reading %s", path)
	var cases []parseCase
	require.NoErrorf(t, json.Unmarshal(data, &cases), "parsing %s", path)
	return cases
}

func writeCases(t *testing.T, path string, cases []parseCase) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	require.NoError(t, enc.Encode(cases))
}
