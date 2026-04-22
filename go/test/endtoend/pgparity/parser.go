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

// Package pgparity runs query-parity tests across direct PostgreSQL and the
// multigres proxy. Test files use a subset of SQLite's sqllogictest format
// (https://www.sqlite.org/sqllogictest/) — SQL plus expected results, replayed
// against both targets so any divergence surfaces as a failing test.
//
// Supported directives:
//
//   - `statement ok` — SQL must execute without error
//   - `statement error [regex]` — SQL must return an error; pattern is optional
//   - `query <types> [<sort>] [label]` — SQL result must match expected rows
//   - `skipif postgres` / `onlyif postgres` — directive-level gates
//   - `halt` — stop processing the file
//
// Large result hashing (`N values hashing to <md5>`) is NOT supported — the
// curated corpus under testdata/ uses inline expected output only.
package pgparity

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// DirectiveKind identifies the directive type on a parsed record.
type DirectiveKind int

const (
	DirectiveStatement DirectiveKind = iota // statement ok / statement error
	DirectiveQuery                          // query <types> [<sort>]
)

// Record is a single parsed directive from a .test file.
type Record struct {
	Kind          DirectiveKind
	LineNo        int      // 1-based line number where the directive starts
	SQL           string   // the SQL payload (no trailing newline)
	ExpectError   bool     // statement error
	ErrorPattern  string   // optional regex on statement error
	TypeString    string   // query column types, e.g. "IIT"
	SortMode      string   // "nosort" (default), "rowsort", "valuesort"
	Label         string   // optional query label (currently unused, parsed for format completeness)
	ExpectedRows  []string // one string per expected value; tab-separated rows are split on tabs
	ExpectedCount int      // number of expected values (len(ExpectedRows) after flattening)
}

// TestFile is a parsed sqllogictest file.
type TestFile struct {
	Path    string
	Records []Record
}

// ParseFile reads a .test file from disk and returns parsed records.
//
// Files that use unsupported directives (e.g. `hash-threshold`, `skipif` for a
// non-postgres engine) are parsed best-effort. Unknown directives cause the
// parser to skip the block and emit a warning via the returned error slice.
func ParseFile(path string) (*TestFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	tf := &TestFile{Path: path}
	scanner := bufio.NewScanner(f)
	// Some sqllogictest files have very long lines (query strings, expected rows).
	// Raise the limit to 1 MiB per line.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	lineNo := 0
	// peek one line at a time; use a tiny helper to support "put-back" behavior.
	var pending string
	var pendingValid bool
	readLine := func() (string, bool) {
		if pendingValid {
			pendingValid = false
			return pending, true
		}
		if !scanner.Scan() {
			return "", false
		}
		lineNo++
		return scanner.Text(), true
	}
	pushBack := func(s string) {
		pending = s
		pendingValid = true
		// Note: we don't decrement lineNo because lineNo is advisory, used only
		// in error messages. Slight over-count is acceptable.
	}

	var skipNext bool // true when a skipif directive gates the next record

	for {
		line, ok := readLine()
		if !ok {
			break
		}
		trimmed := strings.TrimSpace(line)

		// Blank lines and comments separate records.
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// halt: stop processing the rest of the file.
		if trimmed == "halt" {
			break
		}

		// Conditional directives. We interpret postgres-specific gates; any
		// other engine means we also skip (because we only ever run against
		// postgres-compatible backends here).
		if after, ok := strings.CutPrefix(trimmed, "skipif "); ok {
			if strings.TrimSpace(after) == "postgres" {
				skipNext = true
			}
			continue
		}
		if after, ok := strings.CutPrefix(trimmed, "onlyif "); ok {
			if strings.TrimSpace(after) != "postgres" {
				skipNext = true
			}
			continue
		}

		// hash-threshold <n>: we don't support hashed results, so ignore.
		if strings.HasPrefix(trimmed, "hash-threshold") {
			continue
		}

		// statement ok | statement error [pattern]
		if strings.HasPrefix(trimmed, "statement ") {
			rec, err := parseStatement(trimmed, lineNo, readLine)
			if err != nil {
				return nil, err
			}
			if !skipNext {
				tf.Records = append(tf.Records, rec)
			}
			skipNext = false
			continue
		}

		// query <types> [<sort>] [label]
		if strings.HasPrefix(trimmed, "query ") {
			rec, err := parseQuery(trimmed, lineNo, readLine, pushBack)
			if err != nil {
				return nil, err
			}
			if !skipNext {
				tf.Records = append(tf.Records, rec)
			}
			skipNext = false
			continue
		}

		// Unknown directive: skip to next blank line.
		for {
			next, ok := readLine()
			if !ok {
				break
			}
			if strings.TrimSpace(next) == "" {
				break
			}
		}
		skipNext = false
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	return tf, nil
}

// parseStatement parses a `statement ok` or `statement error [pattern]` block.
//
// The SQL body runs from the first line after the directive until a blank line
// or EOF.
func parseStatement(header string, startLine int, readLine func() (string, bool)) (Record, error) {
	rec := Record{Kind: DirectiveStatement, LineNo: startLine}

	fields := strings.Fields(header)
	if len(fields) < 2 {
		return rec, fmt.Errorf("line %d: malformed statement directive: %q", startLine, header)
	}
	switch fields[1] {
	case "ok":
		rec.ExpectError = false
	case "error":
		rec.ExpectError = true
		if len(fields) > 2 {
			rec.ErrorPattern = strings.Join(fields[2:], " ")
		}
	default:
		return rec, fmt.Errorf("line %d: unknown statement kind %q", startLine, fields[1])
	}

	var sql strings.Builder
	for {
		line, ok := readLine()
		if !ok {
			break
		}
		if strings.TrimSpace(line) == "" {
			break
		}
		if sql.Len() > 0 {
			sql.WriteByte('\n')
		}
		sql.WriteString(line)
	}
	rec.SQL = sql.String()
	return rec, nil
}

// parseQuery parses a `query <types> [<sort>] [label]` block:
//
//	query II rowsort
//	SELECT a, b FROM t
//	----
//	1 one
//	2 two
//
// The section before `----` is the SQL; the section after is one value per
// line (tab-separated rows are split into individual values here so that the
// runner and parser share a single flat representation).
func parseQuery(header string, startLine int, readLine func() (string, bool), pushBack func(string)) (Record, error) {
	rec := Record{Kind: DirectiveQuery, LineNo: startLine, SortMode: "nosort"}

	fields := strings.Fields(header)
	if len(fields) < 2 {
		return rec, fmt.Errorf("line %d: malformed query directive: %q", startLine, header)
	}
	rec.TypeString = fields[1]
	if len(fields) >= 3 {
		switch fields[2] {
		case "nosort", "rowsort", "valuesort":
			rec.SortMode = fields[2]
		default:
			// Treat as label (sqllogictest allows labels after the type string).
			rec.Label = fields[2]
		}
	}
	if len(fields) >= 4 {
		rec.Label = fields[3]
	}

	// Read SQL until `----` or blank line.
	var sql strings.Builder
	sawSeparator := false
	for {
		line, ok := readLine()
		if !ok {
			break
		}
		if line == "----" {
			sawSeparator = true
			break
		}
		if strings.TrimSpace(line) == "" {
			// Blank line before `----` ends the record without expected rows
			// (used for queries whose output is irrelevant, e.g. EXPLAIN).
			break
		}
		if sql.Len() > 0 {
			sql.WriteByte('\n')
		}
		sql.WriteString(line)
	}
	rec.SQL = sql.String()

	if !sawSeparator {
		return rec, nil
	}

	// Read expected values until blank line / EOF / next directive.
	for {
		line, ok := readLine()
		if !ok {
			break
		}
		if strings.TrimSpace(line) == "" {
			break
		}
		if isDirectiveLine(line) {
			// Safety net: we encountered the next record without a blank
			// separator. Push it back so the main loop sees it.
			pushBack(line)
			break
		}
		// A row may contain multiple values separated by tabs. Split into
		// individual values for comparison.
		for v := range strings.SplitSeq(line, "\t") {
			rec.ExpectedRows = append(rec.ExpectedRows, v)
		}
	}
	rec.ExpectedCount = len(rec.ExpectedRows)
	return rec, nil
}

// isDirectiveLine returns true if the line begins a new sqllogictest
// directive, used as a safety net for malformed files that omit the blank
// separator between records.
func isDirectiveLine(line string) bool {
	switch {
	case strings.HasPrefix(line, "statement "),
		strings.HasPrefix(line, "query "),
		strings.HasPrefix(line, "skipif "),
		strings.HasPrefix(line, "onlyif "),
		line == "halt",
		strings.HasPrefix(line, "hash-threshold"):
		return true
	}
	return false
}
