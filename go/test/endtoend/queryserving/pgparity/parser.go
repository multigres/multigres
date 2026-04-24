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
// multigres proxy. Test files contain SQL plus expected results, replayed
// against both targets so any divergence surfaces as a failing test.
//
// Supported directives (parser rejects anything else):
//
//   - `#` comments and blank lines — ignored
//   - `statement ok` — SQL must execute without error
//   - `statement error [regex]` — SQL must return an error; optional regex
//     matches against the error message
//   - `query <types> [rowsort]` — SQL result must match the expected rows
//     that follow a `----` separator. Default sort mode preserves result
//     order; `rowsort` sorts whole rows before comparison.
//
// The file format is deliberately minimal — a strict parser means a
// misspelled directive fails loudly rather than silently disabling tests.
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
	DirectiveQuery                          // query <types> [rowsort]
)

// Record is a single parsed directive from a .slt file.
type Record struct {
	Kind          DirectiveKind
	LineNo        int      // 1-based line number where the directive starts
	SQL           string   // the SQL payload (no trailing newline)
	ExpectError   bool     // statement error
	ErrorPattern  string   // optional regex on statement error
	TypeString    string   // query column types, e.g. "IIT"
	SortMode      string   // "nosort" (default) or "rowsort"
	ExpectedRows  []string // one string per expected value; tab-separated rows are split on tabs
	ExpectedCount int      // number of expected values (len(ExpectedRows) after flattening)
}

// TestFile is a parsed .slt file.
type TestFile struct {
	Path    string
	Records []Record
}

// ParseFile reads a .slt file from disk and returns parsed records. Any
// unsupported directive causes a parse error rather than a silent skip so
// misspelled directives fail loudly at parse time.
func ParseFile(path string) (*TestFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	tf := &TestFile{Path: path}
	scanner := bufio.NewScanner(f)
	// Some lines (query strings, expected rows) can be long. Raise the limit
	// to 1 MiB per line.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	lineNo := 0
	readLine := func() (string, bool) {
		if !scanner.Scan() {
			return "", false
		}
		lineNo++
		return scanner.Text(), true
	}

	for {
		line, ok := readLine()
		if !ok {
			break
		}
		trimmed := strings.TrimSpace(line)

		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		if strings.HasPrefix(trimmed, "statement ") {
			rec, err := parseStatement(trimmed, lineNo, readLine)
			if err != nil {
				return nil, err
			}
			tf.Records = append(tf.Records, rec)
			continue
		}
		if strings.HasPrefix(trimmed, "query ") {
			rec, err := parseQuery(trimmed, lineNo, readLine)
			if err != nil {
				return nil, err
			}
			tf.Records = append(tf.Records, rec)
			continue
		}

		return nil, fmt.Errorf("%s:%d: unsupported directive %q (only `statement` and `query` are allowed)", path, lineNo, trimmed)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	return tf, nil
}

// parseStatement parses a `statement ok` or `statement error [regex]` block.
//
// The SQL body runs from the first line after the directive until a blank
// line or EOF.
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
		return rec, fmt.Errorf("line %d: unknown statement kind %q (want `ok` or `error`)", startLine, fields[1])
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

// parseQuery parses a `query <types> [rowsort]` block:
//
//	query II rowsort
//	SELECT a, b FROM t
//	----
//	1 one
//	2 two
//
// The section before `----` is the SQL; the section after is one row per
// line, tab-separated within a row. The expected-rows block ends at a blank
// line or EOF — callers MUST leave a blank line between records, otherwise
// a subsequent directive header would be consumed as an expected value.
// The parser flattens rows into a single list of values so the runner can
// compare cell-by-cell.
func parseQuery(header string, startLine int, readLine func() (string, bool)) (Record, error) {
	rec := Record{Kind: DirectiveQuery, LineNo: startLine, SortMode: "nosort"}

	fields := strings.Fields(header)
	if len(fields) < 2 {
		return rec, fmt.Errorf("line %d: malformed query directive: %q", startLine, header)
	}
	rec.TypeString = fields[1]
	if len(fields) >= 3 {
		switch fields[2] {
		case "nosort", "rowsort":
			rec.SortMode = fields[2]
		default:
			return rec, fmt.Errorf("line %d: unsupported sort mode %q (want `nosort` or `rowsort`)", startLine, fields[2])
		}
	}
	if len(fields) > 3 {
		return rec, fmt.Errorf("line %d: too many fields on query directive: %q", startLine, header)
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
			// Blank line before `----` ends the record without expected rows.
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

	// Read expected values until blank line / EOF. Heuristics like "stop at
	// the next directive-looking line" are avoided on purpose: a query whose
	// expected output contains a literal `statement ok` (e.g. `SELECT
	// 'statement ok'`) would be truncated. Blank lines are authoritative.
	for {
		line, ok := readLine()
		if !ok {
			break
		}
		if strings.TrimSpace(line) == "" {
			break
		}
		for v := range strings.SplitSeq(line, "\t") {
			rec.ExpectedRows = append(rec.ExpectedRows, v)
		}
	}
	rec.ExpectedCount = len(rec.ExpectedRows)
	return rec, nil
}
