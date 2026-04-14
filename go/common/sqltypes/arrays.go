// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqltypes

import (
	"fmt"
	"strings"
)

// ParseTextArray parses a PostgreSQL text-format array literal (e.g. {foo,bar}) into a []string.
// Follows the PostgreSQL specification:
//   - Quoted elements preserve all content; backslash escapes apply inside quotes.
//   - Unquoted elements have leading/trailing whitespace trimmed; backslash escapes apply.
//   - Unquoted NULL (any case, no escapes) is SQL NULL — an error since it cannot be a string.
//   - Backslash-escaped values (e.g. \null) are never treated as NULL.
//
// Multi-dimensional arrays return an error. Malformed input returns an error.
func ParseTextArray(s string) ([]string, error) {
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil, fmt.Errorf("not a PostgreSQL array literal: %q", s)
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return []string{}, nil
	}

	var result []string
	i := 0

	for {
		// Skip leading whitespace before the element.
		for i < len(inner) && isArraySpace(inner[i]) {
			i++
		}
		if i >= len(inner) {
			// Reached end after a comma: trailing delimiter.
			return nil, fmt.Errorf("trailing delimiter in PostgreSQL array: %q", s)
		}
		if inner[i] == '{' {
			return nil, fmt.Errorf("multi-dimensional arrays are not supported: %q", s)
		}

		var (
			elem       string
			hasEscapes bool
			wasQuoted  bool
			err        error
		)

		if inner[i] == '"' {
			elem, i, err = readQuotedArrayElem(inner, i, s)
			wasQuoted = true
		} else {
			elem, hasEscapes, i, err = readUnquotedArrayElem(inner, i, s)
		}
		if err != nil {
			return nil, err
		}

		// Consecutive or trailing delimiters produce an empty unquoted element.
		if !wasQuoted && elem == "" {
			return nil, fmt.Errorf("unexpected empty element in PostgreSQL array: %q", s)
		}
		// Unquoted NULL without backslash escapes is SQL NULL, which has no string representation.
		if !wasQuoted && !hasEscapes && strings.EqualFold(elem, "null") {
			return nil, fmt.Errorf("NULL element in PostgreSQL array is not supported: %q", s)
		}

		result = append(result, elem)

		if i >= len(inner) {
			break
		}
		if inner[i] != ',' {
			return nil, fmt.Errorf("unexpected character %q in PostgreSQL array: %q", inner[i], s)
		}
		i++ // skip comma
	}

	return result, nil
}

// readQuotedArrayElem reads a double-quoted element starting at inner[i] (the opening `"`).
// Returns the element value and the position pointing at `,` or end of inner.
func readQuotedArrayElem(inner string, i int, orig string) (string, int, error) {
	i++ // skip opening `"`
	var cur strings.Builder
	for i < len(inner) {
		c := inner[i]
		if c == '\\' {
			i++
			if i >= len(inner) {
				return "", i, fmt.Errorf("unterminated escape sequence in PostgreSQL array: %q", orig)
			}
			cur.WriteByte(inner[i])
			i++
			continue
		}
		if c == '"' {
			i++ // skip closing `"`
			// After the closing quote only whitespace and then `,` or end is valid.
			for i < len(inner) && isArraySpace(inner[i]) {
				i++
			}
			if i < len(inner) && inner[i] != ',' {
				return "", i, fmt.Errorf("incorrectly quoted array element in PostgreSQL array: %q", orig)
			}
			return cur.String(), i, nil
		}
		cur.WriteByte(c)
		i++
	}
	return "", i, fmt.Errorf("unterminated quoted element in PostgreSQL array: %q", orig)
}

// readUnquotedArrayElem reads an unquoted element starting at inner[i].
// Processes backslash escapes and trims trailing whitespace (leading whitespace is
// consumed by the caller before invoking this function).
// Returns (element, hasEscapes, newPos, error) where newPos points at `,` or end of inner.
func readUnquotedArrayElem(inner string, i int, orig string) (string, bool, int, error) {
	var cur strings.Builder
	hasEscapes := false
	lastNonSpace := 0 // length of cur up to and including the last non-whitespace byte

	for i < len(inner) {
		c := inner[i]
		switch c {
		case ',':
			return cur.String()[:lastNonSpace], hasEscapes, i, nil
		case '{':
			return "", false, i, fmt.Errorf("multi-dimensional arrays are not supported: %q", orig)
		case '}':
			return "", false, i, fmt.Errorf("unexpected '}' in PostgreSQL array: %q", orig)
		case '"':
			return "", false, i, fmt.Errorf("incorrectly quoted array element in PostgreSQL array: %q", orig)
		case '\\':
			i++
			if i >= len(inner) {
				return "", false, i, fmt.Errorf("unterminated escape sequence in PostgreSQL array: %q", orig)
			}
			cur.WriteByte(inner[i])
			hasEscapes = true
			lastNonSpace = cur.Len()
			i++
			continue
		default:
			cur.WriteByte(c)
			if !isArraySpace(c) {
				lastNonSpace = cur.Len()
			}
		}
		i++
	}
	return cur.String()[:lastNonSpace], hasEscapes, i, nil
}

// isArraySpace reports whether c is whitespace per PostgreSQL's scanner_isspace:
// space, tab, newline, carriage return, form feed, vertical tab.
func isArraySpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}
