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
// Quoted elements (e.g. {"foo bar","baz,qux"}) and backslash escapes within quoted elements
// are handled. Returns an error if the input is not a valid PostgreSQL array literal.
func ParseTextArray(s string) ([]string, error) {
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil, fmt.Errorf("not a PostgreSQL array literal: %q", s)
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return []string{}, nil
	}
	var result []string
	var cur strings.Builder
	quoted := false
	i := 0
	for i < len(inner) {
		c := inner[i]
		switch {
		case c == '"' && !quoted:
			quoted = true
		case c == '"' && quoted:
			quoted = false
		case c == '\\' && quoted && i+1 < len(inner):
			i++
			cur.WriteByte(inner[i])
		case c == ',' && !quoted:
			result = append(result, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
		i++
	}
	result = append(result, cur.String())
	return result, nil
}
