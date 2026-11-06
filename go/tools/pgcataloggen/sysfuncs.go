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

package pgcataloggen

import (
	"fmt"
	"regexp"
	"strings"
)

// pg_proc.dat never sets pronargdefaults. The builtins that take defaults
// are stubbed there and redefined at initdb by system_functions.sql with
// CREATE OR REPLACE FUNCTION ... DEFAULT, so the default counts have to be
// read from that file. Only the parameter lists are parsed; the bodies and
// the GRANT/REVOKE statements are ignored.

var (
	sqlCommentRE   = regexp.MustCompile(`--[^\n]*`)
	sqlBlockRE     = regexp.MustCompile(`(?s)/\*.*?\*/`)
	createFuncRE   = regexp.MustCompile(`(?i)\bCREATE\s+OR\s+REPLACE\s+FUNCTION\s+`)
	paramDefaultRE = regexp.MustCompile(`(?i)\s+DEFAULT\s`)
)

// sqlTypeAliases maps the SQL-standard spellings used in
// system_functions.sql to pg_type names (parser/gram.y: GenericType and the
// Numeric/Character/ConstDatetime productions).
var sqlTypeAliases = map[string]string{
	"boolean":                     "bool",
	"int":                         "int4",
	"integer":                     "int4",
	"smallint":                    "int2",
	"bigint":                      "int8",
	"real":                        "float4",
	"float":                       "float8",
	"double precision":            "float8",
	"decimal":                     "numeric",
	"dec":                         "numeric",
	"character":                   "bpchar",
	"char":                        "bpchar",
	"character varying":           "varchar",
	"bit varying":                 "varbit",
	"timestamp with time zone":    "timestamptz",
	"time with time zone":         "timetz",
	"timestamp without time zone": "timestamp",
	"time without time zone":      "time",
}

// ParseSystemFunctionDefaults returns, for every function in
// system_functions.sql that declares at least one DEFAULT, the number of
// defaulted parameters keyed by "name(argtype,...)" in pg_proc.dat's
// proargtypes spelling (input parameters only, OUT parameters dropped). typeNames
// reports whether a pg_type name exists; an unknown type in a defaulted
// signature is an error so that a new PostgreSQL release cannot silently
// drop a function's defaults.
func ParseSystemFunctionDefaults(src []byte, typeNames func(string) bool) (map[string]int16, error) {
	text := sqlBlockRE.ReplaceAllString(string(src), " ")
	text = sqlCommentRE.ReplaceAllString(text, " ")
	defaults := map[string]int16{}
	for _, m := range createFuncRE.FindAllStringIndex(text, -1) {
		rest := text[m[1]:]
		open := strings.IndexByte(rest, '(')
		if open < 0 {
			return nil, fmt.Errorf("system_functions.sql: CREATE FUNCTION without a parameter list near %q", head(rest))
		}
		name := strings.Trim(strings.TrimSpace(rest[:open]), `"`)
		params, err := parenGroup(rest[open:])
		if err != nil {
			return nil, fmt.Errorf("system_functions.sql: function %s: %w", name, err)
		}
		var argTypes []string
		var ndefaults int16
		for _, param := range splitTopLevel(params) {
			param = strings.TrimSpace(param)
			if param == "" {
				continue
			}
			if paramDefaultRE.MatchString(param + " ") {
				// Keep the declaration, drop the default expression.
				loc := paramDefaultRE.FindStringIndex(param + " ")
				param = strings.TrimSpace(param[:loc[0]])
				ndefaults++
			}
			tokens := strings.Fields(param)
			mode := strings.ToUpper(tokens[0])
			switch mode {
			case "OUT":
				continue // not part of proargtypes
			case "IN", "INOUT", "VARIADIC":
				tokens = tokens[1:]
			}
			// "[name] type", where the type may be several words.
			typeSpelling := tokens[0]
			if len(tokens) > 1 {
				typeSpelling = strings.Join(tokens[1:], " ")
			}
			argTypes = append(argTypes, typeSpelling)
		}
		if ndefaults == 0 {
			continue
		}
		resolved := make([]string, len(argTypes))
		for i, spelling := range argTypes {
			typname, ok := pgTypeName(spelling, typeNames)
			if !ok {
				return nil, fmt.Errorf("system_functions.sql: function %s: unknown parameter type %q", name, spelling)
			}
			resolved[i] = typname
		}
		sig := name + "(" + strings.Join(resolved, ",") + ")"
		if _, dup := defaults[sig]; dup {
			return nil, fmt.Errorf("system_functions.sql: %s is defined twice", sig)
		}
		defaults[sig] = ndefaults
	}
	return defaults, nil
}

// pgTypeName converts a SQL type spelling to its pg_type name: lower-cased,
// quotes removed, "x[]" to "_x", and SQL-standard aliases resolved.
func pgTypeName(spelling string, typeNames func(string) bool) (string, bool) {
	s := strings.ToLower(strings.TrimSpace(spelling))
	array := false
	if strings.HasSuffix(s, "[]") {
		array = true
		s = strings.TrimSpace(strings.TrimSuffix(s, "[]"))
	}
	s = strings.Trim(s, `"`)
	if alias, ok := sqlTypeAliases[s]; ok {
		s = alias
	}
	if array {
		s = "_" + s
	}
	return s, typeNames(s)
}

// parenGroup returns the text inside the parenthesized group that s starts
// with, honoring nesting.
func parenGroup(s string) (string, error) {
	depth := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[1:i], nil
			}
		}
	}
	return "", fmt.Errorf("unbalanced parentheses near %q", head(s))
}

// splitTopLevel splits s on commas that are not inside parentheses or
// single quotes, so that a DEFAULT '{a,b}' or numeric(10,2) stays whole.
func splitTopLevel(s string) []string {
	var parts []string
	depth, quoted, start := 0, false, 0
	for i, r := range s {
		switch {
		case r == '\'':
			quoted = !quoted
		case quoted:
		case r == '(':
			depth++
		case r == ')':
			depth--
		case r == ',' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

func head(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 60 {
		s = s[:60] + "..."
	}
	return s
}
