// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgutil

import (
	"sort"
	"strings"
)

// QuoteConnInfoValue escapes v for embedding as a libpq keyword=value conninfo
// value, always producing the single-quoted form so empty strings, embedded
// spaces, backslashes, and quotes all round-trip correctly through libpq's own
// parser (pgconn.parseKeywordValueSettings) instead of being misread as the
// start of a new keyword=value pair. Required whenever a conninfo string is
// built by interpolating a value that is not a compile-time constant (e.g. an
// operator- or caller-supplied database/host/user name) — otherwise a value
// containing a space can inject extra keywords (see the historical fix this
// guards: an unquoted database name let a caller redirect the whole
// connection via an embedded "host=...").
func QuoteConnInfoValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}

// isConnInfoSpace reports whether b is whitespace by libpq conninfo's own
// rules (matches pgconn's asciiSpace table: space, tab, newline, CR, vertical
// tab, form feed).
func isConnInfoSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

// unescapeConnInfoValue reverses QuoteConnInfoValue's escaping.
func unescapeConnInfoValue(s string) string {
	s = strings.ReplaceAll(s, `\\`, `\`)
	return strings.ReplaceAll(s, `\'`, `'`)
}

// ParseConnInfo splits a libpq-style keyword=value conninfo string into a
// key/value map. A value is either a run of non-whitespace characters, or a
// '...'-quoted string — both forms may backslash-escape a literal backslash
// or quote — matching libpq's own tokenizer (pgconn.parseKeywordValueSettings),
// so this stays the exact inverse of QuoteConnInfoValue/ConnInfoString.
func ParseConnInfo(dsn string) map[string]string {
	kv := map[string]string{}
	s := dsn
	for len(s) > 0 && isConnInfoSpace(s[0]) {
		s = s[1:]
	}
	for len(s) > 0 {
		eqIdx := strings.IndexByte(s, '=')
		if eqIdx < 0 {
			break
		}
		key := strings.ToLower(strings.TrimSpace(s[:eqIdx]))
		s = s[eqIdx+1:]
		for len(s) > 0 && isConnInfoSpace(s[0]) {
			s = s[1:]
		}

		var raw string
		switch {
		case len(s) == 0:
			raw = ""
		case s[0] != '\'':
			end := 0
			for end < len(s) && !isConnInfoSpace(s[end]) {
				if s[end] == '\\' {
					end++
					if end >= len(s) {
						break
					}
				}
				end++
			}
			raw = s[:end]
			s = s[end:]
		default:
			s = s[1:]
			end := 0
			for end < len(s) && s[end] != '\'' {
				if s[end] == '\\' {
					end++
					if end >= len(s) {
						break
					}
				}
				end++
			}
			raw = s[:min(end, len(s))]
			if end < len(s) {
				end++ // consume the closing quote
			}
			s = s[end:]
		}
		for len(s) > 0 && isConnInfoSpace(s[0]) {
			s = s[1:]
		}

		if key != "" {
			kv[key] = unescapeConnInfoValue(raw)
		}
	}
	return kv
}

// ConnInfoString serializes a conninfo map back to libpq's space-separated
// "k=v" form, in sorted key order for a deterministic result. Every value is
// quoted via QuoteConnInfoValue so a value containing a space, quote, or
// backslash round-trips through ParseConnInfo unchanged.
func ConnInfoString(kv map[string]string) string {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + QuoteConnInfoValue(kv[k])
	}
	return strings.Join(parts, " ")
}
