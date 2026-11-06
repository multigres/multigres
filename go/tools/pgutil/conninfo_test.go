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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteConnInfoValue(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "appdb", "'appdb'"},
		{"empty", "", "''"},
		{"backslash", `a\b`, `'a\\b'`},
		{"quote", "a'b", `'a\'b'`},
		{
			name: "injection attempt: embedded keyword=value pair",
			in:   "appdb host=attacker.org sslmode=disable",
			want: "'appdb host=attacker.org sslmode=disable'",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := QuoteConnInfoValue(tc.in); got != tc.want {
				t.Errorf("QuoteConnInfoValue(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseConnInfo(t *testing.T) {
	kv := ParseConnInfo("host=h port=5432  dbname=app")
	assert.Equal(t, "h", kv["host"])
	assert.Equal(t, "5432", kv["port"])
	assert.Equal(t, "app", kv["dbname"])
	assert.Empty(t, ParseConnInfo(""))

	// Keys fold to lowercase (callers look up fixed lowercase keys).
	assert.Equal(t, "h", ParseConnInfo("Host=h")["host"])

	// A quoted value keeps an embedded space instead of being split at it.
	kv = ParseConnInfo(`host=h password='has space'`)
	assert.Equal(t, "has space", kv["password"])

	// Escaped backslash and quote inside a quoted value round-trip.
	kv = ParseConnInfo(`host=h password='a\\b\'c'`)
	assert.Equal(t, `a\b'c`, kv["password"])

	// ParseConnInfo is the exact inverse of ConnInfoString/QuoteConnInfoValue.
	original := map[string]string{"host": "h", "password": `has space ' and \ backslash`}
	assert.Equal(t, original, ParseConnInfo(ConnInfoString(original)))
}
