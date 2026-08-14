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

package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQuoteConfValue covers GUC config-file quoting: both single quotes and
// backslashes must be doubled (the config-file lexer processes backslash
// escapes inside quoted values), matching ALTER SYSTEM's write_auto_conf_file.
// The contrast cases with QuoteStringLiteral document why the two are not
// interchangeable.
func TestQuoteConfValue(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"plain", "host=h port=5432", "'host=h port=5432'"},
		{"empty", "", "''"},
		{"single_quote_doubled", "pa'ss", "'pa''ss'"},
		{"backslash_doubled", `a\tb`, `'a\\tb'`},
		{"backslash_and_quote", `a\'b`, `'a\\''b'`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := QuoteConfValue(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	// Line breaks are rejected, mirroring ALTER SYSTEM's "must not contain a
	// newline" check: the config-file lexer cannot lex a quoted string across
	// lines, so accepting one would corrupt the file.
	t.Run("rejects_line_breaks", func(t *testing.T) {
		for _, v := range []string{"a\nb", "a\rb", "a\r\nb", "\n"} {
			_, err := QuoteConfValue(v)
			assert.Error(t, err, "value %q must be rejected", v)
		}
	})

	// A backslash-carrying value must NOT use QuoteStringLiteral's config-file
	// form: it switches to E'...', which the config-file lexer would read as a
	// literal E.
	assert.Equal(t, `E'a\\b'`, QuoteStringLiteral(`a\b`))
	confQuoted, err := QuoteConfValue(`a\b`)
	require.NoError(t, err)
	assert.Equal(t, `'a\\b'`, confQuoted)
}
