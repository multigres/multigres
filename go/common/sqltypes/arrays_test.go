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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTextArray(t *testing.T) {
	// ── Happy path ───────────────────────────────────────────────────────────

	t.Run("empty array", func(t *testing.T) {
		result, err := ParseTextArray("{}")
		require.NoError(t, err)
		assert.Equal(t, []string{}, result)
	})

	t.Run("single element", func(t *testing.T) {
		result, err := ParseTextArray("{foo}")
		require.NoError(t, err)
		assert.Equal(t, []string{"foo"}, result)
	})

	t.Run("multiple elements", func(t *testing.T) {
		result, err := ParseTextArray("{foo,bar,baz}")
		require.NoError(t, err)
		assert.Equal(t, []string{"foo", "bar", "baz"}, result)
	})

	t.Run("quoted element with spaces", func(t *testing.T) {
		result, err := ParseTextArray(`{"foo bar",baz}`)
		require.NoError(t, err)
		assert.Equal(t, []string{"foo bar", "baz"}, result)
	})

	t.Run("quoted element with comma", func(t *testing.T) {
		result, err := ParseTextArray(`{"foo,bar","baz"}`)
		require.NoError(t, err)
		assert.Equal(t, []string{"foo,bar", "baz"}, result)
	})

	t.Run("quoted element with escaped double quote", func(t *testing.T) {
		result, err := ParseTextArray(`{"foo\"bar"}`)
		require.NoError(t, err)
		assert.Equal(t, []string{`foo"bar`}, result)
	})

	t.Run("quoted element with escaped backslash", func(t *testing.T) {
		result, err := ParseTextArray(`{"foo\\bar"}`)
		require.NoError(t, err)
		assert.Equal(t, []string{`foo\bar`}, result)
	})

	t.Run("pooler ID format", func(t *testing.T) {
		result, err := ParseTextArray("{zone1_pooler-1,zone1_pooler-2,zone1_pooler-3}")
		require.NoError(t, err)
		assert.Equal(t, []string{"zone1_pooler-1", "zone1_pooler-2", "zone1_pooler-3"}, result)
	})

	// ── Whitespace handling (PostgreSQL spec: trim around unquoted elements) ─

	t.Run("whitespace around elements is trimmed", func(t *testing.T) {
		result, err := ParseTextArray("{ foo , bar }")
		require.NoError(t, err)
		assert.Equal(t, []string{"foo", "bar"}, result)
	})

	t.Run("internal whitespace in unquoted element is preserved", func(t *testing.T) {
		// PostgreSQL preserves whitespace surrounded by non-whitespace chars.
		result, err := ParseTextArray("{0 second  ,0 second}")
		require.NoError(t, err)
		assert.Equal(t, []string{"0 second", "0 second"}, result)
	})

	t.Run("whitespace inside quoted element is preserved", func(t *testing.T) {
		result, err := ParseTextArray(`{"  0 second  ","0 second"}`)
		require.NoError(t, err)
		assert.Equal(t, []string{"  0 second  ", "0 second"}, result)
	})

	// ── Backslash escaping outside of quotes ─────────────────────────────────

	t.Run("backslash escapes special char in unquoted element", func(t *testing.T) {
		result, err := ParseTextArray(`{ab\c}`)
		require.NoError(t, err)
		assert.Equal(t, []string{"abc"}, result)
	})

	t.Run("backslash escapes comma delimiter in unquoted element", func(t *testing.T) {
		// {foo\,bar} is a single element "foo,bar", not two elements.
		result, err := ParseTextArray(`{foo\,bar}`)
		require.NoError(t, err)
		assert.Equal(t, []string{"foo,bar"}, result)
	})

	t.Run("backslash-escaped null is not treated as SQL NULL", func(t *testing.T) {
		// \null has a backslash escape so it is the string "null", not SQL NULL.
		result, err := ParseTextArray(`{n\ull}`)
		require.NoError(t, err)
		assert.Equal(t, []string{"null"}, result)
	})

	// ── NULL handling ─────────────────────────────────────────────────────────

	t.Run("unquoted NULL element returns error", func(t *testing.T) {
		_, err := ParseTextArray(`{NULL}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NULL element")
	})

	t.Run("unquoted null is case-insensitive", func(t *testing.T) {
		_, err := ParseTextArray(`{foo,null,bar}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NULL element")
	})

	t.Run("quoted NULL string is valid", func(t *testing.T) {
		result, err := ParseTextArray(`{"NULL"}`)
		require.NoError(t, err)
		assert.Equal(t, []string{"NULL"}, result)
	})

	// ── Unicode ───────────────────────────────────────────────────────────────

	t.Run("unicode elements", func(t *testing.T) {
		result, err := ParseTextArray(`{"ᚼᛅᛁᛚ","ᚼᛅᛁᛗᚱ"}`)
		require.NoError(t, err)
		assert.Equal(t, []string{"ᚼᛅᛁᛚ", "ᚼᛅᛁᛗᚱ"}, result)
	})

	// ── Structural errors ─────────────────────────────────────────────────────

	t.Run("not an array literal - missing braces", func(t *testing.T) {
		_, err := ParseTextArray("foo,bar")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a PostgreSQL array literal")
	})

	t.Run("not an array literal - empty string", func(t *testing.T) {
		_, err := ParseTextArray("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a PostgreSQL array literal")
	})

	t.Run("not an array literal - only open brace", func(t *testing.T) {
		_, err := ParseTextArray("{")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a PostgreSQL array literal")
	})

	t.Run("missing closing brace", func(t *testing.T) {
		_, err := ParseTextArray(`{"foo","bar"`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a PostgreSQL array literal")
	})

	t.Run("trailing backslash in quoted element", func(t *testing.T) {
		// {"foo\} has valid outer braces but a backslash at end of inner content.
		_, err := ParseTextArray(`{"foo\}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unterminated escape sequence")
	})

	t.Run("unterminated quoted element", func(t *testing.T) {
		// {"foo} has valid outer braces but the closing quote is missing.
		_, err := ParseTextArray(`{"foo}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unterminated quoted element")
	})

	t.Run("junk after quoted element", func(t *testing.T) {
		_, err := ParseTextArray(`{"a"b}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "incorrectly quoted")
	})

	t.Run("quote in middle of unquoted element", func(t *testing.T) {
		_, err := ParseTextArray(`{a"b"}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "incorrectly quoted")
	})

	t.Run("consecutive delimiters", func(t *testing.T) {
		_, err := ParseTextArray(`{1,,3}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected empty element")
	})

	t.Run("trailing delimiter", func(t *testing.T) {
		_, err := ParseTextArray(`{1,}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "trailing delimiter")
	})

	t.Run("stray closing brace inside element", func(t *testing.T) {
		_, err := ParseTextArray(`{foo}bar}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected '}'")
	})

	t.Run("multi-dimensional array", func(t *testing.T) {
		_, err := ParseTextArray(`{{a,b},{c,d}}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "multi-dimensional")
	})
}
