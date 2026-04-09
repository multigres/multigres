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

	t.Run("quoted element with backslash escape", func(t *testing.T) {
		result, err := ParseTextArray(`{"foo\"bar"}`)
		require.NoError(t, err)
		assert.Equal(t, []string{`foo"bar`}, result)
	})

	t.Run("quoted element with backslash", func(t *testing.T) {
		result, err := ParseTextArray(`{"foo\\bar"}`)
		require.NoError(t, err)
		assert.Equal(t, []string{`foo\bar`}, result)
	})

	t.Run("pooler ID format", func(t *testing.T) {
		result, err := ParseTextArray("{zone1_pooler-1,zone1_pooler-2,zone1_pooler-3}")
		require.NoError(t, err)
		assert.Equal(t, []string{"zone1_pooler-1", "zone1_pooler-2", "zone1_pooler-3"}, result)
	})

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
}
