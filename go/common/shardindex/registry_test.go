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

package shardindex

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
)

// userDefinedType is the type name of the user-provided implementation that
// userdefined_test.go registers from outside the package.
const userDefinedType = "test.identity"

func TestRegister_Duplicate(t *testing.T) {
	t.Parallel()
	assert.PanicsWithValue(t, "shardindex: type hash is already registered", func() {
		registerBuiltin(HashType, newHashIndex)
	})
	assert.PanicsWithValue(t, "shardindex: type test.identity is already registered", func() {
		Register(userDefinedType, newHashIndex)
	})
	assert.Panics(t, func() { Register("test.nil_factory", nil) })
	assert.Panics(t, func() { registerBuiltin("test_nil_factory", nil) })
	// A rejected registration leaves nothing behind.
	assert.NotContains(t, Types(), "test.nil_factory")
	assert.NotContains(t, Types(), "test_nil_factory")
}

// Names without a dot belong to the built-in implementations, so a built-in
// added later cannot collide with a user-provided type already in a catalog.
func TestRegister_Namespaces(t *testing.T) {
	t.Parallel()
	for _, typ := range []string{"", "range", "lookup", ".range", "acme.", "."} {
		assert.PanicsWithValue(t,
			"shardindex: type "+strconv.Quote(typ)+
				" is not namespaced: user-provided types are named <namespace>.<name>, names without a dot are reserved for built-in types",
			func() { Register(typ, newHashIndex) }, "Register(%q)", typ)
		assert.NotContains(t, Types(), typ)
	}
	for _, typ := range []string{"", "acme.range"} {
		assert.Panics(t, func() { registerBuiltin(typ, newHashIndex) }, "registerBuiltin(%q)", typ)
		assert.NotContains(t, Types(), typ)
	}
}

func TestNew_UnknownType(t *testing.T) {
	t.Parallel()
	for _, typ := range []string{"", "Hash", "hash ", "lookup"} {
		index, err := New(typ, "by_id", nil)
		requireSQLState(t, err, mterrors.PgSSUndefinedObject)
		assert.Nil(t, index)
		assert.Contains(t, err.Error(), `"`+typ+`"`)
	}
}

func TestTypes(t *testing.T) {
	t.Parallel()
	types := Types()
	assert.Contains(t, types, HashType)
	assert.Contains(t, types, userDefinedType)
	assert.IsIncreasing(t, types)
}

func TestCheckParams(t *testing.T) {
	t.Parallel()
	require.NoError(t, CheckParams("t", "n", nil))
	require.NoError(t, CheckParams("t", "n", map[string]string{}, "a"))
	require.NoError(t, CheckParams("t", "n", map[string]string{"a": "", "b": "x"}, "a", "b", "c"))

	// Names are matched exactly, and the first unknown name in sorted order
	// is reported, whatever the map's iteration order.
	err := CheckParams("lookup", "by_email", map[string]string{"table": "t", "zeta": "1", "From": "2", "alpha": "3"}, "table", "from")
	requireSQLState(t, err, mterrors.PgSSInvalidParameterValue)
	assert.Equal(t, `unrecognized parameter "From" for shard index "by_email" of type "lookup"`, diagnosticMessage(t, err))
}

func diagnosticMessage(t *testing.T, err error) string {
	t.Helper()
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	return diag.Message
}

func TestParseParams(t *testing.T) {
	t.Parallel()
	for text, want := range map[string]map[string]string{
		"":                               {},
		"{}":                             {},
		" { } ":                          {},
		`{"table": "lookup", "from":""}`: {"table": "lookup", "from": ""},
		`{"a": "é\"\n"}`:                 {"a": "é\"\n"},
	} {
		got, err := ParseParams(text)
		require.NoError(t, err, "%q", text)
		assert.Equal(t, want, got, "%q", text)
	}

	for _, text := range []string{
		"null", "[]", `"a"`, "1", "{", `{"a": "b"} x`,
		`{"a": 1}`, `{"a": true}`, `{"a": null}`, `{"a": ["b"]}`, `{"a": {"b": "c"}}`, `{"a": "b", "c": 2}`,
	} {
		got, err := ParseParams(text)
		requireSQLState(t, err, mterrors.PgSSInvalidParameterValue)
		assert.Nil(t, got, "%q", text)
	}

	// The catalog's default goes straight into New.
	params, err := ParseParams("{}")
	require.NoError(t, err)
	_, err = New(HashType, "by_id", params)
	require.NoError(t, err)
}
