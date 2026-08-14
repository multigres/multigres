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

package plpgsql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Counts mirror postgres/src/pl/plpgsql/src/pl_reserved_kwlist.h (24 entries)
// and pl_unreserved_kwlist.h (83 entries, where elseif/elsif share K_ELSIF).
func TestKeywordTableCounts(t *testing.T) {
	assert.Len(t, reservedKeywords, 24, "reserved keyword count must match pl_reserved_kwlist.h")
	assert.Len(t, unreservedKeywords, 83, "unreserved keyword count must match pl_unreserved_kwlist.h")
}

// The PG headers require a word to live in exactly one list.
func TestKeywordTablesDisjoint(t *testing.T) {
	for name := range reservedKeywords {
		_, dup := unreservedKeywords[name]
		assert.Falsef(t, dup, "%q appears in both reserved and unreserved tables", name)
	}
}

// Keys must be lowercase: the lexer looks up by the SQL lexer's downcased text.
func TestKeywordKeysAreLowercase(t *testing.T) {
	for name := range reservedKeywords {
		assert.Equalf(t, strings.ToLower(name), name, "reserved key %q must be lowercase", name)
	}
	for name := range unreservedKeywords {
		assert.Equalf(t, strings.ToLower(name), name, "unreserved key %q must be lowercase", name)
	}
}

// elseif and elsif are two spellings of the same token, matching PG.
func TestElsifAliases(t *testing.T) {
	assert.Equal(t, K_ELSIF, unreservedKeywords["elsif"])
	assert.Equal(t, K_ELSIF, unreservedKeywords["elseif"])
}

func TestKeywordSpotChecks(t *testing.T) {
	assert.Equal(t, K_BEGIN, reservedKeywords["begin"])
	assert.Equal(t, K_WHILE, reservedKeywords["while"])
	assert.Equal(t, K_RAISE, unreservedKeywords["raise"])
	assert.Equal(t, K_PERFORM, unreservedKeywords["perform"])
}
