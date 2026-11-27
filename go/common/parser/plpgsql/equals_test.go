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

package plpgsql

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
)

// TestEqualsNodeMatchesDeepEqual checks the generated plpgsqlast.EqualsNode
// against reflect.DeepEqual over the case corpus: each body is parsed twice and
// compared with its own second parse, with its clone, and with the previous
// case's tree so the unequal branch is exercised too.
func TestEqualsNodeMatchesDeepEqual(t *testing.T) {
	for _, filename := range caseFiles {
		t.Run(filename, func(t *testing.T) {
			var prev *plpgsqlast.PLpgSQL_function
			for _, c := range readCases(t, filepath.Join("testdata", filename)) {
				if c.Error != "" {
					continue
				}
				first, err := ParsePLpgSQL(c.Body)
				if err != nil {
					continue
				}
				second, err := ParsePLpgSQL(c.Body)
				require.NoError(t, err)

				assert.Truef(t, plpgsqlast.EqualsNode(first, second),
					"two parses of the same body are not equal\nbody: %s", c.Body)
				assert.Truef(t, plpgsqlast.EqualsNode(first, plpgsqlast.CloneNode(first)),
					"a function is not equal to its clone\nbody: %s", c.Body)
				if prev != nil {
					assert.Equalf(t, reflect.DeepEqual(prev, first), plpgsqlast.EqualsNode(prev, first),
						"EqualsNode disagrees with reflect.DeepEqual\nprevious: %s\nbody:     %s",
						prev.SqlString(), c.Body)
				}
				prev = first
			}
		})
	}
}
