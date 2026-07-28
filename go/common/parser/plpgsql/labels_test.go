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
	"testing"

	"github.com/stretchr/testify/assert"
)

// Block-label parse behavior is covered by testdata/block_cases.json; this just
// unit-tests the pure helper.
func TestCheckLabels(t *testing.T) {
	assert.NoError(t, checkLabels("", ""))
	assert.NoError(t, checkLabels("a", ""))
	assert.NoError(t, checkLabels("a", "a"))
	assert.Error(t, checkLabels("", "x"))  // end label on unlabeled block
	assert.Error(t, checkLabels("a", "b")) // mismatch
}
