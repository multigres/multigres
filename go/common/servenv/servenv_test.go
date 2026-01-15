// Copyright 2025 Supabase, Inc.
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

package servenv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateRandomServiceID(t *testing.T) {
	t.Run("generates 8 character string", func(t *testing.T) {
		id := GenerateRandomServiceID()
		require.Len(t, id, 8, "service ID should be 8 characters")
	})

	t.Run("uses valid characters", func(t *testing.T) {
		id := GenerateRandomServiceID()
		// stringutil.RandomString uses these characters (lowercase consonants + digits without confusables)
		validChars := "bcdfghjklmnpqrstvwxz2456789"
		for _, char := range id {
			require.Contains(t, validChars, string(char), "service ID should only contain valid characters")
		}
	})

	t.Run("generates unique IDs", func(t *testing.T) {
		seen := make(map[string]bool)
		for range 100 {
			id := GenerateRandomServiceID()
			require.False(t, seen[id], "should not generate duplicate IDs")
			seen[id] = true
		}
	})
}
