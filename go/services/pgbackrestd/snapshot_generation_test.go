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

package pgbackrestd

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
)

func TestCipherGenerationBoundary(t *testing.T) {
	for _, tc := range []struct {
		generation string
		valid      bool
	}{
		{"4294967295", true}, // MaxUint32 must survive without truncation.
		{"4294967296", false},
		{"4294967297", false}, // Would wrap to generation 1 and replace its key.
		{"9223372036854775807", false},
		{"9223372036854775808", false},
		{"0", false},
		{"-1", false},
		{"-9223372036854775808", false},
		{"18446744073709551615", false},
		{"18446744073709551616", false},
		{"01", false},
		{"+1", false},
		{" 1", false},
	} {
		t.Run(tc.generation, func(t *testing.T) {
			newFakeBackrest(t, nil, false)
			keys := writeKeyFile(t, `{"1":"one"}`)
			h := newControlHarness(t, func(c *Config) { c.CipherKeyFile = keys })
			// Model a mounted Secret rotating after startup, as Status reads it
			// afresh independently of Configure.
			require.NoError(t, os.WriteFile(keys, fmt.Appendf(nil, `{"1":"one",%q:"boundary"}`, tc.generation), 0o600))
			snap, err := h.s.buildSnapshot(map[uint32]uint32{1: 1})
			status := h.s.status(t.Context())
			if tc.valid {
				require.NoError(t, err)
				assert.Equal(t, map[uint32]string{1: "one", math.MaxUint32: "boundary"}, snap.CipherPass)
				assert.Equal(t, []uint32{1, math.MaxUint32}, status.GenerationsWithKeys)
				assert.Equal(t, map[uint32]string{1: backup.CipherKeyFingerprint("one"), math.MaxUint32: backup.CipherKeyFingerprint("boundary")}, status.GenerationFingerprints)
			} else {
				require.Error(t, err)
				assert.Empty(t, snap.CipherPass, "reject the entire file rather than publish aliased keys")
				assert.Empty(t, status.GenerationsWithKeys)
				assert.Empty(t, status.GenerationFingerprints)
			}
		})
	}
}
