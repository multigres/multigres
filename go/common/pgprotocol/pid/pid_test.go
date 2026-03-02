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

package pid

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		prefix      uint32
		localConnID uint32
	}{
		{"minimum values", 1, 0},
		{"typical values", 5, 42},
		{"max prefix", MaxPrefix, 100},
		{"max local conn ID", 1, MaxLocalConnID},
		{"both max", MaxPrefix, MaxLocalConnID},
		{"prefix 1 conn 1", 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pid := EncodePID(tt.prefix, tt.localConnID)
			gotPrefix, gotLocal := DecodePID(pid)
			assert.Equal(t, tt.prefix, gotPrefix)
			assert.Equal(t, tt.localConnID, gotLocal)
		})
	}
}

func TestEncodePID_BitIsolation(t *testing.T) {
	// Verify prefix only affects upper bits.
	pid := EncodePID(1, 0)
	assert.Equal(t, uint32(1<<LocalConnBits), pid)

	// Verify local conn only affects lower bits.
	pid = EncodePID(0, 1)
	assert.Equal(t, uint32(1), pid)

	// Verify both combined.
	pid = EncodePID(5, 42)
	require.Equal(t, uint32(5), pid>>LocalConnBits)
	require.Equal(t, uint32(42), pid&localConnMask)
}

func TestDecodePID_ZeroPID(t *testing.T) {
	prefix, localConnID := DecodePID(0)
	assert.Equal(t, uint32(0), prefix)
	assert.Equal(t, uint32(0), localConnID)
}

func TestConstants(t *testing.T) {
	// Verify bit split adds up to 32.
	assert.Equal(t, 32, PrefixBits+LocalConnBits)

	// Verify max values.
	assert.Equal(t, 4095, MaxPrefix)
	assert.Equal(t, 1048575, MaxLocalConnID)
}

func TestEncodePID_LocalConnMasked(t *testing.T) {
	// If localConnID exceeds MaxLocalConnID, only lower bits are kept.
	pid := EncodePID(1, MaxLocalConnID+1)
	prefix, localConnID := DecodePID(pid)
	assert.Equal(t, uint32(1), prefix)
	assert.Equal(t, uint32(0), localConnID) // overflow wraps to 0
}
