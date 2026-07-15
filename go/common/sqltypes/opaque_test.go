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

package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResultRowCount(t *testing.T) {
	assert.Equal(t, 0, (*Result)(nil).RowCount(), "nil result")
	assert.Equal(t, 3, (&Result{Rows: []*Row{{}, {}, {}}}).RowCount(), "structured counts Rows")
	assert.Equal(t, 5, (&Result{RawData: []byte("frames"), RawRowCount: 5}).RowCount(), "opaque counts RawRowCount")
}

// TestResultOpaqueRoundTrip verifies the opaque passthrough branch of ToProto
// and ResultFromProto: RawData and RawRowCount cross the proto boundary intact
// and no structured Rows are produced on either side.
func TestResultOpaqueRoundTrip(t *testing.T) {
	block := []byte("D\x00\x00\x00\x0eraw-frame-bytes")
	r := &Result{
		RawData:      block,
		RawRowCount:  4,
		CommandTag:   "SELECT 4",
		RowsAffected: 4,
	}

	pr := r.ToProto()
	require.NotNil(t, pr)
	assert.Equal(t, block, pr.RawDataBlock)
	assert.Equal(t, uint32(4), pr.RawRowCount)
	assert.Empty(t, pr.Rows, "opaque result must not carry structured Row messages")

	back := ResultFromProto(pr)
	require.NotNil(t, back)
	assert.Equal(t, block, back.RawData)
	assert.Equal(t, 4, back.RawRowCount)
	assert.Nil(t, back.Rows, "opaque result must not reconstruct structured Rows")
	assert.Equal(t, "SELECT 4", back.CommandTag)
}
