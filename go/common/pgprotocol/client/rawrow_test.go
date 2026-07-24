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

package client

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAppendRawDataRow verifies opaque passthrough reconstructs the full
// PostgreSQL DataRow wire frame: 'D', an int32 length that counts itself but
// not the type byte, then the message body verbatim; and that appending a
// second frame leaves the first intact.
func TestAppendRawDataRow(t *testing.T) {
	body := []byte{0, 1, 0, 0, 0, 3, 'a', 'b', 'c'} // int16 col count + int32 len + value

	frame := appendRawDataRow(nil, body)
	assert.Equal(t, byte('D'), frame[0], "type byte")
	assert.Equal(t, uint32(len(body)+4), binary.BigEndian.Uint32(frame[1:5]), "length counts itself, not the type byte")
	assert.Equal(t, body, frame[5:], "body preserved verbatim")

	two := appendRawDataRow(frame, []byte{9, 9})
	assert.Equal(t, frame, two[:len(frame)], "first frame intact after appending a second")
	assert.Equal(t, byte('D'), two[len(frame)], "second frame starts with its own type byte")
}
