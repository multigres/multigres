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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWalName(t *testing.T) {
	valid := []string{
		"000000010000000000000001",
		"0000000A00000000FFFFFFFF",
		"000000010000000000000001.partial",
		"000000010000000000000001.00000028.backup",
		"00000002.history",
	}
	for _, name := range valid {
		assert.True(t, validWALName(name), name)
	}

	invalid := []string{
		"",
		"000000010000000000000001\n",
		"00000001000000000000000a",
		"0000000100000000000000011",
		"00000001000000000000001",
		"000000010000000000000001.PARTIAL",
		"000000010000000000000001.0000002.backup",
		"000000010000000000000001.00000028.BACKUP",
		"000000010000000000000001.history",
		"0000002.history",
		"00000002.history.partial",
		"00000002.HISTORY",
		"../x",
		"..",
		".",
		"000000010000000000000001/x",
		"/000000010000000000000001",
		"000000010000000000000001/../x",
		"archive.info",
	}
	for _, name := range invalid {
		assert.False(t, validWALName(name), "%q", name)
	}
}
