// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package units

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"0", 0},
		{"1048576", 1048576},        // bare integer bytes
		{"1 MiB", 1048576},          // whitespace
		{"1MiB", 1048576},           // no whitespace
		{"8 MiB", 8 * 1048576},      //
		{"1KiB", 1024},              //
		{"1 GiB", 1 << 30},          //
		{"1 TiB", 1 << 40},          //
		{"1KB", 1000},               // decimal
		{"1MB", 1000 * 1000},        //
		{"1GB", 1000 * 1000 * 1000}, //
		{"512B", 512},               // bare B
		{"1.5 GiB", 1610612736},     // fractional, rounded
		{"1 mib", 1048576},          // case-insensitive
		{"  4 MiB  ", 4 * 1048576},  // surrounding whitespace
	}
	for _, c := range cases {
		got, err := ParseBytes(c.in)
		require.NoError(t, err, "input %q", c.in)
		assert.Equal(t, c.want, got, "input %q", c.in)
	}
}

func TestParseBytes_Errors(t *testing.T) {
	for _, in := range []string{
		"",              // empty
		"-5",            // negative bare
		"-1 MiB",        // negative with unit
		"1 PiB",         // unknown unit
		"1 foo",         // unknown unit
		"MiB",           // no number
		"1.2.3 MiB",     // bad number
		"abc",           // not a number
		"1.2",           // bare float: no unit means whole bytes, not silently rounded
		"100000000 TiB", // out of uint64 range
	} {
		_, err := ParseBytes(in)
		assert.Error(t, err, "input %q should error", in)
	}
}
