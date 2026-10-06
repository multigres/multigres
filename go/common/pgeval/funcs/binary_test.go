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

package funcs_test

import (
	"encoding/hex"
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/funcs"
)

func TestBinaryIntegers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		typ   string
		value int64
		hex   string
	}{
		{"int2", 0, "0000"},
		{"int2", -1, "ffff"},
		{"int2", math.MinInt16, "8000"},
		{"int2", math.MaxInt16, "7fff"},
		{"int2", 0x0102, "0102"},
		{"int4", 0, "00000000"},
		{"int4", -1, "ffffffff"},
		{"int4", math.MinInt32, "80000000"},
		{"int4", math.MaxInt32, "7fffffff"},
		{"int4", 0x01020304, "01020304"},
		{"int8", 0, "0000000000000000"},
		{"int8", -1, "ffffffffffffffff"},
		{"int8", math.MinInt64, "8000000000000000"},
		{"int8", math.MaxInt64, "7fffffffffffffff"},
		{"int8", 0x0102030405060708, "0102030405060708"},
	} {
		t.Run(tc.typ+"/"+tc.hex, func(t *testing.T) {
			data, err := hex.DecodeString(tc.hex)
			require.NoError(t, err)
			sent, err := scalarCall(t, tc.typ+"send", datum.Int64GetDatum(tc.value))
			require.NoError(t, err)
			assert.Equal(t, data, datum.DatumGetBytes(sent.Value))
			input := funcs.NewBinaryInput(data)
			got, err := scalarCall(t, tc.typ+"recv", input.Datum())
			require.NoError(t, err)
			assert.Equal(t, tc.value, datum.DatumGetInt64(got.Value))
			assert.Zero(t, input.Remaining())
			for n := range len(data) {
				short := funcs.NewBinaryInput(data[:n])
				_, err := scalarCall(t, tc.typ+"recv", short.Datum())
				var diag *mterrors.PgDiagnostic
				require.ErrorAs(t, err, &diag)
				assert.Equal(t, "08P01", diag.Code)
				assert.Equal(t, "insufficient data left in message", diag.Message)
				assert.Equal(t, n, short.Remaining(), "failed read must not consume bytes")
			}
		})
	}
}

func TestBinaryBooleans(t *testing.T) {
	t.Parallel()
	for n := 0; n <= 255; n++ {
		input := funcs.NewBinaryInput([]byte{byte(n)})
		got, err := scalarCall(t, "boolrecv", input.Datum())
		require.NoError(t, err)
		assert.Equal(t, n != 0, datum.DatumGetBool(got.Value))
		assert.Zero(t, input.Remaining())
		sent, err := scalarCall(t, "boolsend", got.Value)
		require.NoError(t, err)
		want := byte(0)
		if n != 0 {
			want = 1
		}
		assert.Equal(t, []byte{want}, datum.DatumGetBytes(sent.Value))
	}
}

func TestBinaryInputCursorAndLifetime(t *testing.T) {
	t.Parallel()
	input := funcs.NewBinaryInput([]byte{2, 0, 0, 0, 42, 255, 255, 170})
	for _, tc := range []struct {
		src       string
		want      int64
		remaining int
	}{{"boolrecv", 1, 7}, {"int4recv", 42, 3}, {"int2recv", -1, 1}} {
		got, err := scalarCall(t, tc.src, input.Datum())
		require.NoError(t, err)
		assert.Equal(t, tc.want, datum.DatumGetInt64(got.Value))
		assert.Equal(t, tc.remaining, input.Remaining())
	}
	_, err := scalarCall(t, "int2recv", input.Datum())
	require.Error(t, err)
	assert.Equal(t, 1, input.Remaining())
	_, err = scalarCall(t, "boolrecv", input.Datum())
	require.NoError(t, err)
	_, err = scalarCall(t, "boolrecv", input.Datum())
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	assert.Equal(t, "08P01", diag.Code)
	assert.Equal(t, "no data left in message", diag.Message)
	assert.Zero(t, input.Remaining())

	// Neither the BinaryInput nor its backing slice has another live owner.
	arg := funcs.NewBinaryInput([]byte{0, 0, 0, 42}).Datum()
	runtime.GC()
	got, err := scalarCall(t, "int4recv", arg)
	require.NoError(t, err)
	assert.Equal(t, int32(42), datum.DatumGetInt32(got.Value))
}

func FuzzBinaryInput(f *testing.F) {
	for _, data := range [][]byte{{}, {0}, {255}, {128, 0}, {1, 2, 3, 4, 5, 6, 7, 8, 9}} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, tc := range []struct {
			typ  string
			size int
		}{{"bool", 1}, {"int2", 2}, {"int4", 4}, {"int8", 8}} {
			input := funcs.NewBinaryInput(data)
			got, err := scalarCall(t, tc.typ+"recv", input.Datum())
			if len(data) < tc.size {
				var diag *mterrors.PgDiagnostic
				require.ErrorAs(t, err, &diag)
				require.Equal(t, "08P01", diag.Code)
				require.Equal(t, len(data), input.Remaining())
				continue
			}
			require.NoError(t, err)
			require.False(t, got.IsNull)
			require.Equal(t, len(data)-tc.size, input.Remaining())
			sent, err := scalarCall(t, tc.typ+"send", got.Value)
			require.NoError(t, err)
			want := data[:tc.size]
			if tc.typ == "bool" {
				want = []byte{0}
				if data[0] != 0 {
					want[0] = 1
				}
			}
			require.Equal(t, want, datum.DatumGetBytes(sent.Value))
		}
	})
}
