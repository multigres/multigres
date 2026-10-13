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
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgeval/datum"
)

func TestIntegerInput(t *testing.T) {
	t.Parallel()
	for _, typ := range []struct {
		prefix, name string
		oid          pgoid.Oid
	}{
		{"int2", "smallint", pgoid.INT2OID},
		{"int4", "integer", pgoid.INT4OID},
		{"int8", "bigint", pgoid.INT8OID},
	} {
		t.Run(typ.prefix, func(t *testing.T) {
			for input, want := range map[string]int64{
				"0": 0, "-0": 0, "+0": 0, "000": 0, "010": 10, "08": 8,
				"123": 123, "+123": 123, "-123": -123, "1_2_3": 123,
				" \t\n\r\v\f-123 \t\n\r\v\f": -123,
				"0x7b":                       123, "0X_7B": 123, "-0x7_B": -123,
				"0o173": 123, "0O_173": 123, "-0o1_73": -123,
				"0b1111011": 123, "0B_1111011": 123, "-0b111_1011": -123,
				"123\x00ignored": 123,
			} {
				t.Run(input, func(t *testing.T) {
					got, err := scalarCall(t, typ.prefix+"in", datum.BytesGetDatum([]byte(input)))
					require.NoError(t, err)
					assert.Equal(t, want, datum.DatumGetInt64(got.Value))
				})
			}
			lo, hi := integerLimits(typ.oid)
			for _, n := range []int64{lo, hi} {
				for _, base := range []int{2, 8, 10, 16} {
					text := strconv.FormatInt(n, base)
					prefix := map[int]string{2: "0b", 8: "0o", 10: "", 16: "0x"}[base]
					if n < 0 {
						text = "-" + prefix + text[1:]
					} else {
						text = prefix + text
					}
					got, err := scalarCall(t, typ.prefix+"in", datum.BytesGetDatum([]byte(text)))
					require.NoError(t, err)
					assert.Equal(t, n, datum.DatumGetInt64(got.Value))
				}
			}
			invalid := []string{
				"", " \t", "+", "-", "--1", "1 2", "0x", "0x_", "0x__1", "0xG",
				"0b", "0b2", "0b__1", "0o", "0o8", "0o__1", "0x1_", "0o1_", "0b1_",
				"_1", "+_1", "1_", "1__0", "1_\t0", "00x1", "0d12", "1.0",
				"１２", "١٢", "\u00a0123", "123\u2003", "0x-1", "1\"2", "1\\2",
			}
			aboveMax := new(big.Int).Add(big.NewInt(hi), big.NewInt(1)).String()
			belowMin := new(big.Int).Sub(big.NewInt(lo), big.NewInt(1)).String()
			// Syntax wins if the last digit only just exceeded the signed
			// range; range wins when the pre-digit overflow check already fired.
			invalid = append(invalid, aboveMax+"x", aboveMax+"_", belowMin+"x")
			for _, input := range invalid {
				_, err := scalarCall(t, typ.prefix+"in", datum.BytesGetDatum([]byte(input)))
				var diag *mterrors.PgDiagnostic
				require.ErrorAs(t, err, &diag, "input %q", input)
				assert.Equal(t, "22P02", diag.Code, "input %q", input)
				assert.Equal(t, fmt.Sprintf("invalid input syntax for type %s: \"%s\"", typ.name, input), diag.Message)
			}
			for _, input := range []string{aboveMax, belowMin, aboveMax + "0x", "+" + aboveMax + "0x", strings.Repeat("9", 100)} {
				_, err := scalarCall(t, typ.prefix+"in", datum.BytesGetDatum([]byte(input)))
				var diag *mterrors.PgDiagnostic
				require.ErrorAs(t, err, &diag, "input %q", input)
				assert.Equal(t, "22003", diag.Code, "input %q", input)
				assert.Equal(t, fmt.Sprintf("value \"%s\" is out of range for type %s", input, typ.name), diag.Message)
			}
		})
	}
}

func TestBooleanInput(t *testing.T) {
	t.Parallel()
	for _, word := range []string{"true", "false", "yes", "no", "on", "off", "1", "0"} {
		want := word == "true" || word == "yes" || word == "on" || word == "1"
		for n := 1; n <= len(word); n++ {
			if word[:n] == "o" {
				continue
			}
			for _, input := range []string{word[:n], strings.ToUpper(word[:n]), " \t\r\n\v\f" + word[:n] + " \t\r\n\v\f"} {
				got, err := scalarCall(t, "boolin", datum.BytesGetDatum([]byte(input)))
				require.NoError(t, err)
				assert.Equal(t, want, datum.DatumGetBool(got.Value), "input %q", input)
			}
		}
	}
	for _, input := range []string{"", " ", "o", "O", "t r", "truee", "10", "01", "-1", "2", "\u00a0true", "false\u2003", "falſe", "1\"2", "1\\2"} {
		_, err := scalarCall(t, "boolin", datum.BytesGetDatum([]byte(input)))
		var diag *mterrors.PgDiagnostic
		require.ErrorAs(t, err, &diag, "input %q", input)
		assert.Equal(t, "22P02", diag.Code)
		assert.Equal(t, "invalid input syntax for type boolean: \""+input+"\"", diag.Message)
	}
}
