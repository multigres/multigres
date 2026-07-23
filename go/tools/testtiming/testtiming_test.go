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

package testtiming

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedValue captures the JSON value that Record would emit, by exercising
// the same marshaling path. It mirrors Record without the testing.T.Attr call,
// which cannot be observed in-process.
func recordedValue(t *testing.T, op string, elapsed, limit time.Duration) string {
	t.Helper()
	b, err := json.Marshal(Measurement{Op: op, Elapsed: elapsed, Limit: limit})
	require.NoError(t, err)
	return string(b)
}

// TestRecordParseRoundTrip locks the emit/consume contract: a value produced the
// way Record produces it parses back into the original Measurement. Durations
// serialize as int64 nanoseconds (45s = 45e9, 1m = 60e9).
func TestRecordParseRoundTrip(t *testing.T) {
	value := recordedValue(t, "shard bootstrap", 45*time.Second, time.Minute)
	assert.JSONEq(t, `{"op":"shard bootstrap","elapsed":45000000000,"limit":60000000000}`, value)

	stream := `{"Action":"attr","Package":"p","Test":"T","Key":"` + AttrKey + `","Value":` +
		mustJSONString(t, value) + "}\n"
	got, err := Parse(strings.NewReader(stream))
	require.NoError(t, err)
	require.Equal(t, []Measurement{{Op: "shard bootstrap", Elapsed: 45 * time.Second, Limit: time.Minute}}, got)
}

func TestParseSkipsNonTimingEvents(t *testing.T) {
	const fixture = `not json at all
{"Action":"run","Package":"p","Test":"T"}
{"Action":"output","Package":"p","Test":"T","Output":"=== RUN T\n"}
{"Action":"attr","Package":"p","Test":"T","Key":"other","Value":"ignored"}
{"Action":"attr","Package":"p","Test":"T","Key":"mg_timing","Value":"{bad json}"}
{"Action":"attr","Package":"p","Test":"T","Key":"mg_timing","Value":"{\"op\":\"manager ready\",\"elapsed\":3000000000,\"limit\":30000000000}"}
{"Action":"pass","Package":"p","Test":"T"}
`
	got, err := Parse(strings.NewReader(fixture))
	require.NoError(t, err)
	require.Equal(t, []Measurement{{Op: "manager ready", Elapsed: 3 * time.Second, Limit: 30 * time.Second}}, got)
}

// mustJSONString returns s encoded as a JSON string literal (with surrounding
// quotes and escaping), so it can be embedded as an attribute Value.
func mustJSONString(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return string(b)
}
