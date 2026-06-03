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

package pgproto

import "testing"

func TestReduceErrorLine(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "postgres error with full source-location fields",
			in:   `<= BE ErrorResponse(S ERROR V ERROR C 42P01 M relation "x" does not exist P 15 F parse_relation.c L 1452 R parserOpenTable )`,
			want: "<= BE ErrorResponse(C 42P01)",
		},
		{
			name: "multigateway error with fewer fields reduces to the same",
			in:   `<= BE ErrorResponse(S ERROR V ERROR C 42P01 M relation "x" does not exist P 15 )`,
			want: "<= BE ErrorResponse(C 42P01)",
		},
		{
			name: "parse error: differing message/position collapse to the SQLSTATE",
			in:   `<= BE ErrorResponse(S ERROR V ERROR C 42601 M parse error at position 17: syntax error )`,
			want: "<= BE ErrorResponse(C 42601)",
		},
		{
			name: "multigres internal code is preserved verbatim",
			in:   `<= BE ErrorResponse(S ERROR V ERROR C MTD04 M parse failed D parse error at position 17: syntax error )`,
			want: "<= BE ErrorResponse(C MTD04)",
		},
		{
			name: "notice response",
			in:   `<= BE NoticeResponse(S WARNING V WARNING C 01000 M something happened )`,
			want: "<= BE NoticeResponse(C 01000)",
		},
		{
			name: "code is taken from the C field, not a look-alike token in the message",
			in:   `<= BE ErrorResponse(S ERROR V ERROR C 22023 M bad value ABCDE here )`,
			want: "<= BE ErrorResponse(C 22023)",
		},
		{
			name: "non-error backend line is untouched",
			in:   "<= BE CommandComplete(SELECT 3)",
			want: "<= BE CommandComplete(SELECT 3)",
		},
		{
			name: "ready-for-query is untouched",
			in:   "<= BE ReadyForQuery(I)",
			want: "<= BE ReadyForQuery(I)",
		},
		{
			name: "frontend line is untouched",
			in:   `FE=> Query (query="SELECT 1")`,
			want: `FE=> Query (query="SELECT 1")`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reduceErrorLine(tt.in); got != tt.want {
				t.Errorf("reduceErrorLine(%q)\n  got  %q\n  want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestNormalizeTraceErrorParity locks in the behaviour the suite depends on: a
// PostgreSQL error trace and a multigateway error trace that differ only in the
// dropped fields (message, position, source location) normalize to the same
// string, while a genuine SQLSTATE difference does not.
func TestNormalizeTraceErrorParity(t *testing.T) {
	pg := `FE=> Query (query="SELECT * FROM missing")
<= BE ErrorResponse(S ERROR V ERROR C 42P01 M relation "missing" does not exist P 15 F parse_relation.c L 1452 R parserOpenTable )
<= BE ReadyForQuery(I)`

	mgSame := `FE=> Query (query="SELECT * FROM missing")
<= BE ErrorResponse(S ERROR V ERROR C 42P01 M relation "missing" does not exist P 15 )
<= BE ReadyForQuery(I)`

	if normalizeTrace(pg) != normalizeTrace(mgSame) {
		t.Errorf("traces differing only in dropped fields should normalize equal:\n pg=%q\n mg=%q",
			normalizeTrace(pg), normalizeTrace(mgSame))
	}

	mgDifferentCode := `FE=> Query (query="SELECT * FROM missing")
<= BE ErrorResponse(S ERROR V ERROR C 08P01 M something else )
<= BE ReadyForQuery(I)`

	if normalizeTrace(pg) == normalizeTrace(mgDifferentCode) {
		t.Errorf("traces with different SQLSTATE must NOT normalize equal, but both were %q",
			normalizeTrace(pg))
	}
}
