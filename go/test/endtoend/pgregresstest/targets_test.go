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

package pgregresstest

import (
	"strings"
	"testing"
)

func TestParseTargets(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []Target
		wantErr string
	}{
		{name: "empty defaults to multigateway", in: "", want: []Target{TargetMultigateway}},
		{name: "whitespace only defaults to multigateway", in: "   ", want: []Target{TargetMultigateway}},
		{name: "single multigateway", in: "multigateway", want: []Target{TargetMultigateway}},
		{name: "all three in canonical order", in: "multigateway,pgbouncer-session,pgbouncer-tx", want: []Target{TargetMultigateway, TargetPgbouncerSession, TargetPgbouncerTx}},
		{name: "preserves caller order", in: "pgbouncer-tx,multigateway", want: []Target{TargetPgbouncerTx, TargetMultigateway}},
		{name: "dedups while preserving first-seen order", in: "multigateway,pgbouncer-tx,multigateway", want: []Target{TargetMultigateway, TargetPgbouncerTx}},
		{name: "ignores surrounding whitespace", in: " multigateway , pgbouncer-session ", want: []Target{TargetMultigateway, TargetPgbouncerSession}},
		{name: "unknown target rejected", in: "multigateway,bogus", wantErr: `unknown target "bogus"`},
		{name: "trailing comma without name is ignored", in: "multigateway,", want: []Target{TargetMultigateway}},
		{name: "only commas yields error", in: ",,", wantErr: "no usable target"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTargets(tc.in)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ParseTargets(%q) = %v, nil; want error containing %q", tc.in, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParseTargets(%q) error = %q; want substring %q", tc.in, err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTargets(%q) returned unexpected error: %v", tc.in, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ParseTargets(%q) length = %d (%v); want %d (%v)", tc.in, len(got), got, len(tc.want), tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ParseTargets(%q)[%d] = %q; want %q", tc.in, i, got[i], tc.want[i])
				}
			}
		})
	}
}
