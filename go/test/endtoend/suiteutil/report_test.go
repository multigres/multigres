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

package suiteutil

import "testing"

func TestNewBadgeEndpoint(t *testing.T) {
	tests := []struct {
		name                    string
		label                   string
		passed, total, expected int
		timedOut                bool
		wantMessage, wantColor  string
	}{
		{
			name:        "partial pass",
			label:       "Regression",
			passed:      160,
			total:       222,
			wantMessage: "160/222 passed",
			wantColor:   "orange", // 72% -> 50..79% -> orange
		},
		{
			name:        "all pass",
			label:       "Isolation",
			passed:      80,
			total:       80,
			wantMessage: "80/80 passed",
			wantColor:   "brightgreen",
		},
		{
			name:        "timed out downgrades brightgreen and annotates",
			label:       "Contrib Extension",
			passed:      10,
			total:       10,
			timedOut:    true,
			wantMessage: "10/10 passed (timed out)",
			wantColor:   "yellow",
		},
		{
			name:        "partial run shows expected denominator",
			label:       "Regression",
			passed:      100,
			total:       150,
			expected:    222,
			timedOut:    true,
			wantMessage: "100/150 passed (of 222) (timed out)",
			wantColor:   "orange", // 66% -> orange, not brightgreen so no downgrade
		},
		{
			name:        "no data",
			label:       "Overall",
			passed:      0,
			total:       0,
			wantMessage: "0/0 passed",
			wantColor:   "lightgrey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep := NewBadgeEndpoint(tt.label, tt.passed, tt.total, tt.expected, tt.timedOut)
			if ep.SchemaVersion != 1 {
				t.Errorf("SchemaVersion = %d, want 1", ep.SchemaVersion)
			}
			if ep.Label != tt.label {
				t.Errorf("Label = %q, want %q", ep.Label, tt.label)
			}
			if ep.Message != tt.wantMessage {
				t.Errorf("Message = %q, want %q", ep.Message, tt.wantMessage)
			}
			if ep.Color != tt.wantColor {
				t.Errorf("Color = %q, want %q", ep.Color, tt.wantColor)
			}
		})
	}
}

func TestBadgeSlug(t *testing.T) {
	cases := map[string]string{
		"Regression":        "regression",
		"Isolation":         "isolation",
		"Contrib Extension": "contrib-extension",
		"Overall":           "overall",
		"  Spaced  ":        "spaced",
	}
	for in, want := range cases {
		if got := BadgeSlug(in); got != want {
			t.Errorf("BadgeSlug(%q) = %q, want %q", in, got, want)
		}
	}
}
