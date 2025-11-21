// Copyright 2025 Supabase, Inc.
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

package lsn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    LSN
		wantErr bool
	}{
		{
			name:  "zero LSN",
			input: "0/0",
			want:  LSN{high: 0, low: 0},
		},
		{
			name:  "small LSN",
			input: "0/3000028",
			want:  LSN{high: 0, low: 0x3000028},
		},
		{
			name:  "LSN with segment 1",
			input: "1/A0000000",
			want:  LSN{high: 1, low: 0xA0000000},
		},
		{
			name:  "max LSN",
			input: "FFFFFFFF/FFFFFFFF",
			want:  LSN{high: 0xFFFFFFFF, low: 0xFFFFFFFF},
		},
		{
			name:  "lowercase hex",
			input: "f/ffffffff",
			want:  LSN{high: 0xF, low: 0xFFFFFFFF},
		},
		{
			name:  "mixed case hex",
			input: "AbCd/12eF",
			want:  LSN{high: 0xABCD, low: 0x12EF},
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "missing separator",
			input:   "1234567",
			wantErr: true,
		},
		{
			name:    "too many parts",
			input:   "1/2/3",
			wantErr: true,
		},
		{
			name:    "invalid hex in high part",
			input:   "G/0",
			wantErr: true,
		},
		{
			name:    "invalid hex in low part",
			input:   "0/GGGG",
			wantErr: true,
		},
		{
			name:    "high part too large",
			input:   "100000000/0",
			wantErr: true,
		},
		{
			name:    "low part too large",
			input:   "0/100000000",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLSN_Compare(t *testing.T) {
	tests := []struct {
		name string
		a    LSN
		b    LSN
		want int
	}{
		{
			name: "equal LSNs",
			a:    LSN{high: 1, low: 1000},
			b:    LSN{high: 1, low: 1000},
			want: 0,
		},
		{
			name: "different high parts - a less",
			a:    LSN{high: 0, low: 0xFFFFFFFF},
			b:    LSN{high: 1, low: 0},
			want: -1,
		},
		{
			name: "different high parts - a greater",
			a:    LSN{high: 2, low: 0},
			b:    LSN{high: 1, low: 0xFFFFFFFF},
			want: 1,
		},
		{
			name: "same high, different low - a less",
			a:    LSN{high: 1, low: 100},
			b:    LSN{high: 1, low: 200},
			want: -1,
		},
		{
			name: "same high, different low - a greater",
			a:    LSN{high: 1, low: 200},
			b:    LSN{high: 1, low: 100},
			want: 1,
		},
		{
			name: "zero vs non-zero",
			a:    LSN{high: 0, low: 0},
			b:    LSN{high: 0, low: 1},
			want: -1,
		},
		{
			name: "max values",
			a:    LSN{high: 0xFFFFFFFF, low: 0xFFFFFFFF},
			b:    LSN{high: 0xFFFFFFFF, low: 0xFFFFFFFE},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Compare(tt.b)
			assert.Equal(t, tt.want, got)

			// Verify symmetry: if a < b, then b > a
			switch tt.want {
			case -1:
				assert.Equal(t, 1, tt.b.Compare(tt.a))
			case 1:
				assert.Equal(t, -1, tt.b.Compare(tt.a))
			default:
				assert.Equal(t, 0, tt.b.Compare(tt.a))
			}
		})
	}
}

func TestCompareStrings(t *testing.T) {
	tests := []struct {
		name    string
		a       string
		b       string
		want    int
		wantErr bool
	}{
		{
			name: "equal strings",
			a:    "0/3000028",
			b:    "0/3000028",
			want: 0,
		},
		{
			name: "lexicographically smaller but numerically larger segment",
			a:    "F/0",
			b:    "10/0",
			want: -1,
		},
		{
			name: "same segment, different offset",
			a:    "1/A0000000",
			b:    "1/B0000000",
			want: -1,
		},
		{
			name: "different segments",
			a:    "2/0",
			b:    "1/FFFFFFFF",
			want: 1,
		},
		{
			name: "zero vs small",
			a:    "0/0",
			b:    "0/1",
			want: -1,
		},
		{
			name: "large vs max",
			a:    "FFFFFFFE/FFFFFFFF",
			b:    "FFFFFFFF/0",
			want: -1,
		},
		{
			name:    "invalid first LSN",
			a:       "invalid",
			b:       "0/0",
			wantErr: true,
		},
		{
			name:    "invalid second LSN",
			a:       "0/0",
			b:       "invalid",
			wantErr: true,
		},
		{
			name:    "both invalid",
			a:       "bad",
			b:       "worse",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareStrings(tt.a, tt.b)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			// Verify symmetry
			gotReverse, err := CompareStrings(tt.b, tt.a)
			require.NoError(t, err)
			switch tt.want {
			case -1:
				assert.Equal(t, 1, gotReverse)
			case 1:
				assert.Equal(t, -1, gotReverse)
			default:
				assert.Equal(t, 0, gotReverse)
			}
		})
	}
}

func TestLSN_String(t *testing.T) {
	tests := []struct {
		name string
		lsn  LSN
		want string
	}{
		{
			name: "zero",
			lsn:  LSN{high: 0, low: 0},
			want: "0/0",
		},
		{
			name: "small LSN",
			lsn:  LSN{high: 0, low: 0x3000028},
			want: "0/3000028",
		},
		{
			name: "with segment",
			lsn:  LSN{high: 1, low: 0xA0000000},
			want: "1/A0000000",
		},
		{
			name: "max LSN",
			lsn:  LSN{high: 0xFFFFFFFF, low: 0xFFFFFFFF},
			want: "FFFFFFFF/FFFFFFFF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.lsn.String()
			assert.Equal(t, tt.want, got)

			// Verify round-trip
			parsed, err := Parse(got)
			require.NoError(t, err)
			assert.Equal(t, tt.lsn, parsed)
		})
	}
}

func TestGreater(t *testing.T) {
	tests := []struct {
		name    string
		a       string
		b       string
		want    bool
		wantErr bool
	}{
		{
			name: "a > b same segment",
			a:    "0/3000000",
			b:    "0/1000000",
			want: true,
		},
		{
			name: "a > b different segments",
			a:    "1/1000000",
			b:    "0/9000000",
			want: true,
		},
		{
			name: "a < b",
			a:    "0/1000000",
			b:    "0/3000000",
			want: false,
		},
		{
			name: "a == b",
			a:    "0/3000000",
			b:    "0/3000000",
			want: false,
		},
		{
			name:    "invalid a",
			a:       "invalid",
			b:       "0/1000000",
			wantErr: true,
		},
		{
			name:    "invalid b",
			a:       "0/1000000",
			b:       "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Greater(tt.a, tt.b)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGreaterOrEqual(t *testing.T) {
	tests := []struct {
		name    string
		a       string
		b       string
		want    bool
		wantErr bool
	}{
		{
			name: "a > b",
			a:    "0/3000000",
			b:    "0/1000000",
			want: true,
		},
		{
			name: "a == b",
			a:    "0/3000000",
			b:    "0/3000000",
			want: true,
		},
		{
			name: "a < b",
			a:    "0/1000000",
			b:    "0/3000000",
			want: false,
		},
		{
			name:    "invalid a",
			a:       "invalid",
			b:       "0/1000000",
			wantErr: true,
		},
		{
			name:    "invalid b",
			a:       "0/1000000",
			b:       "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GreaterOrEqual(tt.a, tt.b)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
