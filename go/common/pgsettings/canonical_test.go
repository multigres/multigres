// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgsettings

import "testing"

func TestCanonicalGUCName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "Work_Mem", want: "work_mem"},
		{in: "SESSION_AUTHORIZATION", want: "session_authorization"},
		{in: "Foo.Bar", want: "foo.bar"},
		{in: "my.Ä", want: "my.Ä"},
		{in: "my.ä", want: "my.ä"},
	}
	for _, tt := range tests {
		if got := CanonicalGUCName(tt.in); got != tt.want {
			t.Fatalf("CanonicalGUCName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
