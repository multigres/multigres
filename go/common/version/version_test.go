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

package version

import (
	"strings"
	"testing"
)

// TestRead exercises Read against the test binary's own embedded build
// info. Specific revision SHAs cannot be asserted (those vary per
// checkout), so the test verifies the fields that are guaranteed by
// the Go toolchain: GoVersion is always populated and the snapshot is
// cached across calls.
func TestRead(t *testing.T) {
	snap := Read()
	if snap.GoVersion == "" {
		t.Error("GoVersion is empty, runtime/debug.BuildInfo should always provide it")
	}
	if !strings.HasPrefix(snap.GoVersion, "go") {
		t.Errorf("GoVersion = %q, expected prefix \"go\"", snap.GoVersion)
	}
	if Read() != snap {
		t.Error("Read returned a different value on second call; expected cached snapshot")
	}
}

func TestString(t *testing.T) {
	out := String("testbin")
	if !strings.HasPrefix(out, "testbin\n") {
		t.Errorf("String output = %q, expected to start with %q", out, "testbin\n")
	}
	for _, field := range []string{"version:", "commit:", "committed:", "go:", "platform:"} {
		if !strings.Contains(out, field) {
			t.Errorf("String output missing %q field:\n%s", field, out)
		}
	}
}
