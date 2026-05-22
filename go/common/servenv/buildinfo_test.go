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

package servenv

import (
	"strings"
	"testing"
)

// TestReadBuildSnapshot exercises readBuildSnapshot against the test
// binary's own embedded build info. We cannot assert specific revision
// SHAs (those vary per checkout), so we verify the fields that are
// guaranteed by the Go toolchain: goVersion is always populated and
// the snapshot is cached across calls.
func TestReadBuildSnapshot(t *testing.T) {
	snap := readBuildSnapshot()
	if snap.goVersion == "" {
		t.Error("goVersion is empty; runtime/debug.BuildInfo should always provide it")
	}
	if !strings.HasPrefix(snap.goVersion, "go") {
		t.Errorf("goVersion = %q, expected prefix \"go\"", snap.goVersion)
	}
	if readBuildSnapshot() != snap {
		t.Error("readBuildSnapshot returned a different value on second call; expected cached snapshot")
	}
}
