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
	"time"
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

// TestFormatAppVersion pins the string layout of the version reported by
// `SHOW multigres.server_version` across the combinations of build fields that may be
// present or absent.
func TestFormatAppVersion(t *testing.T) {
	commit := time.Date(2026, 7, 10, 15, 4, 5, 0, time.UTC)

	tests := []struct {
		name    string
		version string
		snap    buildSnapshot
		want    string
	}{
		{
			name: "revision, commit time, and go version",
			snap: buildSnapshot{revision: "a761b254c0ffeedeadbeef", commitTime: commit, goVersion: "go1.23.4"},
			want: "Multigres (a761b254c0ff, 2026-07-10) built with go1.23.4",
		},
		{
			name: "modified working tree",
			snap: buildSnapshot{revision: "a761b254c0ffee", modified: true, commitTime: commit, goVersion: "go1.23.4"},
			want: "Multigres (a761b254c0ff, modified, 2026-07-10) built with go1.23.4",
		},
		{
			name:    "stamped release version",
			version: "v0.1.0",
			snap:    buildSnapshot{revision: "a761b254c0ffee", commitTime: commit, goVersion: "go1.23.4"},
			want:    "Multigres v0.1.0 (a761b254c0ff, 2026-07-10) built with go1.23.4",
		},
		{
			name: "short revision is not truncated",
			snap: buildSnapshot{revision: "a761b25", goVersion: "go1.23.4"},
			want: "Multigres (a761b25) built with go1.23.4",
		},
		{
			name: "no vcs info at all",
			snap: buildSnapshot{goVersion: "go1.23.4"},
			want: "Multigres (unknown revision) built with go1.23.4",
		},
		{
			name: "no vcs revision but modified flag",
			snap: buildSnapshot{modified: true, goVersion: "go1.23.4"},
			want: "Multigres (modified) built with go1.23.4",
		},
		{
			name: "empty snapshot",
			snap: buildSnapshot{},
			want: "Multigres (unknown revision)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatAppVersion(tt.version, tt.snap); got != tt.want {
				t.Errorf("formatAppVersion(%q, %+v) = %q, want %q", tt.version, tt.snap, got, tt.want)
			}
		})
	}
}

// TestAppVersion smoke-tests the exported entry point against the test binary's
// own build info: it must always start with "Multigres" and never be empty.
func TestAppVersion(t *testing.T) {
	got := AppVersion()
	if !strings.HasPrefix(got, "Multigres") {
		t.Errorf("AppVersion() = %q, expected it to start with \"Multigres\"", got)
	}
}

// TestVersion checks the short release accessor returns the committed release
// version constant.
func TestVersion(t *testing.T) {
	if got := Version(); got != versionName {
		t.Errorf("Version() = %q, want %q", got, versionName)
	}
	if Version() == "" {
		t.Error("Version() is empty; versionName must always be set")
	}
}
