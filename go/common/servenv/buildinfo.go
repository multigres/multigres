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
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// buildSnapshot is the parsed, cached view of the binary's build identity,
// derived from runtime/debug.BuildInfo. All fields may be empty if
// -buildvcs was disabled at build time.
type buildSnapshot struct {
	// revision is the VCS commit SHA (typically a 40-char git SHA).
	revision string

	// modified is true when the working tree had uncommitted changes
	// at build time. Often true in dev builds — informational only.
	modified bool

	// commitTime is the timestamp on the source revision, not the
	// build time. Zero if unavailable.
	commitTime time.Time

	// goVersion is the Go toolchain that produced the binary.
	goVersion string

	// mainPath is the Go module path of the main package (e.g.
	// "github.com/multigres/multigres/go/cmd/multipooler").
	mainPath string
}

var (
	buildSnapshotOnce sync.Once
	buildSnapshotData buildSnapshot
)

// readBuildSnapshot returns the cached build snapshot. The first call
// reads runtime/debug.BuildInfo; subsequent calls return the cached copy.
func readBuildSnapshot() buildSnapshot {
	buildSnapshotOnce.Do(loadBuildSnapshot)
	return buildSnapshotData
}

func loadBuildSnapshot() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	buildSnapshotData.goVersion = info.GoVersion
	buildSnapshotData.mainPath = info.Main.Path
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			buildSnapshotData.revision = s.Value
		case "vcs.modified":
			buildSnapshotData.modified = s.Value == "true"
		case "vcs.time":
			if t, err := time.Parse(time.RFC3339, s.Value); err == nil {
				buildSnapshotData.commitTime = t
			}
		}
	}
}

// Version returns the short release version (versionName, e.g. "0.1.0-SNAPSHOT").
// It is the value reported by `SHOW multigres_version`, mirroring PostgreSQL's
// server_version. The full build string — release version plus VCS revision,
// commit date, and Go toolchain — is AppVersion, reported by
// `SELECT multigres.version()`.
func Version() string {
	return versionName
}

// AppVersion returns a one-line, human-readable description of the running
// binary's Multigres version, suitable for display to operators (e.g. via
// `SHOW multigres_version` or a future --version flag) and for pasting into bug
// reports. It is the release version plus the VCS identity the Go toolchain
// embeds; VCS fields are omitted so it degrades gracefully on builds without VCS
// stamping (e.g. inside a linked git worktree).
func AppVersion() string {
	return formatAppVersion(versionName, readBuildSnapshot())
}

// formatAppVersion is the pure formatting core of AppVersion, split out so the
// string layout can be unit-tested without depending on the ambient build info.
func formatAppVersion(version string, snap buildSnapshot) string {
	var b strings.Builder
	b.WriteString("Multigres")
	if version != "" {
		fmt.Fprintf(&b, " %s", version)
	}

	switch {
	case snap.revision != "":
		short := snap.revision
		if len(short) > 12 {
			short = short[:12]
		}
		fmt.Fprintf(&b, " (%s", short)
		if snap.modified {
			b.WriteString(", modified")
		}
		if !snap.commitTime.IsZero() {
			fmt.Fprintf(&b, ", %s", snap.commitTime.UTC().Format("2006-01-02"))
		}
		b.WriteByte(')')
	case snap.modified:
		b.WriteString(" (modified)")
	default:
		b.WriteString(" (unknown revision)")
	}

	if snap.goVersion != "" {
		fmt.Fprintf(&b, " built with %s", snap.goVersion)
	}
	return b.String()
}
