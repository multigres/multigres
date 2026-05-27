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

// Package version is the single source of truth for build identity
// across every multigres binary. It combines the release tag (manually
// injected at link time via -ldflags -X) with the VCS metadata that
// the Go toolchain embeds automatically through runtime/debug.BuildInfo.
//
// Callers should use this package — not runtime/debug.BuildInfo
// directly — so the CLI --version flag, the HTTP /version endpoint,
// the gRPC ServiceInfo response, the OTel service.version resource
// attribute, and the OCI image labels all agree.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

// Version is the release tag (e.g. "v0.1.0"). Populated at link time by
// goreleaser / the Makefile via -ldflags -X. Defaults to "dev" so local
// builds still produce readable output.
var Version = "dev"

// Snapshot is the cached, parsed view of runtime/debug.BuildInfo. Every
// field may be empty if the binary was built with -buildvcs=false.
type Snapshot struct {
	// Revision is the VCS commit SHA (typically a 40-char git SHA).
	Revision string

	// Modified is true when the working tree had uncommitted changes
	// at build time. Often true in dev builds — informational only.
	Modified bool

	// CommitTime is the timestamp on the source revision, not the
	// build time. Zero if unavailable.
	CommitTime time.Time

	// GoVersion is the Go toolchain that produced the binary.
	GoVersion string

	// MainPath is the Go module path of the main package (e.g.
	// "github.com/multigres/multigres/go/cmd/multipooler").
	MainPath string
}

var (
	snapshotOnce sync.Once
	snapshotData Snapshot
)

// Read returns the cached build snapshot. The first call reads
// runtime/debug.BuildInfo; subsequent calls return the cached copy.
func Read() Snapshot {
	snapshotOnce.Do(loadSnapshot)
	return snapshotData
}

func loadSnapshot() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	snapshotData.GoVersion = info.GoVersion
	snapshotData.MainPath = info.Main.Path
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			snapshotData.Revision = s.Value
		case "vcs.modified":
			snapshotData.Modified = s.Value == "true"
		case "vcs.time":
			if t, err := time.Parse(time.RFC3339, s.Value); err == nil {
				snapshotData.CommitTime = t
			}
		}
	}
}

// String returns the human-readable build identity for binary, suitable
// for cobra's Command.Version value. Combines the release tag with the
// VCS revision (with a "modified" suffix when the working tree was
// dirty), the commit timestamp, the Go toolchain version, and the
// runtime GOOS/GOARCH.
func String(binary string) string {
	snap := Read()
	revision := snap.Revision
	if revision == "" {
		revision = "unknown"
	} else if snap.Modified {
		revision += " (modified)"
	}
	committed := "unknown"
	if !snap.CommitTime.IsZero() {
		committed = snap.CommitTime.UTC().Format(time.RFC3339)
	}
	goVersion := snap.GoVersion
	if goVersion == "" {
		goVersion = runtime.Version()
	}
	return fmt.Sprintf(
		"%s\n  version:   %s\n  commit:    %s\n  committed: %s\n  go:        %s\n  platform:  %s/%s",
		binary, Version, revision, committed, goVersion, runtime.GOOS, runtime.GOARCH,
	)
}
