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

package backup

import (
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

const sha1Suffix = "-0123456789abcdef0123456789abcdef01234567.gz"

func repoLsJSON(entries ...string) string {
	parts := []string{`".":{"type":"path"}`}
	for _, entry := range entries {
		name, entryType, _ := strings.Cut(entry, ":")
		parts = append(parts, fmt.Sprintf("%q:{\"type\":%q}", name, entryType))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func walSegments(timelineLog string, count int) []string {
	entries := make([]string, 0, count+1)
	for i := 1; i <= count; i++ {
		entries = append(entries, fmt.Sprintf("%s%08X%s:file", timelineLog, i, sha1Suffix))
	}
	return append(entries, timelineLog+"00000002.00000028.backup:file")
}

// stubRepoLs serves `repo-ls` from listings keyed by the listed path. A path
// without a listing lists as empty. A listing of "FAIL" makes the command exit 1.
func stubRepoLs(t *testing.T, listings map[string]string) {
	t.Helper()
	dir := t.TempDir()
	for path, listing := range listings {
		name := strings.ReplaceAll(path, "/", "_")
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(listing), 0o644))
	}
	script := fmt.Sprintf(`#!/bin/bash
last="${@: -1}"
f=%q/"${last//\//_}"
if [ ! -f "$f" ]; then
  echo '{".":{"type":"path"}}'
elif [ "$(cat "$f")" = FAIL ]; then
  echo "ERROR: [039]: unable to connect to repository"
  exit 1
else
  cat "$f"
fi
`, dir)
	stubPgbackrest(t, script)
}

func TestArchiveInUse(t *testing.T) {
	const (
		stanzaArchive = "archive/multigres"
		idDir         = stanzaArchive + "/17-1"
		walDir        = idDir + "/0000000100000000"
	)
	oneID := map[string]string{
		stanzaArchive: repoLsJSON("17-1:path", "archive.info:file", "archive.info.copy:file"),
	}
	with := func(extra map[string]string) map[string]string {
		listings := map[string]string{}
		maps.Copy(listings, oneID)
		maps.Copy(listings, extra)
		return listings
	}

	tests := []struct {
		name     string
		listings map[string]string
		want     bool
	}{
		{name: "no archive directory", listings: map[string]string{}},
		{
			name:     "archive info files only",
			listings: map[string]string{stanzaArchive: repoLsJSON("archive.info:file", "archive.info.copy:file")},
		},
		{name: "archive id without WAL", listings: with(map[string]string{idDir: repoLsJSON()})},
		{
			name: "segments a complete bootstrap leaves",
			listings: with(map[string]string{
				idDir:  repoLsJSON("0000000100000000:path"),
				walDir: repoLsJSON(walSegments("0000000100000000", maxBootstrapArchiveSegments)...),
			}),
		},
		{
			name: "one segment more than a bootstrap leaves",
			listings: with(map[string]string{
				idDir:  repoLsJSON("0000000100000000:path"),
				walDir: repoLsJSON(walSegments("0000000100000000", maxBootstrapArchiveSegments+1)...),
			}),
			want: true,
		},
		{
			name: "several archive ids",
			listings: map[string]string{
				stanzaArchive: repoLsJSON("17-1:path", "17-2:path", "archive.info:file"),
			},
			want: true,
		},
		{
			name: "timeline history file",
			listings: with(map[string]string{
				idDir: repoLsJSON("0000000100000000:path", "00000002.history:file"),
			}),
			want: true,
		},
		{
			name: "several WAL directories",
			listings: with(map[string]string{
				idDir: repoLsJSON("0000000100000000:path", "0000000100000001:path"),
			}),
			want: true,
		},
		{
			name: "segments on a later timeline",
			listings: with(map[string]string{
				idDir: repoLsJSON("0000000200000000:path"),
			}),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubRepoLs(t, tt.listings)
			e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "/tmp/backups")

			got, err := e.ArchiveInUse(t.Context())

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestArchiveInUse_ErrorsWhenArchiveCannotBeListed(t *testing.T) {
	const (
		stanzaArchive = "archive/multigres"
		idDir         = stanzaArchive + "/17-1"
		walDir        = idDir + "/0000000100000000"
	)
	healthy := map[string]string{
		stanzaArchive: repoLsJSON("17-1:path"),
		idDir:         repoLsJSON("0000000100000000:path"),
		walDir:        repoLsJSON(walSegments("0000000100000000", 1)...),
	}
	tests := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{name: "stanza archive listing fails", mutate: func(l map[string]string) { l[stanzaArchive] = "FAIL" }},
		{name: "archive id listing fails", mutate: func(l map[string]string) { l[idDir] = "FAIL" }},
		{name: "WAL directory listing fails", mutate: func(l map[string]string) { l[walDir] = "FAIL" }},
		{name: "invalid JSON", mutate: func(l map[string]string) { l[stanzaArchive] = "not json" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listings := maps.Clone(healthy)
			tt.mutate(listings)
			stubRepoLs(t, listings)
			e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "/tmp/backups")

			inUse, err := e.ArchiveInUse(t.Context())

			require.Error(t, err)
			assert.False(t, inUse)
		})
	}
}

// An entry whose name pgBackRest does not write is not something a failed
// bootstrap leaves, and the sidecar would refuse to list it: it counts as in use
// instead of failing every classification.
func TestArchiveInUse_UnexpectedNamesCountAsInUse(t *testing.T) {
	const (
		stanzaArchive = "archive/multigres"
		idDir         = stanzaArchive + "/17-1"
	)
	tests := []struct {
		name     string
		listings map[string]string
	}{
		{name: "archive id of an unknown shape", listings: map[string]string{stanzaArchive: repoLsJSON("9.6-1:path")}},
		{name: "archive id with a traversal", listings: map[string]string{stanzaArchive: repoLsJSON("..:path")}},
		{name: "stray directory beside the archive id", listings: map[string]string{stanzaArchive: repoLsJSON("17-1:path", "stray:path")}},
		{
			name: "WAL directory of an unknown shape",
			listings: map[string]string{
				stanzaArchive: repoLsJSON("17-1:path"),
				idDir:         repoLsJSON("0000000100000000:path", "tmp:path"),
			},
		},
		{
			name: "only WAL directory of an unknown shape",
			listings: map[string]string{
				stanzaArchive: repoLsJSON("17-1:path"),
				idDir:         repoLsJSON("00000001:path"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeSidecar{}
			e := newSidecarTestEngine(t, fake)
			listings := tt.listings
			fake.reply = func(_ pb.Command, _ []string, stdout, _ io.Writer) int {
				fake.mu.Lock()
				last := fake.calls[len(fake.calls)-1]
				fake.mu.Unlock()
				path := "archive/multigres"
				if last.archiveID != "" {
					path += "/" + last.archiveID
				}
				listing, ok := listings[path]
				if !ok {
					listing = repoLsJSON()
				}
				_, _ = io.WriteString(stdout, listing)
				return 0
			}

			inUse, err := e.ArchiveInUse(t.Context())

			require.NoError(t, err)
			assert.True(t, inUse)
			for _, c := range fake.calls {
				assert.True(t, c.archiveID == "" || commonbackup.ValidArchiveID(c.archiveID), "only valid names are sent to the sidecar: %q", c.archiveID)
				assert.True(t, c.walDir == "" || commonbackup.ValidWALDir(c.walDir), "only valid names are sent to the sidecar: %q", c.walDir)
			}
		})
	}
}

// ArchiveInUse sends the sidecar structured fields, never a path, and every
// call carries a deadline (the sidecar derives --io-timeout from it).
func TestArchiveInUse_SendsStructuredFieldsWithADeadline(t *testing.T) {
	fake := &fakeSidecar{}
	fake.reply = func(_ pb.Command, _ []string, stdout, _ io.Writer) int {
		fake.mu.Lock()
		last := fake.calls[len(fake.calls)-1]
		fake.mu.Unlock()
		switch {
		case last.archiveID == "":
			_, _ = io.WriteString(stdout, repoLsJSON("17-1:path", "archive.info:file"))
		case last.walDir == "":
			_, _ = io.WriteString(stdout, repoLsJSON("0000000100000000:path"))
		default:
			_, _ = io.WriteString(stdout, repoLsJSON(walSegments("0000000100000000", 1)...))
		}
		return 0
	}
	e := newSidecarTestEngine(t, fake)

	inUse, err := e.ArchiveInUse(t.Context())
	require.NoError(t, err)
	assert.False(t, inUse)

	require.Len(t, fake.calls, 3)
	want := []struct{ id, dir string }{{"", ""}, {"17-1", ""}, {"17-1", "0000000100000000"}}
	for i, c := range fake.calls {
		assert.Equal(t, pb.Command_COMMAND_REPO_LS, c.cmd)
		assert.Equal(t, []string{"--output=json", "--log-level-console=off"}, c.args, "no path among the options")
		assert.Equal(t, want[i].id, c.archiveID)
		assert.Equal(t, want[i].dir, c.walDir)
		assert.True(t, c.hasDeadline, "call %d has a deadline", i)
		require.NoError(t, sidecarAllows(c.cmd, c.args))
	}
}

func TestArchiveInUse_ErrorsWithoutBackupConfig(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "")
	_, err := e.ArchiveInUse(t.Context())
	require.Error(t, err)
}
