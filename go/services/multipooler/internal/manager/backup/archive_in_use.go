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
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// maxBootstrapArchiveSegments is the most WAL segments a first-backup bootstrap
// leaves in the archive: one from `check` and two more from the full backup.
const maxBootstrapArchiveSegments = 3

const firstTimeline = "00000001"

var (
	walSegmentName = regexp.MustCompile(`^[0-9A-F]{24}-[0-9a-f]{40}`)
	walHistoryName = regexp.MustCompile(`^[0-9A-F]{8}\.history$`)
)

// ArchiveInUse reports whether the stanza's archive holds more than a failed
// first-backup bootstrap leaves: several archive ids, a timeline switch, or more
// than maxBootstrapArchiveSegments segments. `pgbackrest info` cannot tell, as it
// reports no archive once backup.info is missing, so the archive is listed
// directly. Any listing failure is an error and never counts as unused. A
// directory whose name is not one pgBackRest writes (so one the sidecar would
// refuse to list) counts as in use: it is not something a failed bootstrap
// leaves, and an error here would stop the monitor classifying the repository at
// all.
func (e *Engine) ArchiveInUse(ctx context.Context) (bool, error) {
	entries, err := e.repoList(ctx, "", "")
	if err != nil {
		return false, err
	}
	archiveIDs := entries.names("path")
	if slices.ContainsFunc(archiveIDs, func(id string) bool { return !commonbackup.ValidArchiveID(id) }) {
		return true, nil
	}
	if len(archiveIDs) != 1 {
		return len(archiveIDs) > 1, nil
	}

	archiveID := archiveIDs[0]
	entries, err = e.repoList(ctx, archiveID, "")
	if err != nil {
		return false, err
	}
	if slices.ContainsFunc(entries.names("file"), walHistoryName.MatchString) {
		return true, nil
	}
	walDirs := entries.names("path")
	if slices.ContainsFunc(walDirs, func(dir string) bool { return !commonbackup.ValidWALDir(dir) }) {
		return true, nil
	}
	if len(walDirs) != 1 {
		return len(walDirs) > 1, nil
	}
	if !strings.HasPrefix(walDirs[0], firstTimeline) {
		return true, nil
	}

	entries, err = e.repoList(ctx, archiveID, walDirs[0])
	if err != nil {
		return false, err
	}
	segments := 0
	for _, name := range entries.names("file") {
		if walSegmentName.MatchString(name) {
			segments++
		}
	}
	return segments > maxBootstrapArchiveSegments, nil
}

// repoListing maps the entries of one repository directory to their type
// ("path" or "file").
type repoListing map[string]string

func (l repoListing) names(entryType string) []string {
	var names []string
	for name, t := range l {
		if name != "." && t == entryType {
			names = append(names, name)
		}
	}
	return names
}

// repoList lists one directory of the stanza's archive through the sidecar:
// the archive itself, an archive id, or a WAL directory of one. The sidecar
// builds the path from these fields, so no caller string reaches pgBackRest. A
// directory that does not exist lists as empty. The call carries a deadline, from
// which the sidecar derives pgBackRest's --io-timeout.
func (e *Engine) repoList(ctx context.Context, archiveID, walDir string) (repoListing, error) {
	if _, err := e.requireBackupConfig(); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup config not loaded from topology")
	}

	queryCtx, cancel := context.WithTimeout(ctx, commonbackup.InfoTimeout)
	defer cancel()
	cmd := &pgCmd{
		command:   pb.Command_COMMAND_REPO_LS,
		args:      []string{"--output=json", "--log-level-console=off"},
		archiveID: archiveID,
		walDir:    walDir,
	}
	where := "archive/" + stanzaName
	if archiveID != "" {
		where += "/" + archiveID
		if walDir != "" {
			where += "/" + walDir
		}
	}
	output, err := e.output(queryCtx, cmd)
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest repo-ls %s failed: %v\nOutput: %s", where, err, output))
	}

	var raw map[string]struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to parse pgbackrest repo-ls %s output: %v", where, err))
	}
	listing := make(repoListing, len(raw))
	for name, entry := range raw {
		listing[name] = entry.Type
	}
	return listing, nil
}
