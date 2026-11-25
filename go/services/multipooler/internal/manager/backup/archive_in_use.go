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
// directly. Any listing failure is an error and never counts as unused.
func (e *Engine) ArchiveInUse(ctx context.Context) (bool, error) {
	archivePath := "archive/" + stanzaName

	entries, err := e.repoList(ctx, archivePath)
	if err != nil {
		return false, err
	}
	archiveIDs := entries.names("path")
	if len(archiveIDs) != 1 {
		return len(archiveIDs) > 1, nil
	}

	idPath := archivePath + "/" + archiveIDs[0]
	entries, err = e.repoList(ctx, idPath)
	if err != nil {
		return false, err
	}
	if slices.ContainsFunc(entries.names("file"), walHistoryName.MatchString) {
		return true, nil
	}
	walDirs := entries.names("path")
	if len(walDirs) != 1 {
		return len(walDirs) > 1, nil
	}
	if !strings.HasPrefix(walDirs[0], firstTimeline) {
		return true, nil
	}

	entries, err = e.repoList(ctx, idPath+"/"+walDirs[0])
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

// repoList runs `pgbackrest repo-ls` on one repository directory. A directory
// that does not exist lists as empty.
func (e *Engine) repoList(ctx context.Context, path string) (repoListing, error) {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return nil, mterrors.Wrap(err, "pgbackrest config not found")
	}

	queryCtx, cancel := context.WithTimeout(ctx, commonbackup.InfoTimeout)
	defer cancel()
	cmd := e.pgbackrestIOTimeoutCmd(queryCtx,
		"--stanza="+stanzaName,
		"--config="+configPath,
		"--output=json",
		"--log-level-console=off",
		"repo-ls", path)
	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest repo-ls %s failed: %v\nOutput: %s", path, err, output))
	}

	var raw map[string]struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(output), &raw); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to parse pgbackrest repo-ls %s output: %v", path, err))
	}
	listing := make(repoListing, len(raw))
	for name, entry := range raw {
		listing[name] = entry.Type
	}
	return listing, nil
}
