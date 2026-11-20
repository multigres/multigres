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
	"encoding/json"
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// InfoStanza is one stanza of `pgbackrest info --output=json`.
type InfoStanza struct {
	Name   string           `json:"name"`
	Backup []InfoBackup     `json:"backup"`
	Repo   []InfoRepoStatus `json:"repo"`
}

// InfoRepoStatus is the per-repository status pgbackrest reports for a stanza.
// `info` exits 0 and reports its best-effort view even when a repository cannot
// be evaluated at all (e.g. a cipher-key mismatch): that failure only shows up
// here, never as a nonzero exit code.
type InfoRepoStatus struct {
	Key    int           `json:"key"`
	Status InfoRepoState `json:"status"`
}

// InfoRepoState is the status code/message pgbackrest assigns to a repository.
// Per `pgbackrest help info`: known conditions get a specific code (ok, missing
// stanza path, no valid backups); anything else pgbackrest can't classify is
// reported as code 99 ("other") with the full error detail in Message.
type InfoRepoState struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Known-benign pgbackrest repo status codes: the repository was evaluated
// successfully and either has usable backup data or is simply not bootstrapped
// yet. Any other code (including the 99 "other" catch-all) means pgbackrest
// could not properly evaluate the repository and must not be read as "no
// backups".
//
// Source of truth for these codes (INFO_STANZA_STATUS_CODE_*):
// https://github.com/pgbackrest/pgbackrest/blob/main/src/command/info/info.c
const (
	RepoStatusOK                = 0 // stanza/repo is healthy
	RepoStatusMissingStanzaPath = 1 // stanza never created (fresh repo)
	RepoStatusNoValidBackups    = 2 // stanza exists but has no backups yet
	RepoStatusMissingStanzaData = 3 // stanza path exists but backup.info is simply absent (not a decrypt failure)
)

// InfoBackup is a single backup in the info output.
type InfoBackup struct {
	Label      string            `json:"label"`
	Type       string            `json:"type"`
	Error      bool              `json:"error"`
	Timestamp  InfoTimestamp     `json:"timestamp"`
	Annotation map[string]string `json:"annotation,omitempty"`
	LSN        *InfoLSN          `json:"lsn,omitempty"`
	Info       *InfoSizeInfo     `json:"info,omitempty"`
}

// InfoSizeInfo is the size information for a backup.
type InfoSizeInfo struct {
	Size       uint64              `json:"size"`       // Original database size in bytes
	Repository *InfoRepositorySize `json:"repository"` // Compressed size in repository
}

// InfoRepositorySize is the repository size of a backup.
type InfoRepositorySize struct {
	Size uint64 `json:"size"` // Compressed size in repository in bytes
}

// InfoTimestamp is a backup's start and stop times.
type InfoTimestamp struct {
	Start int64 `json:"start"`
	Stop  int64 `json:"stop"`
}

// InfoLSN is a backup's LSN start/stop positions.
type InfoLSN struct {
	Start string `json:"start"` // e.g., "0/21000028"
	Stop  string `json:"stop"`  // e.g., "0/21000100"
}

// DecodeInfo unmarshals `pgbackrest info` JSON output. This can fail even
// though pgbackrest exited 0: when a repo can't be decrypted (e.g. a
// cipher-key mismatch), some of pgbackrest's own error messages splice a raw,
// unescaped snippet of the repo's undecrypted bytes directly into the JSON
// message string. Those bytes can contain characters (control bytes, invalid
// UTF-8) that are illegal inside a JSON string, which breaks decoding of the
// whole document. Callers must treat that as a real error; see RepoStatusError
// for the same underlying failure when the JSON happens to decode fine instead.
func DecodeInfo(output string) ([]InfoStanza, error) {
	var infoData []InfoStanza
	if err := json.Unmarshal([]byte(output), &infoData); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to parse pgbackrest info JSON: %v", err))
	}
	return infoData, nil
}

// RepoStatusError inspects the per-repository status pgbackrest reports and
// returns an error if any repository could not be evaluated. `pgbackrest info`
// always exits 0 and always attaches the correct status code (e.g. 99 "other"
// for a cipher-key mismatch); DecodeInfo covers the case where that status
// arrives in JSON that fails to decode. When it decodes fine, "backup" is simply
// empty, so backup:[] alone must not be read as "no backups yet": check the
// status code too, or a caller could bootstrap a fresh stanza over a repo it
// cannot actually read.
func RepoStatusError(infoData []InfoStanza) error {
	for _, stanza := range infoData {
		for _, repo := range stanza.Repo {
			switch repo.Status.Code {
			case RepoStatusOK, RepoStatusMissingStanzaPath, RepoStatusNoValidBackups, RepoStatusMissingStanzaData:
				continue
			default:
				return mterrors.New(mtrpcpb.Code_INTERNAL,
					fmt.Sprintf("pgbackrest repo %d status: %s", repo.Key, repo.Status.Message))
			}
		}
	}
	return nil
}

// StanzaExists is false only when every repository reports the stanza as never
// created; anything unclear counts as present.
func StanzaExists(infoData []InfoStanza) bool {
	if len(infoData) == 0 || len(infoData[0].Repo) == 0 {
		return true
	}
	for _, repo := range infoData[0].Repo {
		if repo.Status.Code != RepoStatusMissingStanzaPath {
			return true
		}
	}
	return false
}
