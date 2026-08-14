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

// pgBackRestInfo represents the structure of pgbackrest info JSON output
type pgBackRestInfo struct {
	Name   string                 `json:"name"`
	Backup []pgBackRestBackup     `json:"backup"`
	Repo   []pgBackRestRepoStatus `json:"repo"`
}

// pgBackRestRepoStatus represents the per-repository status pgbackrest
// reports for a stanza. `info` always exits 0 and reports its best-effort
// view even when a repository cannot be evaluated at all (e.g. a cipher-key
// mismatch): that failure only shows up here, never as a nonzero exit code.
type pgBackRestRepoStatus struct {
	Key    int                 `json:"key"`
	Status pgBackRestRepoState `json:"status"`
}

// pgBackRestRepoState is the status code/message pgbackrest assigns to a
// repository. Per `pgbackrest help info`: known conditions get a specific
// code (ok, missing stanza path, no valid backups); anything else pgbackrest
// can't classify is reported as code 99 ("other") with the full error detail
// in Message.
type pgBackRestRepoState struct {
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
	pgBackRestRepoStatusOK                = 0 // stanza/repo is healthy
	pgBackRestRepoStatusMissingStanzaPath = 1 // stanza never created (fresh repo)
	pgBackRestRepoStatusNoValidBackups    = 2 // stanza exists but has no backups yet
	pgBackRestRepoStatusMissingStanzaData = 3 // stanza path exists but backup.info is simply absent (not a decrypt failure)
)

// pgBackRestBackup represents a single backup in the info output
type pgBackRestBackup struct {
	Label      string              `json:"label"`
	Type       string              `json:"type"`
	Error      bool                `json:"error"`
	Timestamp  pgBackRestTimestamp `json:"timestamp"`
	Annotation map[string]string   `json:"annotation,omitempty"`
	LSN        *pgBackRestLSN      `json:"lsn,omitempty"` // LSN range for this backup
	Info       *pgBackRestSizeInfo `json:"info,omitempty"`
}

// pgBackRestSizeInfo represents size information for a backup
type pgBackRestSizeInfo struct {
	Size       uint64                    `json:"size"`       // Original database size in bytes
	Repository *pgBackRestRepositorySize `json:"repository"` // Compressed size in repository
}

// pgBackRestRepositorySize represents repository size info
type pgBackRestRepositorySize struct {
	Size uint64 `json:"size"` // Compressed size in repository in bytes
}

// pgBackRestTimestamp represents backup timestamps
type pgBackRestTimestamp struct {
	Start int64 `json:"start"`
	Stop  int64 `json:"stop"`
}

// pgBackRestLSN represents LSN start/stop positions for a backup
type pgBackRestLSN struct {
	Start string `json:"start"` // e.g., "0/21000028"
	Stop  string `json:"stop"`  // e.g., "0/21000100"
}
