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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStanzaExists(t *testing.T) {
	repos := func(codes ...int) []InfoStanza {
		statuses := make([]InfoRepoStatus, 0, len(codes))
		for i, c := range codes {
			statuses = append(statuses, InfoRepoStatus{Key: i + 1, Status: InfoRepoState{Code: c}})
		}
		return []InfoStanza{{Name: "multigres", Repo: statuses}}
	}
	tests := []struct {
		name string
		info []InfoStanza
		want bool
	}{
		{name: "no stanza entry", info: nil, want: true},
		{name: "entry without repositories", info: repos(), want: true},
		{name: "never created", info: repos(RepoStatusMissingStanzaPath), want: false},
		{name: "never created on every repository", info: repos(RepoStatusMissingStanzaPath, RepoStatusMissingStanzaPath), want: false},
		{name: "created on one of two repositories", info: repos(RepoStatusMissingStanzaPath, RepoStatusOK), want: true},
		{name: "stanza data missing", info: repos(RepoStatusMissingStanzaData), want: true},
		{name: "no valid backups", info: repos(RepoStatusNoValidBackups), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, StanzaExists(tt.info))
		})
	}
}

func TestDecodeInfo(t *testing.T) {
	_, err := DecodeInfo("not json")
	require.ErrorContains(t, err, "failed to parse pgbackrest info JSON")

	got, err := DecodeInfo(`[{"name":"multigres","backup":[{"label":"20261116-100000F","type":"full","error":false}],"repo":[{"key":1,"status":{"code":0,"message":"ok"}}]}]`)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "20261116-100000F", got[0].Backup[0].Label)
}

func TestRepoStatusError(t *testing.T) {
	stanza := func(codes ...int) []InfoStanza {
		var repos []InfoRepoStatus
		for i, c := range codes {
			repos = append(repos, InfoRepoStatus{Key: i + 1, Status: InfoRepoState{Code: c, Message: "detail"}})
		}
		return []InfoStanza{{Name: "multigres", Repo: repos}}
	}
	for _, code := range []int{RepoStatusOK, RepoStatusMissingStanzaPath, RepoStatusNoValidBackups, RepoStatusMissingStanzaData} {
		assert.NoError(t, RepoStatusError(stanza(code)), "code %d", code)
	}
	assert.NoError(t, RepoStatusError(nil))
	for _, code := range []int{4, 5, 99} {
		assert.ErrorContains(t, RepoStatusError(stanza(RepoStatusOK, code)), "pgbackrest repo 2 status: detail", "code %d", code)
	}
}
