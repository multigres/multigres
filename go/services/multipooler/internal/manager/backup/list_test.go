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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestListBackups(t *testing.T) {
	// Two backups; the second has a mismatched shard and must be filtered out.
	json := `[{"backup":[
		{"label":"20250104-100000F","type":"full","annotation":{"table_group":"tg1","shard":"0","job_id":"j1","multipooler_id":"mp1","pooler_type":"PRIMARY"}},
		{"label":"20250104-110000F","type":"incr","annotation":{"table_group":"tg1","shard":"9","job_id":"j2"}}
	]}]`
	stubPgbackrest(t, pgbackrestInfoStub(json))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	backups, err := e.ListBackups(context.Background())
	require.NoError(t, err)
	require.Len(t, backups, 1, "the mismatched-shard backup should be filtered out")
	assert.Equal(t, "20250104-100000F", backups[0].BackupId)
	assert.Equal(t, "j1", backups[0].JobId)
	assert.Equal(t, "full", backups[0].Type)
	assert.Equal(t, "mp1", backups[0].MultipoolerId)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, backups[0].PoolerType)
	assert.Equal(t, multipoolermanagerdata.BackupMetadata_COMPLETE, backups[0].Status)
}

func TestList_AppliesLimit(t *testing.T) {
	json := `[{"backup":[
		{"label":"20250104-100000F","type":"full","annotation":{"table_group":"tg1","shard":"0","job_id":"j1"}},
		{"label":"20250104-110000F","type":"incr","annotation":{"table_group":"tg1","shard":"0","job_id":"j2"}}
	]}]`
	stubPgbackrest(t, pgbackrestInfoStub(json))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	all, err := e.List(context.Background(), 0)
	require.NoError(t, err)
	require.Len(t, all, 2)

	limited, err := e.List(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, limited, 1)
}

func TestListBackups_EmptyWhenStanzaMissing(t *testing.T) {
	stubPgbackrest(t, "#!/bin/bash\necho \"ERROR: [055]: unable to load info file: stanza 'multigres' does not exist\" >&2\nexit 1\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	backups, err := e.ListBackups(context.Background())
	require.NoError(t, err)
	assert.Empty(t, backups)
}

func TestListBackups_ErrorWhenConfigPathMissing(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "/tmp/backups")
	_, err := e.ListBackups(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgbackrest config not found")
}

func TestListBackups_ErrorWhenBackupConfigMissing(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "") // no backup location → no repo config
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	_, err := e.ListBackups(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backup config not loaded")
}
