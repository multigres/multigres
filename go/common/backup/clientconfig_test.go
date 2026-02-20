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
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/ini.v1"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestWriteClientConfig_Filesystem(t *testing.T) {
	tmpDir := t.TempDir()

	backupLoc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{
				Path: "/backups",
			},
		},
	}
	backupCfg, err := NewConfig(backupLoc)
	require.NoError(t, err)

	opts := ClientConfigOpts{
		PoolerDir:     tmpDir,
		Pg1Port:       5432,
		Pg1SocketPath: "/tmp/socket",
		Pg1Path:       "/var/lib/postgresql/data",
	}

	configPath, err := WriteClientConfig(opts, backupCfg)
	require.NoError(t, err)

	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "pgbackrest.conf"), configPath)

	cfg, err := ini.Load(configPath)
	require.NoError(t, err)

	global := cfg.Section("global")
	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "log"), global.Key("log-path").String())
	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "spool"), global.Key("spool-path").String())
	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "lock"), global.Key("lock-path").String())
	assert.Equal(t, "zst", global.Key("compress-type").String())

	stanza := cfg.Section("multigres")
	assert.Equal(t, "posix", stanza.Key("repo1-type").String())
	assert.Equal(t, "/backups", stanza.Key("repo1-path").String())
	assert.Equal(t, "/tmp/socket", stanza.Key("pg1-socket-path").String())
	assert.Equal(t, "5432", stanza.Key("pg1-port").String())
	assert.Equal(t, "/var/lib/postgresql/data", stanza.Key("pg1-path").String())

	// Must NOT contain TLS server settings
	for _, key := range global.Keys() {
		assert.False(t, strings.HasPrefix(key.Name(), "tls-server-"), "client config should not contain TLS server settings")
	}
}

func TestWriteClientConfig_S3(t *testing.T) {
	tmpDir := t.TempDir()

	backupLoc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket: "test-bucket",
				Region: "us-west-2",
			},
		},
	}
	backupCfg, err := NewConfig(backupLoc)
	require.NoError(t, err)

	opts := ClientConfigOpts{
		PoolerDir:     tmpDir,
		Pg1Port:       5432,
		Pg1SocketPath: "/tmp/socket",
		Pg1Path:       "/data",
	}

	configPath, err := WriteClientConfig(opts, backupCfg)
	require.NoError(t, err)

	cfg, err := ini.Load(configPath)
	require.NoError(t, err)

	stanza := cfg.Section("multigres")
	assert.Equal(t, "s3", stanza.Key("repo1-type").String())
	assert.Equal(t, "test-bucket", stanza.Key("repo1-s3-bucket").String())
	assert.Equal(t, "us-west-2", stanza.Key("repo1-s3-region").String())
}

func TestWriteClientConfig_CreatesDirs(t *testing.T) {
	tmpDir := t.TempDir()

	backupLoc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{Path: "/backups"},
		},
	}
	backupCfg, err := NewConfig(backupLoc)
	require.NoError(t, err)

	opts := ClientConfigOpts{
		PoolerDir:     tmpDir,
		Pg1Port:       5432,
		Pg1SocketPath: "/tmp/socket",
		Pg1Path:       "/data",
	}

	_, err = WriteClientConfig(opts, backupCfg)
	require.NoError(t, err)

	assert.DirExists(t, filepath.Join(tmpDir, "pgbackrest", "log"))
	assert.DirExists(t, filepath.Join(tmpDir, "pgbackrest", "spool"))
	assert.DirExists(t, filepath.Join(tmpDir, "pgbackrest", "lock"))
}
