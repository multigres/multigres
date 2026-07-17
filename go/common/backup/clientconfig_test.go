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
	"os"
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
		Pg1User:       "admin",
	}

	configPath, err := WriteClientConfig(opts, backupCfg, unencryptedRepos(), nil)
	require.NoError(t, err)

	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "pgbackrest.conf"), configPath)

	cfg, err := ini.Load(configPath)
	require.NoError(t, err)

	global := cfg.Section("global")
	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "log"), global.Key("log-path").String())
	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "spool"), global.Key("spool-path").String())
	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "lock"), global.Key("lock-path").String())
	assert.Equal(t, "zst", global.Key("compress-type").String())

	// Retention settings must appear in [global] for all backend types
	assert.Equal(t, "7", global.Key("repo1-retention-full").String())
	assert.Equal(t, "1", global.Key("repo1-retention-diff").String())
	assert.Equal(t, "count", global.Key("repo1-retention-full-type").String())
	assert.Equal(t, "0", global.Key("repo1-retention-history").String())

	stanza := cfg.Section("multigres")
	assert.Equal(t, "posix", stanza.Key("repo1-type").String())
	assert.Equal(t, "/backups", stanza.Key("repo1-path").String())
	assert.Equal(t, "/tmp/socket", stanza.Key("pg1-socket-path").String())
	assert.Equal(t, "5432", stanza.Key("pg1-port").String())
	assert.Equal(t, "/var/lib/postgresql/data", stanza.Key("pg1-path").String())
	assert.Equal(t, "admin", stanza.Key("pg1-user").String())

	// Must NOT contain TLS server settings
	for _, key := range global.Keys() {
		assert.False(t, strings.HasPrefix(key.Name(), "tls-server-"), "client config should not contain TLS server settings")
	}

	// process-max values derived from runtime.NumCPU(). With the test's CPU
	// count we don't pin a specific number, but the four expected sections
	// must exist and each must carry a process-max line.
	content, err := os.ReadFile(configPath)
	require.NoError(t, err)
	assert.Regexp(t, `(?ms)^\[global\][^[]*?^process-max=\d+`, string(content),
		"[global] section must contain process-max")
	assert.Regexp(t, `(?m)^\[global:archive-get\]\s*\nprocess-max=\d+`, string(content),
		"[global:archive-get] section must contain process-max")
	assert.Regexp(t, `(?m)^\[global:archive-push\]\s*\nprocess-max=\d+`, string(content),
		"[global:archive-push] section must contain process-max")
	assert.Regexp(t, `(?m)^\[multigres:backup\]\s*\nprocess-max=\d+`, string(content),
		"[multigres:backup] section must contain process-max")
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
		Pg1User:       "admin",
	}

	configPath, err := WriteClientConfig(opts, backupCfg, unencryptedRepos(), nil)
	require.NoError(t, err)

	cfg, err := ini.Load(configPath)
	require.NoError(t, err)

	stanza := cfg.Section("multigres")
	assert.Equal(t, "s3", stanza.Key("repo1-type").String())
	assert.Equal(t, "test-bucket", stanza.Key("repo1-s3-bucket").String())
	assert.Equal(t, "us-west-2", stanza.Key("repo1-s3-region").String())
	assert.Equal(t, "admin", stanza.Key("pg1-user").String())
}

// unencryptedRepos returns the conventional single unencrypted repository set.
func unencryptedRepos() []PgBackRestRepo {
	return []PgBackRestRepo{InitialPgBackRestRepo(nil)}
}

func newFilesystemCfg(t *testing.T) *Config {
	t.Helper()
	cfg, err := NewConfig(&clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{Path: "/backups"},
		},
	})
	require.NoError(t, err)
	return cfg
}

func TestWriteClientConfig_Cipher(t *testing.T) {
	tests := []struct {
		name string
		keys CipherKeys
	}{
		{
			name: "encrypted repo renders repo cipher settings",
			keys: CipherKeys{1: "super-secret-passphrase"},
		},
		{
			name: "unencrypted repo renders no cipher settings",
			keys: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			opts := ClientConfigOpts{
				PoolerDir:     tmpDir,
				Pg1Port:       5432,
				Pg1SocketPath: "/tmp/socket",
				Pg1Path:       "/data",
				Pg1User:       "admin",
			}
			repos := []PgBackRestRepo{InitialPgBackRestRepo(tt.keys)}

			configPath, err := WriteClientConfig(opts, newFilesystemCfg(t), repos, tt.keys)
			require.NoError(t, err)

			cfg, err := ini.Load(configPath)
			require.NoError(t, err)
			stanza := cfg.Section("multigres")
			if len(tt.keys) > 0 {
				assert.Equal(t, CipherType, stanza.Key("repo1-cipher-type").String())
				assert.Equal(t, tt.keys[1], stanza.Key("repo1-cipher-pass").String(),
					"rendered cipher pass must be the mounted key for generation 1")
			} else {
				assert.False(t, stanza.HasKey("repo1-cipher-type"))
				assert.False(t, stanza.HasKey("repo1-cipher-pass"))
			}

			info, err := os.Stat(configPath)
			require.NoError(t, err)
			assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(),
				"the conf can carry secrets and is always written 0600")
		})
	}

	t.Run("encrypted repo with missing or mismatched key is an error", func(t *testing.T) {
		opts := ClientConfigOpts{PoolerDir: t.TempDir(), Pg1Port: 5432, Pg1SocketPath: "/tmp/socket", Pg1Path: "/data", Pg1User: "admin"}
		// This will generate the passphrase fingerprint, repos don't contain the actual passphrase.
		repos := []PgBackRestRepo{InitialPgBackRestRepo(CipherKeys{1: "the-real-passphrase"})}

		_, err := WriteClientConfig(opts, newFilesystemCfg(t), repos, nil /* keys */)
		require.ErrorContains(t, err, "no cipher key")

		// The key file declares the generation unencrypted (empty passphrase)
		// while the repo row says it is encrypted — a drift between the mounted
		// keys and the table that must fail, never silently render unencrypted.
		_, err = WriteClientConfig(opts, newFilesystemCfg(t), repos, CipherKeys{1: ""})
		require.ErrorContains(t, err, "no cipher key")

		// Catches keys loaded from disk that don't match what the repo table
		// recorded: the guardrail keeping the cipher key mapping in line with
		// multigres.pgbackrest_repos, so a mislabeled key fails here instead
		// of encrypting the repo with a passphrase nobody recorded.
		_, err = WriteClientConfig(opts, newFilesystemCfg(t), repos, CipherKeys{1: "a-different-passphrase"})
		require.ErrorContains(t, err, "does not match the repository's key fingerprint")
	})
}

func TestWriteClientConfig_MultiRepo(t *testing.T) {
	keys := CipherKeys{1: "gen-one-passphrase", 2: "gen-two-passphrase"}
	repos := []PgBackRestRepo{
		{Generation: 1, RepoNumber: 1, Encrypted: true, KeyFingerprint: CipherKeyFingerprint(keys[1]), State: PgBackRestRepoStateActive, Authoritative: true},
		{Generation: 2, RepoNumber: 2, Encrypted: true, KeyFingerprint: CipherKeyFingerprint(keys[2]), State: "staged"},
	}
	opts := ClientConfigOpts{
		PoolerDir:     t.TempDir(),
		Pg1Port:       5432,
		Pg1SocketPath: "/tmp/socket",
		Pg1Path:       "/data",
		Pg1User:       "admin",
	}

	configPath, err := WriteClientConfig(opts, newFilesystemCfg(t), repos, keys)
	require.NoError(t, err)

	cfg, err := ini.Load(configPath)
	require.NoError(t, err)
	stanza := cfg.Section("multigres")
	global := cfg.Section("global")

	// Authoritative generation renders as repo1 at the base path; the second
	// generation renders as repo2 under its own gen-<N> component.
	assert.Equal(t, "/backups", stanza.Key("repo1-path").String())
	assert.Equal(t, keys[1], stanza.Key("repo1-cipher-pass").String())
	assert.Equal(t, "/backups/gen-2", stanza.Key("repo2-path").String())
	assert.Equal(t, "posix", stanza.Key("repo2-type").String())
	assert.Equal(t, keys[2], stanza.Key("repo2-cipher-pass").String())
	assert.Equal(t, CipherType, stanza.Key("repo2-cipher-type").String())

	// Per-repo retention, and asynchronous archiving once >1 repo is rendered.
	assert.Equal(t, RetentionFull, global.Key("repo2-retention-full").String())
	assert.Equal(t, "y", global.Key("archive-async").String())

	info, err := os.Stat(configPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	t.Run("repo numbers are explicit table state, not derived from generation", func(t *testing.T) {
		flipped := []PgBackRestRepo{
			{Generation: 1, RepoNumber: 2, Encrypted: true, KeyFingerprint: CipherKeyFingerprint(keys[1]), State: "retiring"},
			{Generation: 2, RepoNumber: 1, Encrypted: true, KeyFingerprint: CipherKeyFingerprint(keys[2]), State: PgBackRestRepoStateActive, Authoritative: true},
		}
		configPath, err := WriteClientConfig(opts, newFilesystemCfg(t), flipped, keys)
		require.NoError(t, err)
		cfg, err := ini.Load(configPath)
		require.NoError(t, err)
		stanza := cfg.Section("multigres")

		// gen-2 is now authoritative: it renders as repo1 with its own path
		// AND its own cipher key (resolved by generation, not repo number) —
		// gen-1 moves to repo2 with its key. Path and cipher must move together.
		assert.Equal(t, "/backups/gen-2", stanza.Key("repo1-path").String())
		assert.Equal(t, keys[2], stanza.Key("repo1-cipher-pass").String())
		assert.Equal(t, "/backups", stanza.Key("repo2-path").String())
		assert.Equal(t, keys[1], stanza.Key("repo2-cipher-pass").String())
	})

	t.Run("invalid repository sets are rejected", func(t *testing.T) {
		_, err := WriteClientConfig(opts, newFilesystemCfg(t), []PgBackRestRepo{}, nil)
		require.ErrorContains(t, err, "no pgbackrest repositories")

		authoritativeNotRepoOne := []PgBackRestRepo{
			{Generation: 1, RepoNumber: 1, State: PgBackRestRepoStateActive, Authoritative: true},
			{Generation: 2, RepoNumber: 2, State: PgBackRestRepoStateActive, Authoritative: true},
		}
		_, err = WriteClientConfig(opts, newFilesystemCfg(t), authoritativeNotRepoOne, nil)
		require.ErrorContains(t, err, "the authoritative repository is always repo 1")

		duplicateNumbers := []PgBackRestRepo{
			{Generation: 1, RepoNumber: 1, State: PgBackRestRepoStateActive, Authoritative: true},
			{Generation: 2, RepoNumber: 1, State: "staged", Authoritative: true},
		}
		_, err = WriteClientConfig(opts, newFilesystemCfg(t), duplicateNumbers, nil)
		require.ErrorContains(t, err, "duplicate repo number")

		nonContiguous := []PgBackRestRepo{
			{Generation: 1, RepoNumber: 1, State: PgBackRestRepoStateActive, Authoritative: true},
			{Generation: 2, RepoNumber: 3, State: "staged"},
		}
		_, err = WriteClientConfig(opts, newFilesystemCfg(t), nonContiguous, nil)
		require.ErrorContains(t, err, "outside the contiguous range")

		incoherent := []PgBackRestRepo{{Generation: 1, RepoNumber: 1, Encrypted: true, State: PgBackRestRepoStateActive, Authoritative: true}}
		_, err = WriteClientConfig(opts, newFilesystemCfg(t), incoherent, nil)
		require.ErrorContains(t, err, "inconsistent with key fingerprint")
	})
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
		Pg1User:       "admin",
	}

	_, err = WriteClientConfig(opts, backupCfg, unencryptedRepos(), nil)
	require.NoError(t, err)

	assert.DirExists(t, filepath.Join(tmpDir, "pgbackrest", "log"))
	assert.DirExists(t, filepath.Join(tmpDir, "pgbackrest", "spool"))
	assert.DirExists(t, filepath.Join(tmpDir, "pgbackrest", "lock"))
}
