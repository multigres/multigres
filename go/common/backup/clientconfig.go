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
	"bytes"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"text/template"

	"github.com/multigres/multigres/config"
	"github.com/multigres/multigres/go/tools/fileutil"
)

// ClientConfigOpts holds options for generating pgbackrest.conf for client operations.
type ClientConfigOpts struct {
	PoolerDir     string // Base directory for pooler data
	Pg1Port       int    // Local PostgreSQL port
	Pg1SocketPath string // Local PostgreSQL socket directory
	Pg1Path       string // Local PostgreSQL data directory
	Pg1User       string // PostgreSQL superuser for pgbackrest connections
}

// WriteClientConfig generates pgbackrest.conf for client operations (backup,
// restore, info) from the given repository set and mounted cipher keys. Each
// repository renders under its explicit RepoNumber (the authoritative repo is
// always repo1), and every encrypted repo must resolve to a mounted key
// matching its fingerprint. Returns the path to the generated config file.
func WriteClientConfig(opts ClientConfigOpts, backupCfg *Config, repos []PgBackRestRepo, keys CipherKeys) (string, error) {
	if err := validateRepos(repos); err != nil {
		return "", fmt.Errorf("invalid pgbackrest repository set: %w", err)
	}

	pgbackrestDir := filepath.Join(opts.PoolerDir, "pgbackrest")
	logPath := filepath.Join(pgbackrestDir, "log")
	spoolPath := filepath.Join(pgbackrestDir, "spool")
	lockPath := filepath.Join(pgbackrestDir, "lock")

	// Create directories
	for _, dir := range []string{pgbackrestDir, logPath, spoolPath, lockPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Parse the template
	tmpl, err := template.New("pgbackrest").Parse(config.PgBackRestConfigTmpl)
	if err != nil {
		return "", fmt.Errorf("failed to parse pgbackrest config template: %w", err)
	}

	// Render every repository under its explicit pgbackrest repo number.
	ordered := orderReposForRendering(repos)
	repoConfig := map[string]string{}
	repoCredentials := map[string]string{}
	retentionConfig := map[string]string{}
	for _, repo := range ordered {
		index := int(repo.RepoNumber)

		storage, err := backupCfg.PgBackRestConfig(index, repo.Generation, "multigres")
		if err != nil {
			return "", fmt.Errorf("failed to generate pgBackRest config for generation %d: %w", repo.Generation, err)
		}
		maps.Copy(repoConfig, storage)

		// The cipher is fixed at stanza-create time: it must be in the conf
		// before the first stanza-create and can never change for this repo
		// afterwards.
		cipherPass, err := resolveRepoCipher(repo, keys)
		if err != nil {
			return "", err
		}
		if cipherPass != "" {
			prefix := fmt.Sprintf("repo%d-", index)
			repoConfig[prefix+"cipher-type"] = CipherType
			repoConfig[prefix+"cipher-pass"] = cipherPass
		}

		creds, err := backupCfg.PgBackRestCredentials(index)
		if err != nil {
			return "", fmt.Errorf("failed to get pgBackRest credentials: %w", err)
		}
		maps.Copy(repoCredentials, creds)

		maps.Copy(retentionConfig, repoRetentionConfig(index))
	}

	// Prepare template data
	templateData := struct {
		LogPath   string
		SpoolPath string
		LockPath  string

		// With multiple repositories archive-push must be asynchronous: a
		// down repo would otherwise hold every healthy repo one WAL segment
		// behind (pgbackrest pushes to all repos per invocation).
		ArchiveAsync bool

		RetentionConfig map[string]string
		RepoConfig      map[string]string
		RepoCredentials map[string]string

		Pg1SocketPath string
		Pg1Port       int
		Pg1Path       string
		Pg1User       string

		ProcessLimits ProcessLimits
	}{
		LogPath:   logPath,
		SpoolPath: spoolPath,
		LockPath:  lockPath,

		ArchiveAsync: len(ordered) > 1,

		RetentionConfig: retentionConfig,
		RepoConfig:      repoConfig,
		RepoCredentials: repoCredentials,

		Pg1SocketPath: opts.Pg1SocketPath,
		Pg1Port:       opts.Pg1Port,
		Pg1Path:       opts.Pg1Path,
		Pg1User:       opts.Pg1User,

		ProcessLimits: CurrentProcessLimits(),
	}

	// Execute template
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateData); err != nil {
		return "", fmt.Errorf("failed to execute pgbackrest config template: %w", err)
	}

	// The conf can carry secrets (S3 credentials, cipher passphrases) and is
	// only read by pgbackrest processes running as the service user — always
	// 0600, matching refresh-credentials. Atomic write-then-rename so the
	// mode applies to a pre-existing file too and a concurrent
	// postgres-spawned archive-push never reads a torn conf.
	configPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	if err := fileutil.AtomicWriteFile(configPath, buf.Bytes(), 0o600); err != nil {
		return "", fmt.Errorf("failed to write pgbackrest.conf: %w", err)
	}

	return configPath, nil
}
