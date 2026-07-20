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

package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// ============================================================================
// Backup repository metadata (multigres.pgbackrest_repos)
//
// Consensus-replicated record of the shard's pgBackRest repository
// generations, written at bootstrap and read by repo lifecycle operations
// (the key-rotation flow), which also own the backfill for clusters that
// predate the table.
// ============================================================================

// requireInitialRepoEncryptionError returns the error to surface when the
// backup location requires encryption but keys hold no usable generation-1
// cipher key, and nil when the requirement is satisfied. Checked at startup
// (before the conf renders) and again at bootstrap (before stanza-create
// fixes the cipher forever).
func requireInitialRepoEncryptionError(loc *clustermetadatapb.BackupLocation, keys backup.CipherKeys) error {
	if !loc.GetRequireInitialRepoEncryption() {
		return nil
	}
	pass, declared := keys[backup.InitialRepoGeneration]
	if pass != "" {
		return nil
	}
	if declared {
		return fmt.Errorf("backup encryption is required but generation %d is declared unencrypted in the cipher key file", backup.InitialRepoGeneration)
	}
	return fmt.Errorf("backup encryption is required but no cipher key for generation %d is configured; set --pgbackrest-cipher-key-file", backup.InitialRepoGeneration)
}

// createPgBackRestReposTable creates multigres.pgbackrest_repos. IF NOT
// EXISTS so future lazy provisioning on pre-existing clusters (there is no
// sidecar schema upgrade mechanism) can reuse it on a live primary.
func (pm *MultipoolerManager) createPgBackRestReposTable(ctx context.Context) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for pgbackrest_repos provisioning")
	}
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := queryService.QueryMultiStatement(execCtx, `CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos (
	generation BIGINT PRIMARY KEY,
	repo_number BIGINT NOT NULL UNIQUE CHECK (repo_number >= 1),
	encrypted BOOLEAN NOT NULL,
	key_fingerprint TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL CHECK (state IN ('staged', 'seeded', 'active', 'retiring')),
	authoritative BOOLEAN NOT NULL DEFAULT FALSE,
	version BIGINT NOT NULL DEFAULT 1,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CHECK (encrypted = (key_fingerprint <> '')),
	CHECK (authoritative = (repo_number = 1))
)`); err != nil {
		return mterrors.Wrap(err, "failed to create pgbackrest_repos table")
	}
	return nil
}

// insertInitialPgBackRestRepo seeds the conventional generation-1 row during
// shard bootstrap, before the first backup, so the row is inside every backup
// and standbys inherit it via restore. The row is the same value the
// startup-rendered pgbackrest.conf was generated from
// (backup.InitialPgBackRestRepo). Bootstrap is all-or-nothing — any failure
// tears the database down and starts over — so this is a plain INSERT into a
// freshly created, empty table with no conflict handling needed.
func (pm *MultipoolerManager) insertInitialPgBackRestRepo(ctx context.Context) error {
	repo := backup.InitialPgBackRestRepo(pm.config.BackupCipherKeys)
	pm.logger.InfoContext(ctx, "Seeding pgbackrest_repos",
		"generation", repo.Generation, "repo_number", repo.RepoNumber, "key_fingerprint", repo.KeyFingerprint, "encrypted", repo.Encrypted)
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err := pm.execArgs(execCtx, `INSERT INTO multigres.pgbackrest_repos (generation, repo_number, encrypted, key_fingerprint, state, authoritative)
		VALUES ($1, $2, $3, $4, $5, TRUE)`,
		repo.Generation, repo.RepoNumber, repo.Encrypted, repo.KeyFingerprint, repo.State)
	if err != nil {
		return mterrors.Wrap(err, "failed to seed pgbackrest_repos")
	}
	return nil
}
