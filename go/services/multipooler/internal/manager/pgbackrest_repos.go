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
	"errors"
	"fmt"
	"strconv"
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

// createPgBackRestReposTable creates multigres.pgbackrest_repos and its
// revision counter, multigres.pgbackrest_repos_meta. The counter orders the
// three copies of the generation list (the table, the PVC copy, the
// sidecar's) so a stale one never wins; every writer of the table bumps it in
// the same transaction. IF NOT EXISTS so future lazy provisioning on
// pre-existing clusters (there is no sidecar schema upgrade mechanism) can
// reuse it on a live primary.
func (pm *MultipoolerManager) createPgBackRestReposTable(ctx context.Context) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for pgbackrest_repos provisioning")
	}
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := queryService.QueryAdminMultiStatement(execCtx, `CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos (
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
);
`+createPgBackRestReposMetaSQL+``); err != nil {
		return mterrors.Wrap(err, "failed to create pgbackrest_repos table")
	}
	return nil
}

// createPgBackRestReposMetaSQL is idempotent, so the version-skew catch-up
// (ensurePgBackRestReposMeta) reuses it on shards bootstrapped before the
// revision existed. Seeding at 1 is correct there too: the copy does not exist
// yet on such a shard, and a later bump only ever raises it.
const createPgBackRestReposMetaSQL = `CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos_meta (
	id INT PRIMARY KEY CHECK (id = 1),
	revision BIGINT NOT NULL
);
INSERT INTO multigres.pgbackrest_repos_meta (id, revision) VALUES (1, 1) ON CONFLICT (id) DO NOTHING`

// ensurePgBackRestReposMeta creates and seeds the revision table on a shard
// that predates it. Called from ensureSidecarSchemas, which runs on a primary
// at every open.
func (pm *MultipoolerManager) ensurePgBackRestReposMeta(ctx context.Context) error {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "internal query service unavailable for pgbackrest_repos_meta provisioning")
	}
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := queryService.QueryAdminMultiStatement(execCtx, createPgBackRestReposMetaSQL); err != nil {
		return mterrors.Wrap(err, "failed to create pgbackrest_repos_meta table")
	}
	return nil
}

// reconcileRevisionFloor raises the table's revision to max(table, copy,
// held)+1 and returns it. It never lowers the revision and never touches the
// repo list, which stays authoritative in the table.
//
// The revision orders writes only within one linear history. After a PITR or a
// restore from backup the table can be behind the PVC copy and behind the
// floor the sidecar holds, and both would then outrank the table forever.
// Task 6.1 calls this when the sidecar answers Configure with accepted=false
// (passing the sidecar's held_revision), and at start when the copy outranks
// the table. It must run on the primary; the caller then refreshes the copy
// and re-Configures with the returned revision.
func (pm *MultipoolerManager) reconcileRevisionFloor(ctx context.Context, held int64) (int64, error) {
	copyRev, _, _ := pm.readGenerationsCopy()
	res, err := pm.adminQueryArgs(ctx, `UPDATE multigres.pgbackrest_repos_meta
		SET revision = GREATEST(revision, $1::bigint, $2::bigint) + 1
		WHERE id = 1
		RETURNING revision`, copyRev, held)
	if err != nil {
		return 0, mterrors.Wrap(err, "failed to raise pgbackrest revision floor")
	}
	if len(res.Rows) != 1 || len(res.Rows[0].Values) != 1 {
		return 0, errors.New("multigres.pgbackrest_repos_meta has no revision row")
	}
	rev, err := strconv.ParseInt(string(res.Rows[0].Values[0]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid pgbackrest_repos_meta revision %q: %w", res.Rows[0].Values[0], err)
	}
	return rev, nil
}

// bumpPgBackRestReposRevisionSQL advances the generation-list revision. It
// must run in the same transaction as the write it accounts for: every
// statement that inserts into, updates or deletes from
// multigres.pgbackrest_repos carries it (as a data-modifying CTE when the
// write is a single statement).
const bumpPgBackRestReposRevisionSQL = `UPDATE multigres.pgbackrest_repos_meta SET revision = revision + 1 WHERE id = 1`

// readGenerations reads the revision and every repo row in one statement, so
// both come from the same snapshot.
func (pm *MultipoolerManager) readGenerations(ctx context.Context) (revision int64, repos []RepoGeneration, err error) {
	res, err := pm.adminQuery(ctx, `SELECT m.revision, r.generation, r.repo_number
		FROM multigres.pgbackrest_repos_meta m
		LEFT JOIN multigres.pgbackrest_repos r ON true
		WHERE m.id = 1
		ORDER BY r.generation`)
	if err != nil {
		return 0, nil, mterrors.Wrap(err, "failed to read pgbackrest generations")
	}
	if len(res.Rows) == 0 {
		return 0, nil, errors.New("multigres.pgbackrest_repos_meta has no revision row")
	}
	for i, row := range res.Rows {
		if len(row.Values) != 3 {
			return 0, nil, fmt.Errorf("pgbackrest generations row has %d columns, want 3", len(row.Values))
		}
		rev, err := strconv.ParseInt(string(row.Values[0]), 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid pgbackrest_repos_meta revision %q: %w", row.Values[0], err)
		}
		if i == 0 {
			revision = rev
		}
		// The LEFT JOIN yields one all-NULL row when there are no repos.
		if row.Values[1].IsNull() {
			continue
		}
		gen, err := strconv.ParseUint(string(row.Values[1]), 10, 32)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid pgbackrest_repos generation %q: %w", row.Values[1], err)
		}
		num, err := strconv.ParseUint(string(row.Values[2]), 10, 32)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid pgbackrest_repos repo_number %q: %w", row.Values[2], err)
		}
		repos = append(repos, RepoGeneration{Generation: uint32(gen), RepoNumber: uint32(num)})
	}
	return revision, repos, nil
}

// insertInitialPgBackRestRepo seeds the conventional generation-1 row during
// shard bootstrap, before the first backup, so the row is inside every backup
// and standbys inherit it via restore. The row is the same value the
// startup-rendered pgbackrest.conf was generated from
// (backup.InitialPgBackRestRepo). Bootstrap is all-or-nothing — any failure
// tears the database down and starts over — so this is a plain INSERT into a
// freshly created, empty table with no conflict handling needed.
func (pm *MultipoolerManager) insertInitialPgBackRestRepo(ctx context.Context) error {
	repo, err := pm.initialRepo(ctx)
	if err != nil {
		return err
	}
	pm.logger.InfoContext(ctx, "seeding pgbackrest_repos",
		"generation", repo.Generation, "repo_number", repo.RepoNumber, "key_fingerprint", repo.KeyFingerprint, "encrypted", repo.Encrypted)
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err = pm.adminExecArgs(execCtx, `WITH seeded AS (
		INSERT INTO multigres.pgbackrest_repos (generation, repo_number, encrypted, key_fingerprint, state, authoritative)
		VALUES ($1, $2, $3, $4, $5, TRUE)
		RETURNING generation
	) `+bumpPgBackRestReposRevisionSQL,
		repo.Generation, repo.RepoNumber, repo.Encrypted, repo.KeyFingerprint, repo.State)
	if err != nil {
		return mterrors.Wrap(err, "failed to seed pgbackrest_repos")
	}
	return nil
}
