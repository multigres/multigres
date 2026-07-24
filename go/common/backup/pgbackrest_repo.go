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
	"errors"
	"fmt"
	"sort"
)

// PgBackRestRepoStateActive is the state of a repository that takes backups.
// The remaining lifecycle states (staged, seeded, retiring) belong to the
// repo-rotation flow and are only ever written by it.
const PgBackRestRepoStateActive = "active"

// PgBackRestRepo mirrors one row of the multigres.pgbackrest_repos sidecar
// table: a pgBackRest repository generation the shard knows about. The
// rendered pgbackrest.conf is a pure function of a set of these rows plus the
// mounted cipher keys — every repoN index in the conf is the row's explicit
// RepoNumber, never derived. The authoritative repo is always repo 1 (default
// backup target, preferred restore/archive-get source).
type PgBackRestRepo struct {
	Generation     int64
	RepoNumber     int64  // pgbackrest index this row renders as (repoN-*); authoritative iff 1
	Encrypted      bool   // repository must be encrypted; rendering fails without a matching key
	KeyFingerprint string // fingerprint of the cipher passphrase; empty iff unencrypted
	State          string
	Authoritative  bool
	Version        int64
}

// InitialPgBackRestRepo returns the conventional generation-1 repository
// every cluster gets by convention: encrypted when a generation-1 key is
// mounted, unencrypted otherwise. It is the single source of truth for both
// the bootstrap-rendered pgbackrest.conf and the row seeded into
// multigres.pgbackrest_repos.
func InitialPgBackRestRepo(keys CipherKeys) PgBackRestRepo {
	repo := PgBackRestRepo{
		Generation:    InitialRepoGeneration,
		RepoNumber:    1,
		State:         PgBackRestRepoStateActive,
		Authoritative: true,
		Version:       1,
	}
	if pass := keys[InitialRepoGeneration]; pass != "" {
		repo.Encrypted = true
		repo.KeyFingerprint = CipherKeyFingerprint(pass)
	}
	return repo
}

// validateRepos checks that a repository set is renderable: non-empty, unique
// positive generations, repo numbers contiguous from 1 (pgbackrest requires
// commands to name the repo explicitly when repo1 is absent), the
// authoritative repository being exactly repo 1, and coherent encryption
// declarations.
func validateRepos(repos []PgBackRestRepo) error {
	if len(repos) == 0 {
		return errors.New("no pgbackrest repositories to render")
	}
	generations := make(map[int64]bool, len(repos))
	numbers := make(map[int64]bool, len(repos))
	for _, repo := range repos {
		if repo.Generation < 1 {
			return fmt.Errorf("invalid repository generation %d", repo.Generation)
		}
		if generations[repo.Generation] {
			return fmt.Errorf("duplicate repository generation %d", repo.Generation)
		}
		generations[repo.Generation] = true
		if repo.RepoNumber < 1 || repo.RepoNumber > int64(len(repos)) {
			return fmt.Errorf("repository generation %d: repo number %d is outside the contiguous range 1..%d", repo.Generation, repo.RepoNumber, len(repos))
		}
		if numbers[repo.RepoNumber] {
			return fmt.Errorf("duplicate repo number %d", repo.RepoNumber)
		}
		numbers[repo.RepoNumber] = true
		if repo.Authoritative != (repo.RepoNumber == 1) {
			return fmt.Errorf("repository generation %d: authoritative=%t is inconsistent with repo number %d (the authoritative repository is always repo 1)", repo.Generation, repo.Authoritative, repo.RepoNumber)
		}
		if repo.Encrypted != (repo.KeyFingerprint != "") {
			return fmt.Errorf("repository generation %d: encrypted=%t is inconsistent with key fingerprint %q", repo.Generation, repo.Encrypted, repo.KeyFingerprint)
		}
	}
	return nil
}

// orderReposForRendering returns the repositories sorted by their explicit
// repo number. The generation→repo-number mapping is table state, never
// derived. Does not mutate its input.
func orderReposForRendering(repos []PgBackRestRepo) []PgBackRestRepo {
	ordered := make([]PgBackRestRepo, len(repos))
	copy(ordered, repos)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].RepoNumber < ordered[j].RepoNumber
	})
	return ordered
}

// resolveRepoCipher returns the passphrase to render for a repository. An
// unencrypted repo renders no cipher regardless of mounted keys; an encrypted
// repo requires a declared, non-empty key whose fingerprint matches the row —
// never fall back to rendering it unencrypted.
func resolveRepoCipher(repo PgBackRestRepo, keys CipherKeys) (string, error) {
	if !repo.Encrypted {
		return "", nil
	}
	pass, declared := keys[int(repo.Generation)]
	if !declared || pass == "" {
		return "", fmt.Errorf("repository generation %d is encrypted but no cipher key for it is mounted", repo.Generation)
	}
	if CipherKeyFingerprint(pass) != repo.KeyFingerprint {
		return "", fmt.Errorf("cipher key for generation %d does not match the repository's key fingerprint", repo.Generation)
	}
	return pass, nil
}
