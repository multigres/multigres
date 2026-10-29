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
