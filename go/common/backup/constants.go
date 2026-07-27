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

import "time"

const (
	// S3 upload chunk size for pgBackRest
	S3UploadChunkSize = "10MiB"

	// InitialRepoGeneration is the generation of the first backup repository
	// every cluster gets by convention.
	InitialRepoGeneration = 1

	// CipherType is the pgBackRest repository cipher used for client-side
	// backup encryption.
	CipherType = "aes-256-cbc"

	// CipherKeyFileEnvVar names an environment variable that points at a file
	// containing backup repository cipher keys: a JSON document mapping
	// repository generation to passphrase, e.g. {"1": "<passphrase>"}.
	// Typically a mounted Kubernetes Secret. An explicitly configured file is
	// authoritative and must be readable, or startup fails — never a silent
	// fallback to unencrypted.
	CipherKeyFileEnvVar = "MULTIGRES_PGBACKREST_CIPHER_KEY_FILE"

	// cipherKeyFingerprintLen is the length in hex characters of a cipher key
	// fingerprint (128 bits of the SHA-256).
	cipherKeyFingerprintLen = 32

	// Retention policies for backups
	// RetentionDifferential: Keep the most recent differential backup
	RetentionDifferential = "1"
	// RetentionFull: Keep this many full backups
	RetentionFull = "7"
	// RetentionFullType: Use count-based retention for full backups
	RetentionFullType = "count"
	// RetentionHistory: Disable WAL archive retention (0 = disabled)
	// When set to 0, pgBackRest does not enforce WAL retention separately from backup retention
	RetentionHistory = "0"

	// Operation timeouts
	// BackupTimeout is the maximum time allowed for a backup operation
	BackupTimeout = 30 * time.Minute
	// RestoreTimeout is the maximum time allowed for a restore operation
	RestoreTimeout = 30 * time.Minute
	// VerifyTimeout is the maximum time allowed for backup verification
	VerifyTimeout = 10 * time.Minute
	// StanzaVerifyTimeout is the maximum time allowed for a full-stanza
	// verify (no --set). Larger than VerifyTimeout because verify walks
	// every backup + WAL segment in the repository.
	StanzaVerifyTimeout = 60 * time.Minute
	// InfoTimeout is the maximum time allowed for querying backup information
	InfoTimeout = 30 * time.Second
	// ValidationTimeout is the maximum time allowed for S3 bucket validation
	ValidationTimeout = 10 * time.Second
	// ExpireTimeout is the maximum time allowed for a backup expiration operation
	ExpireTimeout = 10 * time.Minute
	// CheckTimeout bounds `pgbackrest check`. check forces a WAL switch and
	// waits for the segment to reach the repo, so it is short — but allow
	// headroom for archive-push latency to a remote (S3) repo.
	CheckTimeout = 2 * time.Minute
)
