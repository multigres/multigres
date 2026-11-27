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
	// ResetDrainWait is how long the sidecar waits for in-flight commands and
	// WAL requests before a stanza delete or an archive spool reset gives up.
	ResetDrainWait = 30 * time.Second
	// ResetStepTimeout bounds each pgBackRest child of a stanza delete or an
	// archive spool reset: start, stop, stanza-delete.
	ResetStepTimeout = 30 * time.Second
	// WorkerStopWait bounds how long the sidecar waits for a cancelled or stopped
	// worker, and for the worker barrier, to be acknowledged quiescent. A child
	// that is cancelled can need this long on top of its own timeout, and so can
	// the pause of the 8432 server.
	WorkerStopWait = 15 * time.Second
	// StartAttempts is how many times the sidecar tries the final
	// "pgbackrest start" of a restore, stanza delete or spool reset before it
	// hands a persistent failure to its background retry; StartRetryDelay is the
	// pause between attempts.
	StartAttempts   = 3
	StartRetryDelay = time.Second
	// ChildSpawnedTrailer is the response trailer a COMMAND_STANZA_DELETE Run ends
	// with: "1" when a stanza-delete child was started for the run, "0" when none
	// was (refused, failed or cancelled before spawn). A caller that gets no
	// trailer, because the response was lost, must assume one was.
	ChildSpawnedTrailer = "Pgbackrestd-Child-Spawned"
	// ChildSpawnedYes and ChildSpawnedNone are its two values.
	ChildSpawnedYes  = "1"
	ChildSpawnedNone = "0"
)
