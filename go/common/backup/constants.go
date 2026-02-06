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

	// Retention policies for backups
	// RetentionDifferential: Keep the most recent differential backup
	RetentionDifferential = "1"
	// RetentionFull: Keep full backups for this many days
	RetentionFull = "28"
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
	// InfoTimeout is the maximum time allowed for querying backup information
	InfoTimeout = 30 * time.Second
	// ValidationTimeout is the maximum time allowed for S3 bucket validation
	ValidationTimeout = 10 * time.Second
)
