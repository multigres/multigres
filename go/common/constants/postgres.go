// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package constants

import "time"

// PostgreSQL default values - semantically separate concepts.
// These are distinct constants despite having the same string value because
// they represent different concepts that could diverge in the future.
const (
	// DefaultPostgresUser is the default PostgreSQL superuser name.
	// This is the administrative user that owns the database cluster and is used
	// by pgctld for all internal operations.
	DefaultPostgresUser = "postgres"

	// PgUserEnvVar is the environment variable for the PostgreSQL role used by pgctld.
	PgUserEnvVar = "POSTGRES_USER"

	// PgPasswordEnvVar is the environment variable for the PostgreSQL password.
	PgPasswordEnvVar = "POSTGRES_PASSWORD" //nolint:gosec // This is an env var name, not a credential

	// PgDatabaseEnvVar is the environment variable for the PostgreSQL database name.
	PgDatabaseEnvVar = "POSTGRES_DB"

	// PgDataDirEnvVar is the environment variable for the PostgreSQL data directory.
	PgDataDirEnvVar = "PGDATA"

	// PgInitdbArgsEnvVar is the environment variable for extra arguments passed to initdb.
	PgInitdbArgsEnvVar = "POSTGRES_INITDB_ARGS"

	// PgInitDbSQLFilesEnvVar is the environment variable for init SQL files to run after initdb.
	// Multiple files are comma-separated.
	PgInitDbSQLFilesEnvVar = "POSTGRES_INITDB_SQL_FILES"

	// DefaultPostgresDatabase is the default database that always exists in PostgreSQL.
	// This database is created during cluster initialization.
	DefaultPostgresDatabase = "postgres"

	// PostgresExecutable is the name of the PostgreSQL server binary.
	PostgresExecutable = "postgres"

	// MultigresMarkerDirectory is the name of the directory used by pgctld to
	// mark a PostgreSQL data directory as managed by pgctld. This is also where
	// all marker files are stored, such as the file indicating that the cluster
	// is in the process of being initialized. This directory is created inside
	// the PostgreSQL data directory.
	MultigresMarkerDirectory = "multigres"

	// ConsensusTermFile is the name of the file used to persist the consensus term
	// for a multipooler instance. It is stored under the pooler directory.
	ConsensusTermFile = "consensus_term.json"

	// BootstrapSentinelFile marks an in-progress first-backup bootstrap. Written
	// before initdb and removed after the final data-directory cleanup; its
	// presence on startup means a prior attempt crashed and the stale pg_data
	// can be removed. Lives in pooler_dir (not PGDATA) to stay out of pgBackRest
	// backups.
	BootstrapSentinelFile = ".multigres-bootstrap-in-progress"

	// DefaultSlowQueryThreshold is the duration after which a query is logged at WARN level.
	DefaultSlowQueryThreshold = 1 * time.Second

	// CrashRecoveryMaxAttempts bounds the retry window used by pgctld to wait out
	// the orphan-cleanup race after a postmaster crash. Suggested by MUL-394:
	// ~5s covers the worst-case worker PostmasterIsAlive() detection latency
	// observed in practice.
	CrashRecoveryMaxAttempts = 10

	// CrashRecoveryRetryDelay caps the delay between `postgres --single` retry
	// attempts during the orphan-cleanup window.
	CrashRecoveryRetryDelay = 500 * time.Millisecond
)
