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

	// PgPasswordFileEnvVar names an environment variable that points at a file
	// containing the PostgreSQL password. Takes precedence over PgPasswordEnvVar
	// when set. Matches the docker-library/postgres convention: the file holds
	// the plaintext password, not a pre-hashed SCRAM verifier.
	PgPasswordFileEnvVar = "POSTGRES_PASSWORD_FILE" //nolint:gosec // env var name, not a credential

	// PgDatabaseEnvVar is the environment variable for the PostgreSQL database name.
	PgDatabaseEnvVar = "POSTGRES_DB"

	// PgDataDirEnvVar is the environment variable for the PostgreSQL data directory.
	PgDataDirEnvVar = "PGDATA"

	// PgInitdbArgsEnvVar is the environment variable for extra arguments passed to initdb.
	PgInitdbArgsEnvVar = "POSTGRES_INITDB_ARGS"

	// PgInitdbSQLFilesEnvVar is the environment variable for init SQL files to run after initdb.
	// Multiple files are comma-separated.
	PgInitdbSQLFilesEnvVar = "POSTGRES_INITDB_SQL_FILES"

	// PgInitdbSQLDirsEnvVar is the environment variable for init SQL dirs to run after initdb.
	// Multiple entries are comma-separated, each in role:path format.
	PgInitdbSQLDirsEnvVar = "POSTGRES_INITDB_SQL_DIRS"

	// PgInitdbExtraConfEnvVar is the environment variable for extra postgresql.conf
	// files appended verbatim onto the generated config at init time.
	// Multiple files are comma-separated. Postgres applies last-write-wins, so
	// extras override values from the templated defaults.
	PgInitdbExtraConfEnvVar = "POSTGRES_INITDB_EXTRA_CONF"

	// PgConfigTemplateEnvVar is the environment variable for the path to a
	// custom postgresql.conf Go template, replacing pgctld's embedded default
	// (config/postgres/template.cnf). Some settings the embedded default
	// deliberately leaves unset for compatibility with stock PostgreSQL
	// images -- e.g. shared_preload_libraries for extensions a custom base
	// image bundles (supautils, pgaudit, pg_cron, ...) -- can be set
	// correctly here instead. Unlike PgInitdbExtraConfEnvVar (which is
	// appended verbatim, unrendered), this file is rendered through the same
	// template engine as the embedded default, so it must use the same
	// {{.Field}} placeholders (see PostgresServerConfig's template fields).
	PgConfigTemplateEnvVar = "POSTGRES_CONFIG_TEMPLATE_PATH"

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

	// ConsensusPromisesFile is the name of the file used to persist a
	// multipooler instance's consensus promises (term revocation and
	// recruit-position floor). It is stored under the pooler directory.
	ConsensusPromisesFile = "consensus_promises.json"

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

	// PgLocksAdvisoryProbeSQL reports whether the current backend still holds any
	// advisory lock. Run only outside a transaction, where every advisory lock
	// visible here is session-level (transaction-level advisory locks are
	// released at transaction end), so a false result means the session has
	// released all of its advisory locks and the backend can be unpinned.
	PgLocksAdvisoryProbeSQL = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid())"

	// RestoreCommandPIDFile is the filename (joined onto the pooler directory)
	// that `pgctld restore-wrapper` writes its own PID to, so pgctld's
	// StopRestoreCommand RPC can check liveness or signal it. Shared between
	// go/services/pgctld (which writes/reads it directly) and
	// go/services/multipooler (which constructs the wrapped restore_command
	// string embedding this path) — a plain constant here avoids
	// go/services/multipooler needing to import go/services/pgctld, which
	// pgctld-isolation forbids. Lives in the pooler dir, not PGDATA, so it
	// survives restore_command being cleared and PGDATA being wiped by a
	// subsequent restore.
	RestoreCommandPIDFile = "restore_command.pid"
)
