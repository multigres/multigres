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

	// PgInitSecretsFileEnvVar names an environment variable that points at a JSON
	// file of per-project day-0 state (role passwords/verifiers and database
	// settings) applied during the transient init phase.
	PgInitSecretsFileEnvVar = "POSTGRES_INIT_SECRETS_FILE" //nolint:gosec // env var name, not a credential

	// PgInitdbExtraConfEnvVar is the environment variable for extra postgresql.conf
	// files live-included (via include_if_exists) at the end of the generated
	// config. Multiple files are comma-separated. Postgres applies
	// last-write-wins, so extras override values from the templated defaults.
	// The referenced files are re-read on every server start and reload, so
	// editing them (e.g. an operator-managed ConfigMap) takes effect on restart
	// rather than being frozen at first initdb.
	PgInitdbExtraConfEnvVar = "POSTGRES_INITDB_EXTRA_CONF"

	// PgConfigTemplateEnvVar is the environment variable for the path to a
	// custom postgresql.conf Go template, replacing pgctld's embedded default
	// (config/postgres/template.cnf). Some settings the embedded default
	// deliberately leaves unset for compatibility with stock PostgreSQL
	// images -- e.g. shared_preload_libraries for extensions a custom base
	// image bundles (supautils, pgaudit, pg_cron, ...) -- can be set
	// correctly here instead. Unlike PgInitdbExtraConfEnvVar (which is
	// live-included unrendered), this file is rendered through the same
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

	// RewindSentinelFile marks an in-progress pg_rewind. Written before the actual
	// (mutating) pg_rewind runs and removed only after postgres is verified back up
	// as a standby; its presence on startup means a prior rewind was interrupted
	// (e.g. the pod was killed mid-rewind) and the data directory is partially
	// rewound — unstartable and, per PostgreSQL guidance, generally unrecoverable.
	// The monitor uses it to force the rewind-repair path instead of starting
	// postgres on the half-rewound directory, and to quarantine if repair keeps
	// failing. Lives in pooler_dir (not PGDATA) so it stays out of pgBackRest
	// backups, and on the local volume so it survives a pod restart on the same PVC.
	RewindSentinelFile = ".multigres-rewind-in-progress"

	// StandbySignalFile is PostgreSQL's marker file (in PGDATA) whose presence
	// puts the server into standby mode. Notably, postgres --single refuses to
	// run with it present, so crash recovery removes and recreates it.
	StandbySignalFile = "standby.signal"

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
	// Schema-qualified so a pg_temp relation or a search_path function cannot
	// shadow the catalog (see SessionSourceProbeSQL).
	PgLocksAdvisoryProbeSQL = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype = 'advisory' AND pid = pg_catalog.pg_backend_pid())"

	// PreparedStatementsProbeSQL lists the named prepared statements the
	// current backend holds, protocol-level (Parse) and SQL-level (PREPARE)
	// alike. The unnamed statement is never listed. Run by the multipooler
	// scrubber to compare against the pool's tracked prepared statements.
	PreparedStatementsProbeSQL = "SELECT name FROM pg_catalog.pg_prepared_statements"

	// TempObjectsProbeSQL returns one row per object in the current backend's
	// temporary schema, tagged by kind, across every namespace-scoped catalog
	// the gateway's pg_temp CREATE rejection covers: a relkind code per
	// pg_class entry, 'function' per pg_proc entry (aggregates included),
	// 'type:' plus the typtype code per standalone pg_type entry (domains,
	// enums, ranges, multiranges), and a fixed tag per operator, collation,
	// statistics object, operator class, operator family, conversion, and
	// text-search parser/dictionary/template/configuration. Composite types
	// are counted through pg_class, and the array type PostgreSQL creates
	// alongside every type is skipped, so each user-created object yields
	// one row. pg_my_temp_schema() is 0 when the session has no temp schema,
	// which no namespace OID matches, so the probe returns no rows.
	//
	// Run by the multipooler scrubber; an idle pooled backend must own none.
	// Relations and types are the dangerous classes: pg_temp is searched
	// before pg_catalog for unqualified relation and type names (verified: a
	// pg_temp domain named text captures unqualified ::text), so a leftover
	// shadows the catalog for the next borrower. Operators, collations, and
	// the rest are never resolved through pg_temp, even with pg_temp listed
	// in search_path; they are reported as stale state for completeness.
	TempObjectsProbeSQL = "SELECT relkind::text FROM pg_catalog.pg_class WHERE relnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'function' FROM pg_catalog.pg_proc WHERE pronamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'type:' || typtype::text FROM pg_catalog.pg_type WHERE typnamespace = pg_catalog.pg_my_temp_schema()" +
		" AND typtype <> 'c' AND NOT (typtype = 'b' AND typelem <> 0)" +
		" UNION ALL SELECT 'operator' FROM pg_catalog.pg_operator WHERE oprnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'collation' FROM pg_catalog.pg_collation WHERE collnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'statistics' FROM pg_catalog.pg_statistic_ext WHERE stxnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'operator_class' FROM pg_catalog.pg_opclass WHERE opcnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'operator_family' FROM pg_catalog.pg_opfamily WHERE opfnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'conversion' FROM pg_catalog.pg_conversion WHERE connamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'ts_parser' FROM pg_catalog.pg_ts_parser WHERE prsnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'ts_dictionary' FROM pg_catalog.pg_ts_dict WHERE dictnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'ts_template' FROM pg_catalog.pg_ts_template WHERE tmplnamespace = pg_catalog.pg_my_temp_schema()" +
		" UNION ALL SELECT 'ts_config' FROM pg_catalog.pg_ts_config WHERE cfgnamespace = pg_catalog.pg_my_temp_schema()"

	// SessionSourceProbeSQL is the session-state probe run by the multipooler
	// scrubber. It reads the backend's real session GUC state in one
	// round trip and compares it against the connection's tracked settings label.
	// Three sources are combined, because pg_settings alone cannot see everything
	// (verified on PostgreSQL 17):
	//
	//   - 'session' rows: pg_settings WHERE source = 'session' — every defined
	//     GUC whose current value was installed by this session (SET,
	//     set_config(..., false), including any hidden inside routine bodies).
	//     current_setting() is used instead of pg_settings.setting because it
	//     returns the SHOW-style display form, which is also what set_config
	//     returns in the normalization probe, keeping comparisons
	//     apples-to-apples.
	//     Both pg_settings and current_setting are schema-qualified: pg_temp
	//     is searched before pg_catalog for unqualified relation names, and a
	//     client's search_path could shadow an unqualified function name.
	//   - 'identity' rows: role and session_authorization are GUC_NO_SHOW_ALL —
	//     they NEVER appear in pg_settings — so they are read explicitly.
	//     current_setting('role') reports 'none' when no SET ROLE is in effect;
	//     session_user is the current session authorization.
	//   - 'custom' rows: placeholder GUCs (names with a dot, e.g. 'my.tenant')
	//     are also hidden from pg_settings until an extension defines them, so
	//     every custom name in the tracked label is read explicitly with
	//     current_setting(name, missing_ok := true), which returns NULL when the
	//     session has never seen the GUC.
	//
	// Known blind spot: a custom GUC set behind tracking's back on a connection
	// whose label does not contain it is undetectable — placeholder GUCs cannot
	// be enumerated from SQL. The creation-time rejection gates remain the
	// defense for that class.
	SessionSourceProbeSQL = "SELECT name, pg_catalog.current_setting(name), 'session' FROM pg_catalog.pg_settings WHERE source = 'session'" +
		" UNION ALL SELECT 'role', pg_catalog.current_setting('role'), 'identity'" +
		" UNION ALL SELECT 'session_authorization', session_user::text, 'identity'"

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
