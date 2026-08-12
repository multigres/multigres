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

package command

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// initSecrets is the day-0 state handed to pgctld at init via
// --pg-init-secrets-file: per-project role login credentials and database
// settings that cannot be baked into the shared image. The file is a
// per-project Kubernetes Secret; see the MINT-60 plan for the delivery model.
//
// Role values are opaque: a plaintext password (Postgres hashes it per
// password_encryption) or a pre-hashed "SCRAM-SHA-256$..." verifier (stored
// verbatim). pgctld does not distinguish the two — it passes the value straight
// into ALTER ROLE ... PASSWORD.
type initSecrets struct {
	// Roles maps additional login role names to its password/verifier. The
	// superuser is NOT expected here and it is wrong to add it. It is set at
	// initdb via --pwfile from its own Secret.
	Roles map[string]string `json:"roles"`

	// DatabaseSettings maps a database name to GUCs applied via
	// ALTER DATABASE <db> SET <key> TO <value> (e.g. app.settings.jwt_exp).
	DatabaseSettings map[string]map[string]string `json:"database_settings"`
}

// loadInitSecrets reads and parses the init-secrets JSON file. Unknown fields
// are ignored so a newer platform payload does not break an older pgctld.
func loadInitSecrets(path string) (*initSecrets, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read init secrets file %q: %w", path, err)
	}
	var s initSecrets
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("cannot parse init secrets file %q: %w", path, err)
	}
	return &s, nil
}

// applyInitSecrets sets role passwords and database settings from the
// init-secrets file over the transient socket. It runs as the last step of
// postInitdbSetup, after the init-scripts have created the roles.
//
// The generated SQL is fed to psql via stdin (never argv/env) so secret values
// are not exposed through /proc. When roles are applied, a verify pass asserts
// no login role was left passwordless, turning a missing entry into a hard init
// failure rather than a silently-broken cluster.
func applyInitSecrets(logger *slog.Logger, pg *pgInstance, cfg PgCtldServiceConfig) error {
	if cfg.InitSecretsFile == "" {
		return nil
	}
	secrets, err := loadInitSecrets(cfg.InitSecretsFile)
	if err != nil {
		return err
	}
	if len(secrets.Roles) == 0 && len(secrets.DatabaseSettings) == 0 {
		logger.Info("init secrets file has no roles or settings, skipping", "file", cfg.InitSecretsFile)
		return nil
	}

	script, err := buildInitSecretsSQL(secrets)
	if err != nil {
		return err
	}

	logger.Info("applying init secrets", "file", cfg.InitSecretsFile)

	// -f - reads the script from stdin so no secret value lands in argv.
	// ON_ERROR_STOP aborts on the first failing statement.
	if out, err := pg.psql(cfg.Database, strings.NewReader(script), "-v", "ON_ERROR_STOP=1", "-f", "-"); err != nil {
		// Never include the script or psql output verbatim: both may echo a
		// secret value. Report only that application failed.
		return fmt.Errorf("failed to apply init secrets (%d role(s)): %w", len(secrets.Roles), redactPsqlOutput(out))
	}

	// Per-section summary. Role names are safe to log, but passwords are NOT, so
	// only names/counts are printed. Database setting values ARE logged: they are
	// non-secret configuration (e.g. app.settings.jwt_exp) — do not put secrets
	// in database_settings.
	if roles := sortedKeys(secrets.Roles); len(roles) > 0 {
		logger.Info("init secrets: set role passwords", "count", len(roles), "roles", roles)
	} else {
		logger.Info("init secrets: no role passwords to set")
	}
	if len(secrets.DatabaseSettings) > 0 {
		for _, db := range sortedKeys(secrets.DatabaseSettings) {
			settings := secrets.DatabaseSettings[db]
			for _, key := range sortedKeys(settings) {
				logger.Info("init secrets: set database option",
					"database", db, "option", key, "value", settings[key])
			}
		}
	} else {
		logger.Info("init secrets: no database options to set")
	}

	if len(secrets.Roles) > 0 {
		if err := verifyNoPasswordlessLoginRoles(logger, pg, cfg); err != nil {
			return err
		}
	}
	return nil
}

// buildInitSecretsSQL renders the ALTER ROLE / ALTER DATABASE statements in a
// deterministic order (sorted by name) so output is stable and testable. Role
// names and setting keys are identifier-quoted; values are literal-quoted.
func buildInitSecretsSQL(secrets *initSecrets) (string, error) {
	var sb strings.Builder

	// Suppress server-side statement logging for this session before running any
	// ALTER ROLE ... PASSWORD. The generated postgresql.conf sets
	// log_statement = 'ddl', which would otherwise write the plaintext password
	// to the postgres server log; log_min_error_statement / log_min_duration
	// would leak it on error or as a "slow" statement. These SETs are
	// session-local to this transient psql connection (superuser), and SET is
	// not itself DDL so these lines are not logged. Keep them ahead of the
	// generated statements in the same stdin script.
	fmt.Fprintln(&sb, "SET log_statement = 'none';")
	fmt.Fprintln(&sb, "SET log_min_error_statement = 'panic';")
	fmt.Fprintln(&sb, "SET log_min_duration_statement = -1;")

	for _, role := range sortedKeys(secrets.Roles) {
		if role == "" {
			return "", errors.New("init secrets: empty role name")
		}
		if secrets.Roles[role] == "" {
			return "", fmt.Errorf("init secrets: empty password for role %q", role)
		}
		fmt.Fprintf(&sb, "ALTER ROLE %s PASSWORD %s;\n",
			quoteIdentifier(role), ast.QuoteStringLiteral(secrets.Roles[role]))
	}

	for _, db := range sortedKeys(secrets.DatabaseSettings) {
		if db == "" {
			return "", errors.New("init secrets: empty database name in database_settings")
		}
		settings := secrets.DatabaseSettings[db]
		for _, key := range sortedKeys(settings) {
			if key == "" {
				return "", fmt.Errorf("init secrets: empty setting key for database %q", db)
			}
			fmt.Fprintf(&sb, "ALTER DATABASE %s SET %s TO %s;\n",
				quoteIdentifier(db), quoteIdentifier(key), ast.QuoteStringLiteral(settings[key]))
		}
	}

	return sb.String(), nil
}

// verifyNoPasswordlessLoginRoles fails if any role that can log in still has no
// password after applying the secrets. This catches both a role omitted from
// the Secret and a role created LOGIN by the init-scripts but never seeded —
// exactly the MINT-60 failure mode — turning it into a hard init error.
//
// It cannot detect a password set to the WRONG verifier (Postgres stores a
// pre-hashed value verbatim without validation); that limit is documented.
func verifyNoPasswordlessLoginRoles(logger *slog.Logger, pg *pgInstance, cfg PgCtldServiceConfig) error {
	out, err := pg.psql(cfg.Database, nil, "-tAc",
		"SELECT rolname FROM pg_authid WHERE rolcanlogin AND rolpassword IS NULL ORDER BY rolname")
	if err != nil {
		return fmt.Errorf("failed to verify role passwords: %w\nOutput: %s", err, out)
	}
	missing := strings.Fields(strings.TrimSpace(string(out)))
	if len(missing) > 0 {
		return fmt.Errorf("login role(s) still without a password after applying init secrets: %s",
			strings.Join(missing, ", "))
	}
	logger.Info("verified all login roles have a password")
	return nil
}

// redactPsqlOutput scrubs psql output before it goes into an error, since a
// failing ALTER ROLE ... PASSWORD statement can echo the secret value. We keep
// only the SQLSTATE-ish prefix lines that do not contain the statement text.
func redactPsqlOutput(out []byte) error {
	var kept []string
	for line := range strings.SplitSeq(string(out), "\n") {
		// psql error lines look like "psql:<file>:<n>: ERROR:  <msg>". Drop any
		// line that could contain the statement (e.g. context/detail echoing the
		// value); keep only ERROR/FATAL summary lines without a PASSWORD token.
		if strings.Contains(line, "PASSWORD") {
			continue
		}
		if strings.Contains(line, "ERROR:") || strings.Contains(line, "FATAL:") {
			kept = append(kept, strings.TrimSpace(line))
		}
	}
	if len(kept) == 0 {
		return errors.New("psql reported an error (output redacted)")
	}
	return fmt.Errorf("%s", strings.Join(kept, "; "))
}

// sortedKeys returns the keys of m in ascending order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
