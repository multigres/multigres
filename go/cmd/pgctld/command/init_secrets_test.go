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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logSuppressPreamble is the statement-logging suppression that
// buildInitSecretsSQL prepends to every script, so plaintext passwords in
// ALTER ROLE ... PASSWORD never reach the server log (the generated config uses
// log_statement = 'ddl'). Tests assert it precedes the data statements.
const logSuppressPreamble = "SET log_statement = 'none';\n" +
	"SET log_min_error_statement = 'panic';\n" +
	"SET log_min_duration_statement = -1;\n"

func TestBuildInitSecretsSQL_DeterministicAndQuoted(t *testing.T) {
	secrets := &initSecrets{
		Roles: map[string]string{
			// Intentionally out of order to prove sorting.
			"supabase_auth_admin": "pw2",
			"authenticator":       "O'Brien", // embedded quote must be escaped
		},
		DatabaseSettings: map[string]map[string]string{
			"postgres": {"app.settings.jwt_exp": "3600"},
		},
	}
	sql, err := buildInitSecretsSQL(secrets)
	require.NoError(t, err)

	want := logSuppressPreamble +
		"ALTER ROLE \"authenticator\" PASSWORD 'O''Brien';\n" +
		"ALTER ROLE \"supabase_auth_admin\" PASSWORD 'pw2';\n" +
		"ALTER DATABASE \"postgres\" SET \"app.settings.jwt_exp\" TO '3600';\n"
	assert.Equal(t, want, sql)
}

func TestBuildInitSecretsSQL_VerifierStoredVerbatim(t *testing.T) {
	verifier := "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmU=:c2VydmVy"
	secrets := &initSecrets{Roles: map[string]string{"authenticator": verifier}}
	sql, err := buildInitSecretsSQL(secrets)
	require.NoError(t, err)
	assert.Equal(t, logSuppressPreamble+"ALTER ROLE \"authenticator\" PASSWORD '"+verifier+"';\n", sql)
}

// TestBuildInitSecretsSQL_EmptyCombinations covers absent/empty roles and
// database_settings in every combination. Absent (nil map) and empty ({}) must
// behave identically. `want` is the data statements only; every script is
// prefixed with the log-suppression preamble, so the assertion prepends it.
// A payload that resolves to no data statements yields just the preamble.
func TestBuildInitSecretsSQL_EmptyCombinations(t *testing.T) {
	const roleStmt = "ALTER ROLE \"authenticator\" PASSWORD 'pw';\n"
	const dbStmt = "ALTER DATABASE \"postgres\" SET \"app.settings.jwt_exp\" TO '3600';\n"
	roles := map[string]string{"authenticator": "pw"}
	settings := map[string]map[string]string{"postgres": {"app.settings.jwt_exp": "3600"}}

	tests := []struct {
		name    string
		secrets *initSecrets
		want    string
	}{
		{"both nil", &initSecrets{}, ""},
		{"both empty maps", &initSecrets{Roles: map[string]string{}, DatabaseSettings: map[string]map[string]string{}}, ""},
		{"roles only, settings absent", &initSecrets{Roles: roles}, roleStmt},
		{"roles only, settings empty", &initSecrets{Roles: roles, DatabaseSettings: map[string]map[string]string{}}, roleStmt},
		{"settings only, roles absent", &initSecrets{DatabaseSettings: settings}, dbStmt},
		{"settings only, roles empty", &initSecrets{Roles: map[string]string{}, DatabaseSettings: settings}, dbStmt},
		{"settings present but inner map empty yields no statements", &initSecrets{DatabaseSettings: map[string]map[string]string{"postgres": {}}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, err := buildInitSecretsSQL(tt.secrets)
			require.NoError(t, err)
			assert.Equal(t, logSuppressPreamble+tt.want, sql)
		})
	}
}

func TestBuildInitSecretsSQL_Rejects(t *testing.T) {
	tests := []struct {
		name    string
		secrets *initSecrets
		errWant string
	}{
		{
			name:    "empty role name",
			secrets: &initSecrets{Roles: map[string]string{"": "pw"}},
			errWant: "empty role name",
		},
		{
			name:    "empty password",
			secrets: &initSecrets{Roles: map[string]string{"authenticator": ""}},
			errWant: "empty password for role",
		},
		{
			name: "empty database name",
			secrets: &initSecrets{
				DatabaseSettings: map[string]map[string]string{"": {"k": "v"}},
			},
			errWant: "empty database name",
		},
		{
			name: "empty setting key",
			secrets: &initSecrets{
				DatabaseSettings: map[string]map[string]string{"postgres": {"": "v"}},
			},
			errWant: "empty setting key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildInitSecretsSQL(tt.secrets)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errWant)
		})
	}
}

func TestLoadInitSecrets(t *testing.T) {
	t.Run("valid with unknown field ignored", func(t *testing.T) {
		path := writeTempFile(t, `{
			"roles": {"authenticator": "pw"},
			"database_settings": {"postgres": {"app.settings.jwt_exp": "3600"}},
			"future_field": "ignored"
		}`)
		s, err := loadInitSecrets(path)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"authenticator": "pw"}, s.Roles)
		assert.Equal(t, "3600", s.DatabaseSettings["postgres"]["app.settings.jwt_exp"])
	})

	t.Run("missing sections yield empty maps", func(t *testing.T) {
		path := writeTempFile(t, `{}`)
		s, err := loadInitSecrets(path)
		require.NoError(t, err)
		assert.Nil(t, s.Roles)
		assert.Nil(t, s.DatabaseSettings)
	})

	t.Run("roles only, settings absent", func(t *testing.T) {
		path := writeTempFile(t, `{"roles": {"authenticator": "pw"}}`)
		s, err := loadInitSecrets(path)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"authenticator": "pw"}, s.Roles)
		assert.Nil(t, s.DatabaseSettings)
	})

	t.Run("settings only, roles absent", func(t *testing.T) {
		path := writeTempFile(t, `{"database_settings": {"postgres": {"app.settings.jwt_exp": "3600"}}}`)
		s, err := loadInitSecrets(path)
		require.NoError(t, err)
		assert.Nil(t, s.Roles)
		assert.Equal(t, "3600", s.DatabaseSettings["postgres"]["app.settings.jwt_exp"])
	})

	t.Run("empty database_settings object is not nil-distinct", func(t *testing.T) {
		path := writeTempFile(t, `{"roles": {"authenticator": "pw"}, "database_settings": {}}`)
		s, err := loadInitSecrets(path)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"authenticator": "pw"}, s.Roles)
		assert.Empty(t, s.DatabaseSettings)
	})

	t.Run("malformed JSON errors", func(t *testing.T) {
		path := writeTempFile(t, `{"roles": `)
		_, err := loadInitSecrets(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot parse init secrets file")
	})

	t.Run("missing file errors", func(t *testing.T) {
		_, err := loadInitSecrets(filepath.Join(t.TempDir(), "nope.json"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot read init secrets file")
	})
}

func TestRedactPsqlOutput_DropsPasswordLines(t *testing.T) {
	out := []byte("psql:<stdin>:1: ERROR:  syntax error\n" +
		"LINE 1: ALTER ROLE \"authenticator\" PASSWORD 'supersecret';\n" +
		"                                            ^")
	err := redactPsqlOutput(out)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "supersecret")
	assert.NotContains(t, err.Error(), "PASSWORD")
	assert.Contains(t, err.Error(), "syntax error")
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "init-secrets.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}
