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

// Package pgsecret resolves the PostgreSQL superuser password from one of
// several sources. It exists so every binary that needs the password (pgctld,
// multipooler) applies the same precedence rules and so callers can avoid
// embedding `os.Getenv("POSTGRES_PASSWORD")` directly.
//
// The file-based source matches the docker-library/postgres convention: the
// file contains the plaintext password (initdb hashes it under the configured
// auth method). Tooling that already targets PGDG images is therefore
// byte-for-byte compatible.
package pgsecret

import (
	"fmt"
	"os"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
)

// ReadPostgresPassword returns the plaintext PostgreSQL password, resolved in
// this order, stopping at the first non-empty source:
//
//  1. file at filePath (typically the value of a --pg-password-file flag)
//  2. file at $POSTGRES_PASSWORD_FILE (constants.PgPasswordFileEnvVar)
//  3. $POSTGRES_PASSWORD (constants.PgPasswordEnvVar)
//
// Returns "" with no error when none of the sources are set; callers are
// responsible for enforcing required-ness so they can produce a domain-specific
// error message. Trailing CR/LF is trimmed from file contents so a Kubernetes
// Secret written with `stringData` still yields the intended password.
func ReadPostgresPassword(filePath string) (string, error) {
	if filePath != "" {
		return ReadPasswordFile(filePath)
	}
	if envFile := os.Getenv(constants.PgPasswordFileEnvVar); envFile != "" {
		return ReadPasswordFile(envFile)
	}
	return os.Getenv(constants.PgPasswordEnvVar), nil
}

// ReadPasswordFile reads a postgres password from path and trims trailing
// CR/LF. Exposed for callers that already resolve the file path via their own
// configuration layer (e.g. viperutil) and only need the file-reading
// primitive.
func ReadPasswordFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read postgres password file %q: %w", path, err)
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}
