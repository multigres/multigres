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

// Package pgsecret holds the file-reading primitive for the PostgreSQL
// superuser password. The actual precedence resolution (explicit file path
// beats env vars, explicitly-empty sources are errors, required-ness) lives
// with the two consumers — pgctld's GetPostgresPassword and the multipooler's
// connpoolmanager ResolvePgPassword — which implement the identity/secrets
// contract documented in go/common/constants/postgres.go using
// pflag.Flag.Changed and os.LookupEnv to detect operator intent; a shared
// Getenv-style resolver could not make those distinctions.
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
)

// ReadPasswordFile reads a postgres password from path and trims trailing
// CR/LF. Exposed for callers that already resolve the file path via their own
// configuration layer (e.g. viperutil) and only need the file-reading
// primitive.
func ReadPasswordFile(path string) (string, error) {
	// #nosec G703 -- path is an operator-configured password-file location (flag/env/Secret mount), not external request input.
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read postgres password file %q: %w", path, err)
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}
