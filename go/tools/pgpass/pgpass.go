// Copyright 2025 Supabase, Inc.
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

package pgpass

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CreatePgpassFile creates a .pgpass file with the given password at <poolerDir>/.pgpass.
// The file is created with PostgreSQL standard format: hostname:port:database:username:password
// Using wildcard entries (*:*:*:postgres:password) to match any host/port/database.
// File permissions are set to 0600 as required by PostgreSQL.
func CreatePgpassFile(poolerDir, password string) error {
	pgpassFile := PgpassFilePath(poolerDir)

	// Format: hostname:port:database:username:password
	// Use wildcards to match any connection
	content := fmt.Sprintf("*:*:*:postgres:%s\n", password)

	// Write with 0600 permissions (required by PostgreSQL)
	if err := os.WriteFile(pgpassFile, []byte(content), 0o600); err != nil {
		return fmt.Errorf("failed to create .pgpass file: %w", err)
	}

	return nil
}

// PgpassFilePath returns the path to the .pgpass file for a pooler directory.
func PgpassFilePath(poolerDir string) string {
	return filepath.Join(poolerDir, ".pgpass")
}

// ReadPasswordFromPgpass parses a .pgpass file and returns the password for the postgres user.
// It validates file permissions and format, returning an error if either is invalid.
func ReadPasswordFromPgpass(pgpassFile string) (string, error) {
	// Verify file permissions are 0600
	info, err := os.Stat(pgpassFile)
	if err != nil {
		return "", err
	}
	if info.Mode().Perm() != 0o600 {
		return "", fmt.Errorf("invalid .pgpass file permissions: must be 0600 (current: %04o)", info.Mode().Perm())
	}

	// Read file content
	content, err := os.ReadFile(pgpassFile)
	if err != nil {
		return "", err
	}

	// Parse .pgpass format: hostname:port:database:username:password
	lines := strings.Split(string(content), "\n") //nolint:modernize // TODO: use SplitSeq when upgrading to Go 1.24+
	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Split(line, ":")
		if len(parts) != 5 {
			// Skip malformed lines rather than error - be lenient with format
			continue
		}

		// Look for matching entry (wildcards or specific postgres user)
		username := parts[3]
		password := parts[4]

		if username == "postgres" || username == "*" {
			return password, nil
		}
	}

	return "", errors.New("no entry found for user 'postgres' in .pgpass file")
}

// ValidatePgpassFile checks that a .pgpass file exists, has correct permissions, and valid format.
func ValidatePgpassFile(pgpassFile string) error {
	// Check file exists
	info, err := os.Stat(pgpassFile)
	if err != nil {
		return fmt.Errorf("no .pgpass file found at %s. Please create a .pgpass file with format: *:*:*:postgres:<password>", pgpassFile)
	}

	// Verify permissions
	if info.Mode().Perm() != 0o600 {
		return fmt.Errorf("invalid .pgpass file permissions: must be 0600 (current: %04o)", info.Mode().Perm())
	}

	// Try reading password to validate format
	_, err = ReadPasswordFromPgpass(pgpassFile)
	if err != nil {
		return fmt.Errorf("invalid .pgpass format in %s: %w", pgpassFile, err)
	}

	return nil
}
