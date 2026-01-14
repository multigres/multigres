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

package utils

import (
	"os"
	"path/filepath"
	"testing"
)

// CreateTestPgpassFile creates a .pgpass file for testing purposes.
// The password parameter is marked as #nosec G101 to suppress gosec warnings
// about hardcoded credentials in test code.
func CreateTestPgpassFile(t *testing.T, poolerDir, password string) string {
	t.Helper()

	pgpassFile := filepath.Join(poolerDir, ".pgpass")
	pgpassContent := "*:*:*:postgres:" + password + "\n" // #nosec G101 -- test password

	if err := os.WriteFile(pgpassFile, []byte(pgpassContent), 0o600); err != nil {
		t.Fatalf("failed to create test .pgpass file: %v", err)
	}

	return pgpassFile
}
