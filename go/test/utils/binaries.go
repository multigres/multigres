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
	"fmt"
	"os/exec"
	"testing"
)

// hasPostgreSQLBinaries checks if required PostgreSQL binaries are available
func HasPostgreSQLBinaries() bool {
	requiredBinaries := []string{"initdb", "postgres", "pg_ctl", "pg_isready"}

	for _, binary := range requiredBinaries {
		_, err := exec.LookPath(binary)
		if err != nil {
			return false
		}
	}

	return true
}

// ShouldSkipRealPostgres returns true if tests should skip real PostgreSQL tests.
// If the PostgreSQL binaries are not found, it returns an error.
func ShouldSkipRealPostgres() (bool, error) {
	if testing.Short() {
		return true, nil
	}
	if !HasPostgreSQLBinaries() {
		return true, fmt.Errorf("PostgreSQL binaries not found, please install PostgreSQL to run integration tests")
	}
	return false, nil
}
