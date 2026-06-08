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

package manager

import (
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/common/constants"
)

// postgresDataDir returns the PostgreSQL data directory path from the PGDATA env var.
func postgresDataDir() string {
	return os.Getenv(constants.PgDataDirEnvVar)
}

// multigresDataDir returns the multigres-specific subdirectory within PGDATA.
func multigresDataDir() string {
	return filepath.Join(postgresDataDir(), constants.MultigresMarkerDirectory)
}
