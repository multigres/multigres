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

package constants

import "time"

// Multipooler manager constants.
const (
	// BackupHealthPollInterval is how often the multipooler's backup-health
	// poller refreshes its cached snapshot (last-backup age/count, readiness,
	// WAL archive lag) from the repo and PostgreSQL. The gauges and status page
	// therefore reflect state that may be up to this old. The gauge callbacks
	// derive ages from cached timestamps at scrape time, so this only needs to
	// be frequent enough to catch state changes (a new backup, an expiration,
	// the repo going unreachable), not to keep ages fresh — hence minutes.
	BackupHealthPollInterval = 5 * time.Minute
)
