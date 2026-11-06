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
	// poller refreshes repo-derived state (last-backup age/count, readiness) from
	// `pgbackrest info` and PostgreSQL. It is paced by that repo call, which is a
	// remote round-trip. The gauges it feeds are ages anchored on rare events — a
	// backup completing, a repo becoming unreachable — so deriving them from
	// cached timestamps at scrape time stays accurate between polls, hence
	// minutes. The WAL-archive backlog is not such an age; see ArchiverPollInterval.
	BackupHealthPollInterval = 5 * time.Minute

	// ArchiverPollInterval is how often the WAL-archive backlog is read from
	// PostgreSQL. It is deliberately separate from BackupHealthPollInterval: the
	// backlog is not an age that grows with the clock but a queue depth that
	// collapses to zero the moment its oldest segment is archived, so it cannot be
	// extrapolated between polls, only measured. The read is a local
	// pg_stat_archiver query plus an archive_status listing, cheap enough to run
	// at this cadence.
	ArchiverPollInterval = 30 * time.Second

	// ArchiverStaleAfter is how long the last successful archiver reading stays
	// usable. Past it the backlog gauges stop being emitted, so alerting sees a
	// gap rather than a reading that keeps climbing against a live clock long
	// after it stopped being true.
	ArchiverStaleAfter = 3 * ArchiverPollInterval

	// defaultUnrecoverableMinAttempts is the floor of genuine failed recovery
	// attempts required before a pooler may quarantine itself, regardless of
	// elapsed time. It guards against quarantining on too few real attempts — e.g.
	// a single attempt that hangs for the whole timeout, or sparse monitor ticks.
	// Overridable via --postgres-unrecoverable-min-attempts; used when the manager's
	// unrecoverableMinAttempts is unset (0), e.g. in tests.
	DefaultUnrecoverableMinAttempts = 3
)
