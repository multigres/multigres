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

// Package readonly holds the per-database read-only switch. Multiadmin flips
// it on the topo Database record; every gateway watches the record and
// mirrors it here, so the switch survives gateway restarts and reaches
// gateways started mid-incident.
//
// Enforcement is deliberately PostgreSQL's own: while a database is read-only
// the gateway overlays default_transaction_read_only=on onto every request's
// session settings and postgres rejects writes with SQLSTATE 25006 — including
// writes buried in functions, CTEs and DO blocks that a statement-type check
// would miss. The gateway only has to refuse the session-level overrides
// (SET transaction_read_only, BEGIN READ WRITE, ...), see planner.ReadOnlyOverride.
package readonly

import "sync"

// Mode is the read-only state of one database.
type Mode struct {
	// Enabled rejects new write transactions.
	Enabled bool
	// Force additionally terminates sessions that are inside a transaction or
	// hold a pinned backend when the mode is observed, since those keep their
	// read-write default until they end.
	Force bool
}

// Modes is the in-memory mirror of the topo read-only flags, keyed by database.
type Modes struct {
	mu  sync.RWMutex
	dbs map[string]Mode
}

// New returns an empty Modes: every database is read-write.
func New() *Modes {
	return &Modes{dbs: make(map[string]Mode)}
}

// Get returns the mode for database. A nil receiver reads as read-write so
// tests that never wire a Modes keep working.
func (m *Modes) Get(database string) Mode {
	if m == nil {
		return Mode{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dbs[database]
}

// Set records mode for database and returns the previous mode.
func (m *Modes) Set(database string, mode Mode) Mode {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev := m.dbs[database]
	if mode == (Mode{}) {
		delete(m.dbs, database)
	} else {
		m.dbs[database] = mode
	}
	return prev
}

// Databases returns the databases with a non-zero mode.
func (m *Modes) Databases() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	dbs := make([]string, 0, len(m.dbs))
	for db := range m.dbs {
		dbs = append(dbs, db)
	}
	return dbs
}
