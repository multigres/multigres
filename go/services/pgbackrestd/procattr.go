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

package pgbackrestd

import (
	"fmt"
)

// procEntry is what the wait for the 8432 server's workers needs to know
// about one live process.
type procEntry struct {
	parent int
	group  int
}

// processTable is one reading of the system process table. unread is set when
// some part of it could not be read for a reason other than the process having
// exited in the meantime: the table may then be missing a live process, so it
// can prove that a worker exists but not that none does.
type processTable struct {
	procs  map[int]procEntry
	unread error
}

// readProcessTableFn is a variable so a test can make the table unreadable.
var readProcessTableFn = readProcessTable

// serverWorkersAlive reports whether the 8432 server has a live worker.
//
// The server forks one worker per connection itself. The workers are never
// children of this process, so they cannot be reaped or tracked by PID here;
// the system process table is the only place they show. supervisor is the
// per-child supervisor this process started for the server, and server is the
// pgBackRest server it runs (0 when not known). A worker is any live process
// other than those two that
//
//   - descends from the supervisor, which covers the ordinary case (a child of
//     the server) and, on Linux, a worker whose server died: the supervisor is
//     a subreaper, so the orphan is reparented to it; or
//   - is in the supervisor's or the server's process group, which covers the
//     same orphan where there is no subreaper and it is reparented to init.
//
// Between the supervisor starting and it reporting the server's pid, server is
// 0 and the server itself answers as a worker. That window is a few
// milliseconds and costs a backup one polling interval.
//
// An error means the table could not prove there is no worker, and the caller
// must not read that as "none".
func serverWorkersAlive(supervisor, server int) (bool, error) {
	table, err := readProcessTableFn()
	if err != nil {
		return false, err
	}
	if table.hasWorker(supervisor, server) {
		return true, nil
	}
	if table.unread != nil {
		return false, fmt.Errorf("incomplete process table: %w", table.unread)
	}
	return false, nil
}

func (t processTable) hasWorker(supervisor, server int) bool {
	for pid, p := range t.procs {
		if pid == supervisor || pid == server {
			continue
		}
		if p.group == supervisor || (server != 0 && p.group == server) || t.descendsFrom(pid, supervisor) {
			return true
		}
	}
	return false
}

// descendsFrom walks pid's ancestry. The hop limit ends a walk through a table
// read while PIDs were being reused.
func (t processTable) descendsFrom(pid, root int) bool {
	for range 64 {
		p, ok := t.procs[pid]
		if !ok {
			return false
		}
		if p.parent == root {
			return true
		}
		pid = p.parent
	}
	return false
}
