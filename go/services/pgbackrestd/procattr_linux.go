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
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
)

// readProcStat is a variable so a test can fail the read of one process.
var readProcStat = os.ReadFile

// readProcessTable reads every live process's parent and process group from
// /proc. Zombies and dead processes are left out: they hold no lock. A process
// that exits between the directory read and its stat read is skipped; any
// other failure to read or parse a row marks the table incomplete, because the
// row that could not be read may be the live worker.
func readProcessTable() (processTable, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return processTable{}, fmt.Errorf("read the process table: %w", err)
	}
	table := processTable{procs: map[int]procEntry{}}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := readProcStat("/proc/" + e.Name() + "/stat")
		if err != nil {
			// ESRCH is what a read returns when the process exits while its
			// stat file is open.
			if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ESRCH) {
				table.unread = errors.Join(table.unread, fmt.Errorf("pid %d: %w", pid, err))
			}
			continue
		}
		entry, live, err := parseProcStat(b)
		if err != nil {
			table.unread = errors.Join(table.unread, fmt.Errorf("pid %d: %w", pid, err))
			continue
		}
		if live {
			table.procs[pid] = entry
		}
	}
	return table, nil
}

// parseProcStat reads state, ppid and pgrp (fields 3 to 5) from a
// /proc/<pid>/stat line, counted from the last ")" because the command name
// before it may contain spaces and parentheses. live is false for a zombie or
// a dead process.
func parseProcStat(stat []byte) (entry procEntry, live bool, err error) {
	end := bytes.LastIndexByte(stat, ')')
	if end < 0 {
		return procEntry{}, false, errors.New("malformed stat line: no command name")
	}
	fields := bytes.Fields(stat[end+1:])
	if len(fields) < 3 {
		return procEntry{}, false, fmt.Errorf("malformed stat line: %d fields after the command name", len(fields))
	}
	state := string(fields[0])
	if state == "Z" || state == "X" {
		return procEntry{}, false, nil
	}
	if entry.parent, err = strconv.Atoi(string(fields[1])); err != nil {
		return procEntry{}, false, fmt.Errorf("malformed stat line: parent pid: %w", err)
	}
	if entry.group, err = strconv.Atoi(string(fields[2])); err != nil {
		return procEntry{}, false, fmt.Errorf("malformed stat line: process group: %w", err)
	}
	return entry, true, nil
}
