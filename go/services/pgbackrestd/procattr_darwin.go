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

	"golang.org/x/sys/unix"
)

// procStatZombie is SZOMB from <sys/proc.h>, which x/sys does not export.
const procStatZombie = 5

// readProcessTable reads every live process's parent and process group from
// kern.proc.all. Zombies are left out: they hold no lock. The sysctl returns
// the whole table or fails, so there is no partial reading here.
func readProcessTable() (processTable, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return processTable{}, fmt.Errorf("read the process table: %w", err)
	}
	table := processTable{procs: make(map[int]procEntry, len(procs))}
	for i := range procs {
		if procs[i].Proc.P_stat == procStatZombie {
			continue
		}
		table.procs[int(procs[i].Proc.P_pid)] = procEntry{parent: int(procs[i].Eproc.Ppid), group: int(procs[i].Eproc.Pgid)}
	}
	return table, nil
}
