//go:build linux

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
	"os"
	"strconv"
	"strings"
	"syscall"
)

// Limitation: the kernel clears Pdeathsig on fork, so the local workers
// pgBackRest itself forks (process-max above 1) are not covered. When PID 1
// dies the main restore process is killed, and each worker finishes the file it
// is on and then exits on its closed pipe. That window exists only where the
// sidecar's container does not own a PID namespace, i.e. the local provisioner
// on Linux; in Kubernetes the container's namespace dies with PID 1 and takes
// every process with it.
//
// childSysProcAttr puts a child in its own process group and has the kernel
// kill it if PID 1 dies, so a crashed sidecar cannot leave a restore child
// writing the data directory. Pdeathsig fires when the thread that forked the
// child exits, which is why runChild keeps its goroutine on one OS thread.
func childSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

// groupGone reports whether no live process is left in the group. Zombies do
// not count: they hold no locks, and one reparented to a PID 1 that does not
// reap would otherwise keep the wait going until its bound.
func groupGone(pgid int) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return syscall.Kill(-pgid, 0) == syscall.ESRCH
	}
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		// The command name may contain spaces and parentheses; the fields
		// that matter follow the last one.
		rest := strings.Fields(string(b[bytes.LastIndexByte(b, ')')+1:]))
		if len(rest) >= 3 && rest[0] != "Z" && rest[0] != "X" && rest[2] == strconv.Itoa(pgid) {
			return false
		}
	}
	return true
}

// descendantsAlive reports whether any live process descends from root.
func descendantsAlive(root int) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	parent := map[int]int{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		rest := strings.Fields(string(b[bytes.LastIndexByte(b, ')')+1:]))
		if len(rest) < 2 || rest[0] == "Z" || rest[0] == "X" {
			continue
		}
		ppid, _ := strconv.Atoi(rest[1])
		parent[pid] = ppid
	}
	for pid := range parent {
		for p, hops := pid, 0; hops < 64; hops++ {
			pp, ok := parent[p]
			if !ok {
				break
			}
			if pp == root {
				return true
			}
			p = pp
		}
	}
	return false
}
