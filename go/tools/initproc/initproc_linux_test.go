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

//go:build linux

package initproc

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func init() {
	becomeSubreaper = func() error { return unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) }
	roles["child-orphans"] = childOrphans
}

// childOrphans plays pgctld under the supervisor: it leaves orphans behind the
// way `pg_ctl start` leaves the postmaster, while running a burst of os/exec
// commands. It fails if any orphan is not reparented to the supervisor, is not
// reaped once it exits, or if any os/exec Wait loses its exit status.
func childOrphans() int {
	const orphans = 20
	supervisor := os.Getppid()

	var wg sync.WaitGroup
	execErrs := make(chan error, 200)
	for range cap(execErrs) {
		wg.Go(func() {
			if err := exec.Command("true").Run(); err != nil {
				execErrs <- err
			}
		})
	}

	pids := make([]int, 0, orphans)
	for range orphans {
		// sh exits as soon as it has backgrounded sleep, orphaning it.
		out, err := exec.Command("sh", "-c", "sleep 0.5 & echo $!").Output()
		if err != nil {
			fmt.Fprintln(os.Stderr, "spawn orphan:", err)
			return 1
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
		if err != nil {
			fmt.Fprintln(os.Stderr, "parse orphan pid:", err)
			return 1
		}
		pids = append(pids, pid)
	}

	wg.Wait()
	close(execErrs)
	for err := range execErrs {
		fmt.Fprintln(os.Stderr, "os/exec child reported failure:", err)
		return 1
	}

	for _, pid := range pids {
		if ppid, err := parentOf(pid); err == nil && ppid != supervisor {
			fmt.Fprintf(os.Stderr, "orphan %d has parent %d, want supervisor %d\n", pid, ppid, supervisor)
			return 1
		}
	}

	// A zombie keeps its /proc entry, so the entry vanishing means it was reaped.
	deadline := time.Now().Add(10 * time.Second)
	for _, pid := range pids {
		for {
			if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); errors.Is(err, os.ErrNotExist) {
				break
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "orphan %d was never reaped\n", pid)
				return 1
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return 0
}

// parentOf reads a process's parent PID from /proc/<pid>/stat. The command name
// field may contain spaces, so parsing starts after its closing parenthesis.
func parentOf(pid int) (int, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	s := string(b)
	fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	if len(fields) < 2 {
		return 0, fmt.Errorf("short stat for pid %d: %q", pid, s)
	}
	return strconv.Atoi(fields[1])
}

// TestSupervise_ReapsOrphansWithoutStealingOsExecStatus covers both halves of
// the contract: every orphan reparented to the init is reaped, and the
// supervised child's own os/exec children still report their true status.
func TestSupervise_ReapsOrphansWithoutStealingOsExecStatus(t *testing.T) {
	cmd := supervisorCommand(t, "child-orphans")
	require.Equal(t, 0, exitCodeOf(t, cmd.Run()))
}
