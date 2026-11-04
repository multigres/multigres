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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func becomeSubreaper() error { return unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) }

// Only this supervisor reaps these children. Between enumeration and signal a
// child can exit, but remains a zombie (reserving its PID), so no unrelated PID
// can be signalled. Killing a direct child reparents its descendants here;
// subsequent passes handle them even if they double-forked or called setsid.
func signalOwnedChildren(_ int, sig syscall.Signal) error {
	tasks, err := filepath.Glob("/proc/self/task/*/children")
	if err != nil {
		return fmt.Errorf("cannot enumerate owned children: %w", err)
	}
	if len(tasks) == 0 {
		return errors.New("cannot enumerate owned children: no task lists")
	}
	for _, path := range tasks {
		b, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		} // exited Go runtime thread
		if err != nil {
			return err
		}
		for v := range strings.FieldsSeq(string(b)) {
			pid, err := strconv.Atoi(v)
			if err != nil || pid <= 0 {
				return fmt.Errorf("invalid child pid %q", v)
			}
			if err := syscall.Kill(pid, sig); err != nil && err != syscall.ESRCH {
				return err
			}
		}
	}
	return nil
}

// On Linux ECHILD in a subreaper proves the whole invocation tree is empty.
func portableGroupGone(int) bool { return true }

// The daemon is commonly stopped by signalling its whole process group.
// Its subreapers must survive that signal to consume lifetime EOF, terminate
// and reap owned descendants, and record quiescence. A separate group isolates
// the helper from daemon group-stop/escalation; no Pdeathsig is installed.
func supervisorSysProcAttr() *syscall.SysProcAttr      { return &syscall.SysProcAttr{Setpgid: true} }
func supervisedChildSysProcAttr() *syscall.SysProcAttr { return &syscall.SysProcAttr{Setpgid: true} }
