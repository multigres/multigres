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
	"bufio"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHelperProcess is the parent in the test below.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("PGBACKRESTD_HELPER") != "1" {
		t.Skip("helper for the parent-death test")
	}
	runHelperParent()
}

func TestChildSysProcAttr_Pdeathsig(t *testing.T) {
	assert.Equal(t, syscall.SIGKILL, childSysProcAttr().Pdeathsig)
}

// runHelperParent forks a long sleep the way runChild does and then waits.
func runHelperParent() {
	runtime.LockOSThread()
	cmd := exec.Command("sleep", "300")
	cmd.SysProcAttr = childSysProcAttr()
	if err := cmd.Start(); err != nil {
		os.Exit(2)
	}
	os.Stdout.WriteString(strconv.Itoa(cmd.Process.Pid) + "\n")
	time.Sleep(time.Hour)
}

// A sidecar that dies must take its children with it: nothing may keep writing
// the data directory under a restarted sidecar that knows nothing of it.
func TestChildSysProcAttr_KillsChildWhenParentDies(t *testing.T) {
	self, err := os.Executable()
	require.NoError(t, err)
	parent := exec.Command(self, "-test.run=^TestHelperProcess$")
	parent.Env = append(os.Environ(), "PGBACKRESTD_HELPER=1")
	out, err := parent.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, parent.Start())
	t.Cleanup(func() { _ = parent.Process.Kill(); _ = parent.Wait() })

	line, err := bufio.NewReader(out).ReadString('\n')
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, 0), "the child is running")

	require.NoError(t, parent.Process.Kill())
	_ = parent.Wait()
	require.Eventually(t, func() bool { return syscall.Kill(pid, 0) == syscall.ESRCH }, 5*time.Second, 20*time.Millisecond,
		"the kernel must kill the child when its parent dies")
}
