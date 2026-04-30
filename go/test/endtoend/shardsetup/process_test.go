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

package shardsetup

import (
	"context"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func TestPauseAndResumeProcess(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = executil.KillPID(ctx, cmd.Process.Pid)
	})

	require.NoError(t, pauseProcess(cmd.Process.Pid))
	// Give the kernel a moment to deliver SIGSTOP.
	time.Sleep(50 * time.Millisecond)
	// Signal 0 confirms the pid still exists. (A stopped process is still a
	// process; we cannot portably check stopped-state on macOS without /proc.)
	require.NoError(t, syscall.Kill(cmd.Process.Pid, 0))

	require.NoError(t, resumeProcess(cmd.Process.Pid))
}
