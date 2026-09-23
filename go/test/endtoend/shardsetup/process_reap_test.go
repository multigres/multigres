// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardsetup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

// TestStartAndReap_ExitCodeDetectedWithoutRacing exercises the exact pattern
// waitForStartup uses: startAndReap's background reaper running concurrently
// with repeated ExitCode()/IsRunningOrZombie() polling, the same shape that
// previously read Process.ProcessState directly and raced. Run with -race to
// confirm those reads are actually synchronized, not just correct by luck.
func TestStartAndReap_ExitCodeDetectedWithoutRacing(t *testing.T) {
	cmd := executil.Command(context.Background(), "sh", "-c", "sleep 0.05; exit 7")

	require.NoError(t, startAndReap(cmd))

	// Poll the same way waitForStartup does, concurrently with the
	// background reaper spawned by startAndReap.
	var (
		code   int
		exited bool
	)
	require.Eventually(t, func() bool {
		code, exited = cmd.ExitCode()
		return exited
	}, 2*time.Second, time.Millisecond, "process never reported as exited")

	require.Equal(t, 7, code)
	require.False(t, cmd.IsRunningOrZombie(), "process should not report running/zombie after exit is observed")
}

// TestStartAndReap_StillRunningReportsNotExited verifies ExitCode/
// IsRunningOrZombie behave correctly for a process that hasn't exited yet,
// with the background reaper already racing against these checks.
func TestStartAndReap_StillRunningReportsNotExited(t *testing.T) {
	cmd := executil.Command(context.Background(), "sleep", "5")
	t.Cleanup(func() {
		_, _ = cmd.Stop(context.Background())
	})

	require.NoError(t, startAndReap(cmd))

	if _, exited := cmd.ExitCode(); exited {
		t.Fatal("expected exited=false for a still-running process")
	}
	require.True(t, cmd.IsRunningOrZombie())
}
