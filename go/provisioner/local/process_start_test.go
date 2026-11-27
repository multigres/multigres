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

package local

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func startSleeper(t *testing.T) int {
	t.Helper()
	cmd := executil.Command(context.Background(), "sleep", "300")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _, _ = cmd.Stop(context.Background()); _ = cmd.Wait() })
	return cmd.Process.Pid
}

// The start time is what tells a recorded sidecar from another process that
// later got its PID, so it has to be the same on every reading of one process
// and different for a process started at another time.
func TestProcessStartTime(t *testing.T) {
	first := startSleeper(t)
	started, err := processStartTime(first)
	require.NoError(t, err)
	require.Positive(t, started)

	// Longer than a clock tick, the coarsest unit a platform reports.
	time.Sleep(50 * time.Millisecond)
	second := startSleeper(t)
	later, err := processStartTime(second)
	require.NoError(t, err)
	require.Greater(t, later, started, "a process started later has a later start time")

	for range 3 {
		again, err := processStartTime(first)
		require.NoError(t, err)
		require.Equal(t, started, again, "one process, one start time")
	}

	identity, err := readSidecarIdentity(os.Getpid())
	require.NoError(t, err)
	own, err := processStartTime(os.Getpid())
	require.NoError(t, err)
	require.Equal(t, own, identity.StartTime, "the identity carries the kernel's value unchanged")
}

func TestProcessStartTime_ExitedProcess(t *testing.T) {
	cmd := executil.Command(context.Background(), "true")
	require.NoError(t, cmd.Start())
	require.NoError(t, cmd.Wait())
	_, err := processStartTime(cmd.Process.Pid)
	require.Error(t, err, "a reaped process has no start time to read")
}
