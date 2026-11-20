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
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func TestOwnedChildrenProbeUsesKernelChildLists(t *testing.T) {
	child := executil.Command(t.Context(), "/bin/sleep", "60")
	require.NoError(t, child.Start())
	t.Cleanup(func() { _, _ = child.Kill(t.Context()); _ = child.Wait() })
	// Signal zero traverses the real per-thread procfs child lists without
	// stopping a test runner's other children. The argument is intentionally
	// unrelated: Linux ownership is derived from the kernel, not a stored PID.
	require.NoError(t, signalOwnedChildren(-1, 0))
	require.NoError(t, syscall.Kill(child.Process.Pid, 0))
	// An invalid signal must propagate an error, not report quiescence.
	require.ErrorIs(t, signalOwnedChildren(-1, syscall.Signal(999)), syscall.EINVAL)
	require.NoError(t, syscall.Kill(child.Process.Pid, 0))
	require.True(t, portableGroupGone(-1), "Linux uses ECHILD from the subreaper, not process-group probing")
}
