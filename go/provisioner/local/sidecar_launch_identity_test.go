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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLaunchedSidecarIdentitySettles(t *testing.T) {
	expected := []string{"sidecar", "serve"}
	parent := []string{"launcher", "start"}
	for _, mode := range []string{"empty", "parent", "already-matching"} {
		t.Run(mode, func(t *testing.T) {
			initial := &sidecarOwner{PID: 42, Created: 123}
			switch mode {
			case "parent":
				initial.Argv = parent
			case "already-matching":
				initial.Argv = expected
			}
			calls := 0
			owner, err := waitForLaunchedSidecarIdentity(t.Context(), initial, expected, parent, make(chan struct{}), func(pid int) (*sidecarOwner, error) {
				require.Equal(t, initial.PID, pid)
				calls++
				// Both transient states may be observed before the same child
				// exposes its final argv. No partial observation is accepted.
				argv := parent
				if calls == 2 {
					argv = expected
				}
				return &sidecarOwner{PID: pid, Created: initial.Created, Argv: argv}, nil
			})
			require.NoError(t, err)
			require.Equal(t, expected, owner.Argv)
			require.Equal(t, initial.PID, owner.PID)
			require.Equal(t, initial.Created, owner.Created)
			if mode == "already-matching" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 2, calls)
			}
		})
	}
}

func TestLaunchedSidecarIdentityRefusesUncertainty(t *testing.T) {
	expected := []string{"sidecar", "serve"}
	parent := []string{"launcher", "start"}
	inspectionErr := errors.New("inspection denied")
	for _, mode := range []string{"changed-birth", "changed-pid", "unrelated-argv", "partial-argv", "inspection-error", "nil-observation", "exited-before-read", "exited-with-matching-read", "canceled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			initial := &sidecarOwner{PID: 42, Created: 123}
			exited := make(chan struct{})
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if mode == "exited-before-read" {
				initial.Argv = expected
				close(exited)
			}
			if mode == "canceled" {
				cancel()
			}
			calls := 0
			owner, err := waitForLaunchedSidecarIdentity(ctx, initial, expected, parent, exited, func(pid int) (*sidecarOwner, error) {
				calls++
				next := &sidecarOwner{PID: pid, Created: initial.Created, Argv: expected}
				switch mode {
				case "changed-birth":
					next.Created++
				case "changed-pid":
					next.PID++
				case "unrelated-argv":
					next.Argv = []string{"unrelated", "serve"}
				case "partial-argv":
					next.Argv = expected[:1]
				case "inspection-error":
					return nil, inspectionErr
				case "nil-observation":
					return nil, nil
				case "exited-with-matching-read":
					close(exited)
				case "deadline":
					next.Argv = nil
				}
				return next, nil
			})
			require.Error(t, err)
			require.Nil(t, owner)
			switch mode {
			case "changed-birth", "changed-pid", "nil-observation":
				require.ErrorIs(t, err, errSidecarIdentityChanged)
			case "inspection-error":
				require.ErrorIs(t, err, inspectionErr)
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
			case "deadline":
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case "exited-before-read", "exited-with-matching-read":
				require.ErrorContains(t, err, "sidecar exited")
			default:
				require.ErrorContains(t, err, "does not match command")
			}
			if mode == "exited-before-read" || mode == "canceled" {
				require.Zero(t, calls)
			} else if mode != "deadline" {
				require.Equal(t, 1, calls, "uncertain identity must not be retried")
			}
		})
	}
}

func TestLaunchedSidecarIdentityRequiresBirthAnchor(t *testing.T) {
	for _, initial := range []*sidecarOwner{nil, {PID: 42}, {Created: 123}} {
		owner, err := waitForLaunchedSidecarIdentity(t.Context(), initial, []string{"sidecar", "serve"}, os.Args, make(chan struct{}), func(int) (*sidecarOwner, error) {
			t.Fatal("must not inspect an unanchored PID")
			return nil, nil
		})
		require.Error(t, err)
		require.Nil(t, owner)
	}
}

func TestSidecarEarlyExitNeverCompletesOwnership(t *testing.T) {
	paths := ownerTestPaths(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	pid, err := paths.ensureSidecar(ctx, "/bin/sh", []string{"-c", "exit 7"})
	require.Error(t, err)
	require.Zero(t, pid)
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	if owner != nil {
		// A matching identity may have been durably recorded before exit;
		// it must remain pending, never a healthy adopted instance.
		require.True(t, owner.InstancePending)
		require.Empty(t, owner.InstanceID)
		alive, err := owner.alive()
		require.NoError(t, err)
		require.False(t, alive)
	}
	require.Eventually(t, func() bool {
		_, live := localSidecarChildren.Load(paths.ownerFile())
		return !live
	}, time.Second, time.Millisecond, "failed launch must be reaped")
}
