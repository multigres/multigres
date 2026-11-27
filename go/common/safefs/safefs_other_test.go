//go:build !linux

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

package safefs

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A process that walks directories here also forks helpers, and a child
// inherits every descriptor that lacks close-on-exec at the instant of the
// fork. The walk must therefore never hold a directory descriptor without the
// flag, not even between two system calls: dup(2) clears it.
func TestOpenBeneath_NeverHoldsAnInheritableDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "a", "b", "c"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a", "b", "c", "f"), []byte("x"), 0o600))
	root, err := OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()

	var stop atomic.Bool
	var inheritable atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		for !stop.Load() {
			for fd := 3; fd < 64; fd++ {
				flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
				if err != nil || flags&unix.FD_CLOEXEC != 0 {
					continue
				}
				var st unix.Stat_t
				if unix.Fstat(fd, &st) == nil && st.Mode&unix.S_IFMT == unix.S_IFDIR {
					inheritable.Add(1)
				}
			}
		}
	})
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, err := root.ReadFile("a/b/c/f")
		require.NoError(t, err)
		_, err = root.Stat("a/b")
		require.NoError(t, err)
	}
	stop.Store(true)
	wg.Wait()
	require.Zero(t, inheritable.Load(), "a directory descriptor without close-on-exec was observed during a walk")
}
