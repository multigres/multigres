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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// These tests must run in a disposable mount namespace/container: both sides
// of each bind mount are test-created directories, never host victim data.
func TestRemoveAll_RefusesSameDeviceBindMount(t *testing.T) {
	for _, mountRel := range []string{"tree", "tree/sub/mount"} {
		t.Run(mountRel, func(t *testing.T) {
			r, dir := newRoot(t)
			outside := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(outside, "sub"), 0o755))
			victim := filepath.Join(outside, "sub", "victim")
			require.NoError(t, os.WriteFile(victim, []byte("untouched"), 0o600))
			mnt := filepath.Join(dir, mountRel)
			require.NoError(t, os.MkdirAll(mnt, 0o755))
			if err := unix.Mount(outside, mnt, "", unix.MS_BIND, ""); err != nil {
				if os.Getenv("SAFEFS_REQUIRE_MOUNT_TESTS") == "1" {
					t.Fatalf("bind mount required: %v", err)
				}
				t.Skipf("bind mount needs CAP_SYS_ADMIN: %v", err)
			}
			t.Cleanup(func() { require.NoError(t, unix.Unmount(mnt, unix.MNT_DETACH)) })
			var rootStat, mountStat unix.Stat_t
			require.NoError(t, unix.Stat(dir, &rootStat))
			require.NoError(t, unix.Stat(mnt, &mountStat))
			require.Equal(t, rootStat.Dev, mountStat.Dev, "regression requires the same device")

			// Both a leaf mount and an intermediate mount must be refused.
			for _, rel := range []string{mountRel, mountRel + "/sub"} {
				child, err := r.OpenRoot(rel)
				require.ErrorIs(t, err, unix.EXDEV)
				assert.Nil(t, child)
			}
			err := r.RemoveAll("tree")
			require.Error(t, err)
			// Check contents even if the old implementation returns EBUSY only
			// after emptying the mount. An error alone is not safety evidence.
			got, readErr := os.ReadFile(victim)
			require.NoError(t, readErr)
			assert.Equal(t, "untouched", string(got))
			require.ErrorIs(t, err, unix.EXDEV)
		})
	}
}

// A regression in the mount-crossing guard makes RemoveAll empty the nested
// mount, so the mount here is a tmpfs this test creates, never a host
// directory such as /dev/shm. Mounting needs CAP_SYS_ADMIN.
func TestRemoveAll_RefusesMountCrossing(t *testing.T) {
	r, dir := newRoot(t)
	mnt := filepath.Join(dir, "m")
	require.NoError(t, os.Mkdir(mnt, 0o755))
	if err := unix.Mount("tmpfs", mnt, "tmpfs", 0, ""); err != nil {
		t.Skipf("cannot mount a tmpfs (needs CAP_SYS_ADMIN, run privileged): %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(mnt, unix.MNT_DETACH) })
	victim := filepath.Join(mnt, "victim")
	require.NoError(t, os.WriteFile(victim, []byte("x"), 0o600))

	require.Error(t, r.RemoveAll("m"))
	assert.FileExists(t, victim, "RemoveAll must not empty a nested mount")
}
