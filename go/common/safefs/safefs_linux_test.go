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
