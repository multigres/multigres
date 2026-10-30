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
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newRoot(t *testing.T) (*Root, string) {
	t.Helper()
	dir := t.TempDir()
	r, err := OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	return r, dir
}

func TestReadFile_Regular(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a", "b", "f"), []byte("hello"), 0o600))

	got, err := r.ReadFile("a/b/f")
	require.NoError(t, err)
	assert.Equal(t, "hello", string(got))
}

func TestReadFile_RejectsSymlinkLeaf(t *testing.T) {
	r, dir := newRoot(t)
	secret := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(secret, []byte("s3cret"), 0o600))
	require.NoError(t, os.Symlink(secret, filepath.Join(dir, "leaf")))
	// A relative link that stays beneath the root is refused too: RESOLVE_BENEATH
	// alone would allow it, so this is what guards RESOLVE_NO_SYMLINKS.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "real"), []byte("x"), 0o600))
	require.NoError(t, os.Symlink("real", filepath.Join(dir, "inner")))

	for _, rel := range []string{"leaf", "inner"} {
		got, err := r.ReadFile(rel)
		require.Error(t, err, rel)
		assert.Empty(t, got, rel)
	}
}

func TestReadFile_RejectsSymlinkParent(t *testing.T) {
	r, dir := newRoot(t)
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("s3cret"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "d")))

	got, err := r.ReadFile("d/secret")
	require.Error(t, err)
	assert.Empty(t, got)
}

func TestReadFile_RejectsSymlinkParentInsideRoot(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(dir, "real"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "real", "f"), []byte("x"), 0o600))
	require.NoError(t, os.Symlink("real", filepath.Join(dir, "d")))

	_, err := r.ReadFile("d/f")
	require.Error(t, err)
}

func TestReadFile_RejectsDotDot(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o600))

	for _, rel := range []string{"../f", "sub/../f", "sub/.."} {
		_, err := r.ReadFile(rel)
		require.Error(t, err, rel)
	}
}

func TestReadFile_RejectsAbsolute(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o600))

	_, err := r.ReadFile(filepath.Join(dir, "f"))
	require.Error(t, err)
}

func TestReadFile_RejectsFIFO(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, mkfifo(filepath.Join(dir, "fifo")))

	_, err := r.ReadFile("fifo")
	require.Error(t, err)
}

func TestWriteFile_Atomic(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(dir, "d"), 0o755))

	require.NoError(t, r.WriteFile("d/f", []byte("one"), 0o640))
	require.NoError(t, r.WriteFile("d/f", []byte("two"), 0o640))

	got, err := r.ReadFile("d/f")
	require.NoError(t, err)
	assert.Equal(t, "two", string(got))
	info, err := os.Stat(filepath.Join(dir, "d", "f"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o640), info.Mode().Perm())

	entries, err := os.ReadDir(filepath.Join(dir, "d"))
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no temp file left behind")
}

func TestWriteFile_RejectsSymlinkParent(t *testing.T) {
	r, dir := newRoot(t)
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "d")))

	require.Error(t, r.WriteFile("d/f", []byte("x"), 0o600))
	_, err := os.Stat(filepath.Join(outside, "f"))
	assert.True(t, os.IsNotExist(err))
}

func TestWriteFile_ReplacesSymlinkNotTarget(t *testing.T) {
	r, dir := newRoot(t)
	target := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(target, []byte("original"), 0o600))
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Symlink(target, link))

	require.NoError(t, r.WriteFile("link", []byte("new"), 0o600))

	got, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, "original", string(got), "symlink target must be untouched")
	info, err := os.Lstat(link)
	require.NoError(t, err)
	assert.True(t, info.Mode().IsRegular(), "path must now be a regular file")
	got, err = os.ReadFile(link)
	require.NoError(t, err)
	assert.Equal(t, "new", string(got))
}

func TestWriteFile_NoTruncateInPlace(t *testing.T) {
	r, dir := newRoot(t)
	path := filepath.Join(dir, "f")
	old := make([]byte, 1<<16)
	for i := range old {
		old[i] = 'a'
	}
	require.NoError(t, os.WriteFile(path, old, 0o600))
	held, err := os.Open(path)
	require.NoError(t, err)
	defer held.Close()

	require.NoError(t, r.WriteFile("f", []byte("short"), 0o600))

	got, err := io.ReadAll(held)
	require.NoError(t, err)
	assert.Equal(t, old, got, "reader holding the old file must still see all of it")
}

func TestRemove(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "d"), 0o755))
	outside := filepath.Join(t.TempDir(), "keep")
	require.NoError(t, os.WriteFile(outside, []byte("x"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "l")))

	require.NoError(t, r.Remove("f"))
	require.NoError(t, r.Remove("d"))
	require.NoError(t, r.Remove("l"))
	assert.NoFileExists(t, filepath.Join(dir, "f"))
	assert.NoDirExists(t, filepath.Join(dir, "d"))
	assert.FileExists(t, outside, "removing a symlink must not remove its target")
	require.Error(t, r.Remove("missing"))
}

func TestRemoveAll_DoesNotFollow(t *testing.T) {
	r, dir := newRoot(t)
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim")
	require.NoError(t, os.WriteFile(victim, []byte("x"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "tree", "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tree", "sub", "f"), []byte("x"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "tree", "escape")))

	require.NoError(t, r.RemoveAll("tree"))

	assert.NoDirExists(t, filepath.Join(dir, "tree"))
	assert.FileExists(t, victim, "RemoveAll must not descend through a symlink")
}

func TestRemoveAll_RejectsSymlinkParent(t *testing.T) {
	r, dir := newRoot(t)
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim")
	require.NoError(t, os.WriteFile(victim, []byte("x"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "d")))

	require.Error(t, r.RemoveAll("d/victim"))
	assert.FileExists(t, victim)
}

func TestRemoveAll_Missing(t *testing.T) {
	r, _ := newRoot(t)
	require.NoError(t, r.RemoveAll("nope"))
	require.NoError(t, r.RemoveAll("nope/deeper"))
}

func TestStat_LstatSemantics(t *testing.T) {
	r, dir := newRoot(t)
	target := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(target, []byte("12345"), 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "l")))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("12345"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "d"), 0o755))

	info, err := r.Stat("l")
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink)

	info, err = r.Stat("f")
	require.NoError(t, err)
	assert.True(t, info.Mode().IsRegular())
	assert.EqualValues(t, 5, info.Size())
	assert.Equal(t, "f", info.Name())

	info, err = r.Stat("d")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	info, err = r.Stat(".")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	_, err = r.Stat("missing")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStat_RejectsSymlinkParent(t *testing.T) {
	r, dir := newRoot(t)
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "f"), []byte("x"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "d")))

	_, err := r.Stat("d/f")
	require.Error(t, err)
}

func TestRoot_HoldsDirectoryAcrossRename(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o600))
	moved := dir + ".moved"
	require.NoError(t, os.Rename(dir, moved))
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	require.NoError(t, os.Mkdir(dir, 0o755))

	got, err := r.ReadFile("f")
	require.NoError(t, err)
	assert.Equal(t, "x", string(got))
}

func TestOpenRoot_RejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "l")
	require.NoError(t, os.Symlink(dir, link))

	_, err := OpenRoot(link)
	require.Error(t, err)
}

func mkfifo(path string) error { return unix.Mkfifo(path, 0o600) }

func TestClose_Idempotent(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o600))

	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
	_, err := r.ReadFile("f")
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestReadFile_RejectsOversize(t *testing.T) {
	r, dir := newRoot(t)
	big := filepath.Join(dir, "big")
	f, err := os.Create(big)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(MaxReadSize+1))
	require.NoError(t, f.Close())
	ok := filepath.Join(dir, "ok")
	f, err = os.Create(ok)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(MaxReadSize))
	require.NoError(t, f.Close())

	_, err = r.ReadFile("big")
	require.ErrorContains(t, err, "exceeds")
	got, err := r.ReadFile("ok")
	require.NoError(t, err)
	assert.Len(t, got, MaxReadSize)
}

func TestReadBounded(t *testing.T) {
	got, err := readBounded(strings.NewReader("12345"), 5)
	require.NoError(t, err)
	assert.Equal(t, "12345", string(got))

	_, err = readBounded(strings.NewReader("123456"), 5)
	require.ErrorIs(t, err, errTooLarge)
}

func TestWriteFile_AppliesPermDespiteUmask(t *testing.T) {
	r, dir := newRoot(t)
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)

	require.NoError(t, r.WriteFile("f", []byte("x"), 0o640))

	info, err := os.Stat(filepath.Join(dir, "f"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o640), info.Mode().Perm())
}

func TestWriteFile_CleansTempOnFailure(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "d", "sub"), 0o755))

	require.Error(t, r.WriteFile("d", []byte("x"), 0o600), "rename over a non-empty directory must fail")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "d", entries[0].Name(), "temp file must not be left behind")
}

func TestRemove_RefusesRoot(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o600))

	for _, rel := range []string{"", ".", "./", "d/.."} {
		require.Error(t, r.Remove(rel), rel)
		require.Error(t, r.RemoveAll(rel), rel)
	}
	assert.FileExists(t, filepath.Join(dir, "f"))
}

func TestRemoveAll_RefusesSwappedDirectory(t *testing.T) {
	r, dir := newRoot(t)
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim")
	require.NoError(t, os.WriteFile(victim, []byte("x"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "tree", "d"), 0o755))

	swapped := false
	testHookRemoveAllRace = func(name string) {
		if name != "d" || swapped {
			return
		}
		swapped = true
		d := filepath.Join(dir, "tree", "d")
		require.NoError(t, os.Remove(d))
		require.NoError(t, os.Symlink(outside, d))
	}
	t.Cleanup(func() { testHookRemoveAllRace = nil })

	require.Error(t, r.RemoveAll("tree"))
	assert.True(t, swapped)
	assert.FileExists(t, victim, "a directory swapped for a symlink must not be descended")
}

func TestRemoveAll_BoundsDepth(t *testing.T) {
	r, dir := newRoot(t)
	deep := filepath.Join(dir, "t", strings.Repeat("a/", maxRemoveDepth+20))
	require.NoError(t, os.MkdirAll(deep, 0o755))

	require.ErrorContains(t, r.RemoveAll("t"), "deeper than")

	shallow := filepath.Join(dir, "s", strings.Repeat("a/", 20))
	require.NoError(t, os.MkdirAll(shallow, 0o755))
	require.NoError(t, r.RemoveAll("s"))
}

// nestedMountRoot returns a root with a mount nested beneath it, without
// needing privileges: a container's /dev holds /dev/shm as a separate tmpfs.
func nestedMountRoot(t *testing.T) (*Root, string) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("mount crossing is only refused on Linux")
	}
	var devSt, shmSt syscall.Stat_t
	if err := syscall.Stat("/dev", &devSt); err != nil {
		t.Skipf("no /dev: %v", err)
	}
	if err := syscall.Stat("/dev/shm", &shmSt); err != nil || devSt.Dev == shmSt.Dev {
		t.Skip("/dev/shm is not a separate mount here, and creating one needs CAP_SYS_ADMIN")
	}
	f, err := os.CreateTemp("/dev/shm", "safefs-xdev-")
	if err != nil {
		t.Skipf("/dev/shm not writable: %v", err)
	}
	require.NoError(t, f.Close())
	t.Cleanup(func() { _ = os.Remove(f.Name()) })
	r, err := OpenRoot("/dev")
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	return r, f.Name()
}

func TestReadFile_RejectsMountCrossing(t *testing.T) {
	r, name := nestedMountRoot(t)

	_, err := r.ReadFile("shm/" + filepath.Base(name))
	require.Error(t, err)
}

func TestRemoveAll_RefusesDirectoryReplacedAfterLstat(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "tree", "d"), 0o755))
	keep := filepath.Join(dir, "tree", "d", "keep")

	swapped := false
	testHookRemoveAllRace = func(name string) {
		if name != "d" || swapped {
			return
		}
		swapped = true
		d := filepath.Join(dir, "tree", "d")
		// Renaming (not removing) the original keeps its inode allocated, so
		// the replacement cannot reuse the number and hide the swap.
		require.NoError(t, os.Rename(d, d+".old"))
		require.NoError(t, os.Mkdir(d, 0o755))
		require.NoError(t, os.WriteFile(keep, []byte("x"), 0o600))
	}
	t.Cleanup(func() { testHookRemoveAllRace = nil })

	require.ErrorIs(t, r.RemoveAll("tree"), errNotInRoot)
	assert.True(t, swapped)
	assert.FileExists(t, keep, "a directory that is not the one checked must not be emptied")
}
