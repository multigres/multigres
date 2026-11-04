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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestStat_MetadataMatchesOS(t *testing.T) {
	r, dir := newRoot(t)
	path := filepath.Join(dir, "f")
	require.NoError(t, os.WriteFile(path, []byte("metadata"), 0o640))
	want, err := os.Lstat(path)
	require.NoError(t, err)
	got, err := r.Stat("f")
	require.NoError(t, err)
	assert.Equal(t, want.Name(), got.Name())
	assert.Equal(t, want.Size(), got.Size())
	assert.Equal(t, want.Mode(), got.Mode())
	assert.True(t, want.ModTime().Equal(got.ModTime()))
	st, ok := got.Sys().(*unix.Stat_t)
	require.True(t, ok)
	assert.Equal(t, want.Size(), st.Size)
}

// Synthetic stat results cover special files without creating device nodes or
// relying on the host filesystem allowing set-id bits.
func TestFileInfo_Mode(t *testing.T) {
	for _, tt := range []struct {
		name string
		st   unix.Stat_t
		want fs.FileMode
	}{
		{"regular", unix.Stat_t{Mode: unix.S_IFREG}, 0},
		{"directory", unix.Stat_t{Mode: unix.S_IFDIR}, fs.ModeDir},
		{"symlink", unix.Stat_t{Mode: unix.S_IFLNK}, fs.ModeSymlink},
		{"fifo", unix.Stat_t{Mode: unix.S_IFIFO}, fs.ModeNamedPipe},
		{"socket", unix.Stat_t{Mode: unix.S_IFSOCK}, fs.ModeSocket},
		{"character", unix.Stat_t{Mode: unix.S_IFCHR}, fs.ModeDevice | fs.ModeCharDevice},
		{"block", unix.Stat_t{Mode: unix.S_IFBLK}, fs.ModeDevice},
		{"setuid", unix.Stat_t{Mode: unix.S_IFREG | unix.S_ISUID}, fs.ModeSetuid},
		{"setgid", unix.Stat_t{Mode: unix.S_IFREG | unix.S_ISGID}, fs.ModeSetgid},
		{"sticky", unix.Stat_t{Mode: unix.S_IFDIR | unix.S_ISVTX}, fs.ModeDir | fs.ModeSticky},
	} {
		t.Run(tt.name, func(t *testing.T) {
			info := &fileInfo{st: tt.st}
			info.st.Mode |= 0o640
			assert.Equal(t, tt.want|0o640, info.Mode())
			assert.Equal(t, tt.want.IsDir(), info.IsDir())
		})
	}
}

func TestStat_RejectsInvalidAndClosedRoot(t *testing.T) {
	r, _ := newRoot(t)
	_, err := r.Stat("bad\x00path")
	require.ErrorIs(t, err, fs.ErrInvalid)
	require.NoError(t, r.Close())
	_, err = r.Stat("f")
	require.ErrorIs(t, err, os.ErrClosed)
	require.ErrorIs(t, r.WriteFile("f", []byte("x"), 0o600), os.ErrClosed)
}

func TestReadBounded_PropagatesReadError(t *testing.T) {
	want := errors.New("read failed")
	got, err := readBounded(iotest.ErrReader(want), 5)
	require.ErrorIs(t, err, want)
	assert.Nil(t, got, "a failed read must not return partial contents")
}

func TestRemove_NonemptyDirectoryPreserved(t *testing.T) {
	r, dir := newRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(dir, "d"), 0o755))
	require.NoError(t, r.WriteFile("d/keep", []byte("untouched"), 0o600))
	require.ErrorIs(t, r.Remove("d"), unix.ENOTEMPTY)
	got, err := r.ReadFile("d/keep")
	require.NoError(t, err)
	assert.Equal(t, "untouched", string(got))
}
