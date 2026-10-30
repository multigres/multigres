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

// Package safefs reads and writes files beneath a directory without ever
// following a symlink.
//
// It exists because postgres can plant symlinks on a data volume that is
// shared with another container. A path resolved by that container follows
// the link inside its own mount namespace, where its own secrets live, so
// every access to the shared volume goes through a Root that refuses
// symlinks, "..", and magic links in every component.
package safefs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Root is a directory that all access is confined to. It holds a descriptor
// on the directory, so renaming or replacing the directory path after
// OpenRoot does not redirect later accesses.
type Root struct {
	// mu makes Close safe against concurrent use and idempotent: a second
	// Close must not close whatever descriptor number was reused since.
	mu  sync.RWMutex
	fd  int // -1 once closed
	dev int64
}

// MaxReadSize caps ReadFile. A file on a shared volume is attacker-sized,
// and a sparse multi-terabyte file would otherwise exhaust the reader's memory.
const MaxReadSize = 64 << 20

// maxRemoveDepth bounds RemoveAll recursion, and so the descriptors it holds
// open at once, against a tree built (or extended mid-walk) to exhaust them.
const maxRemoveDepth = 256

var errNotInRoot = errors.New("directory is not the one that was checked, or is on another filesystem")

// testHookRemoveAllRace runs between the lstat and the O_NOFOLLOW open of a
// directory in RemoveAll, so a test can swap the directory for a symlink in
// exactly the window the flag exists to close.
var testHookRemoveAllRace func(name string)

// OpenRoot opens dir as a Root. dir itself must not be a symlink, but its
// parent components are resolved normally, since the caller chose them: dir
// must therefore be a mount root, or have a parent chain no untrusted party
// can rename. safefs does not verify mount-ness; anchoring dir to a mount
// root is the caller's job (sandbox.OpenAnchor).
func OpenRoot(dir string) (*Root, error) {
	fd, err := openRootFD(dir)
	if err != nil {
		return nil, &os.PathError{Op: "openroot", Path: dir, Err: err}
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		_ = unix.Close(fd)
		return nil, &os.PathError{Op: "openroot", Path: dir, Err: err}
	}
	return &Root{fd: fd, dev: int64(st.Dev)}, nil
}

// Close releases the root descriptor. Calling it again is a no-op.
func (r *Root) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fd < 0 {
		return nil
	}
	err := unix.Close(r.fd)
	r.fd = -1
	return err
}

// hold returns the root descriptor and a release func that must be called
// when the operation is done, so Close cannot pull the descriptor away
// mid-operation.
func (r *Root) hold(op, rel string) (int, func(), error) {
	r.mu.RLock()
	if r.fd < 0 {
		r.mu.RUnlock()
		return -1, nil, &os.PathError{Op: op, Path: rel, Err: os.ErrClosed}
	}
	return r.fd, r.mu.RUnlock, nil
}

// ReadFile reads rel beneath the root. Any symlink in any component, any
// "..", or any magic link fails.
func (r *Root) ReadFile(rel string) ([]byte, error) {
	comps, err := splitRel("read", rel)
	if err != nil {
		return nil, err
	}
	rootfd, release, err := r.hold("read", rel)
	if err != nil {
		return nil, err
	}
	// O_NONBLOCK so a FIFO planted at rel fails the regular-file check below
	// instead of blocking the open forever.
	fd, err := openBeneath(rootfd, comps, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	// The opened fd does not depend on the root fd, so the lock is not held
	// across the read: a pending Close would otherwise stall every new operation.
	release()
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: rel, Err: err}
	}
	f := os.NewFile(uintptr(fd), rel)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: rel, Err: err}
	}
	if !info.Mode().IsRegular() {
		return nil, &os.PathError{Op: "read", Path: rel, Err: errors.New("not a regular file")}
	}
	if info.Size() > MaxReadSize {
		return nil, &os.PathError{Op: "read", Path: rel, Err: errTooLarge}
	}
	// The file can grow after the fstat, so the read is bounded as well.
	data, err := readBounded(f, MaxReadSize)
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: rel, Err: err}
	}
	return data, nil
}

var errTooLarge = fmt.Errorf("file exceeds %d bytes", MaxReadSize)

func readBounded(rd io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(rd, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errTooLarge
	}
	return data, nil
}

// WriteFile writes rel atomically: a temp file in the same directory, fsync,
// rename over rel, fsync the dir. It never truncates rel in place, and it
// replaces a symlink at rel rather than following it.
func (r *Root) WriteFile(rel string, data []byte, perm os.FileMode) error {
	dirfd, base, err := r.openParent("write", rel)
	if err != nil {
		return err
	}
	defer unix.Close(dirfd)

	tmp, err := tempName()
	if err != nil {
		return &os.PathError{Op: "write", Path: rel, Err: err}
	}
	fd, err := unix.Openat(dirfd, tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(perm.Perm()))
	if err != nil {
		return &os.PathError{Op: "write", Path: rel, Err: err}
	}
	f := os.NewFile(uintptr(fd), tmp)
	committed := false
	defer func() {
		if !committed {
			_ = unix.Unlinkat(dirfd, tmp, 0)
		}
	}()

	// Chmod rather than trusting the open mode, which the umask trims.
	if err := f.Chmod(perm.Perm()); err != nil {
		_ = f.Close()
		return &os.PathError{Op: "write", Path: rel, Err: err}
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return &os.PathError{Op: "write", Path: rel, Err: err}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return &os.PathError{Op: "write", Path: rel, Err: err}
	}
	if err := f.Close(); err != nil {
		return &os.PathError{Op: "write", Path: rel, Err: err}
	}
	if err := unix.Renameat(dirfd, tmp, dirfd, base); err != nil {
		return &os.PathError{Op: "write", Path: rel, Err: err}
	}
	committed = true
	if err := unix.Fsync(dirfd); err != nil {
		return &os.PathError{Op: "write", Path: rel, Err: err}
	}
	return nil
}

// Remove removes the file or empty directory at rel. A symlink at rel is
// removed, not followed.
func (r *Root) Remove(rel string) error {
	dirfd, base, err := r.openParent("remove", rel)
	if err != nil {
		return err
	}
	defer unix.Close(dirfd)

	err = unix.Unlinkat(dirfd, base, 0)
	if err == nil {
		return nil
	}
	// Linux reports EISDIR and BSD EPERM for unlink on a directory.
	if errors.Is(err, unix.EISDIR) || errors.Is(err, unix.EPERM) {
		if derr := unix.Unlinkat(dirfd, base, unix.AT_REMOVEDIR); derr == nil {
			return nil
		} else if !errors.Is(derr, unix.ENOTDIR) {
			err = derr
		}
	}
	return &os.PathError{Op: "remove", Path: rel, Err: err}
}

// RemoveAll removes rel and everything beneath it. It does not follow
// symlinks, and a missing rel is not an error. On error, part of the tree may
// already be removed.
func (r *Root) RemoveAll(rel string) error {
	dirfd, base, err := r.openParent("removeall", rel)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(dirfd)
	if err := removeAllAt(dirfd, base, r.dev, 0); err != nil {
		return &os.PathError{Op: "removeall", Path: rel, Err: err}
	}
	return nil
}

// Stat returns info about rel with lstat semantics: a symlink at rel is
// described, not followed. Intermediate components still may not be symlinks.
func (r *Root) Stat(rel string) (os.FileInfo, error) {
	comps, err := splitRel("stat", rel)
	if err != nil {
		return nil, err
	}
	rootfd, release, err := r.hold("stat", rel)
	if err != nil {
		return nil, err
	}
	defer release()
	var st unix.Stat_t
	name := "."
	if len(comps) == 0 {
		err = unix.Fstat(rootfd, &st)
	} else {
		var dirfd int
		dirfd, err = openBeneath(rootfd, comps[:len(comps)-1], unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, &os.PathError{Op: "stat", Path: rel, Err: err}
		}
		defer unix.Close(dirfd)
		name = comps[len(comps)-1]
		err = unix.Fstatat(dirfd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	}
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: rel, Err: err}
	}
	return &fileInfo{name: name, st: st}, nil
}

// openParent opens the directory containing rel and returns it with rel's
// final component. The directory is opened O_RDONLY, not O_PATH, because the
// caller needs to fsync it.
func (r *Root) openParent(op, rel string) (int, string, error) {
	comps, err := splitRel(op, rel)
	if err != nil {
		return -1, "", err
	}
	if len(comps) == 0 {
		return -1, "", &os.PathError{Op: op, Path: rel, Err: errors.New("path names the root")}
	}
	rootfd, release, err := r.hold(op, rel)
	if err != nil {
		return -1, "", err
	}
	defer release()
	fd, err := openBeneath(rootfd, comps[:len(comps)-1], unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, "", &os.PathError{Op: op, Path: rel, Err: err}
	}
	return fd, comps[len(comps)-1], nil
}

// splitRel validates rel and returns its components. Absolute paths and any
// ".." component are rejected here, since openat2 would still accept a ".."
// that stays beneath the root and the fallback walk has no such check.
// Empty and "." components are dropped.
func splitRel(op, rel string) ([]string, error) {
	if strings.ContainsRune(rel, 0) {
		return nil, &os.PathError{Op: op, Path: rel, Err: fs.ErrInvalid}
	}
	if strings.HasPrefix(rel, "/") {
		return nil, &os.PathError{Op: op, Path: rel, Err: errors.New("absolute path")}
	}
	var comps []string
	for c := range strings.SplitSeq(rel, "/") {
		switch c {
		case "", ".":
		case "..":
			return nil, &os.PathError{Op: op, Path: rel, Err: errors.New("path contains ..")}
		default:
			comps = append(comps, c)
		}
	}
	return comps, nil
}

func removeAllAt(dirfd int, name string, dev int64, depth int) error {
	if depth > maxRemoveDepth {
		return fmt.Errorf("directory tree deeper than %d levels", maxRemoveDepth)
	}
	err := unix.Unlinkat(dirfd, name, 0)
	if err == nil || errors.Is(err, unix.ENOENT) {
		return nil
	}
	var st unix.Stat_t
	if serr := unix.Fstatat(dirfd, name, &st, unix.AT_SYMLINK_NOFOLLOW); serr != nil {
		if errors.Is(serr, unix.ENOENT) {
			return nil
		}
		return serr
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return err
	}
	// A mount nested under the root would be emptied through it, so refuse to
	// cross onto another filesystem.
	if int64(st.Dev) != dev {
		return errNotInRoot
	}
	if testHookRemoveAllRace != nil {
		testHookRemoveAllRace(name)
	}

	// O_NOFOLLOW closes the window between the lstat above and this open: a
	// directory swapped for a symlink in between is refused, not descended.
	fd, err := unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	// The lstat above can be stale by now: a mountpoint (or another directory)
	// renamed into place in the window is opened as the mount's root. Check the
	// directory actually opened before unlinking anything inside it.
	var ost unix.Stat_t
	if err := unix.Fstat(fd, &ost); err != nil {
		_ = unix.Close(fd)
		return err
	}
	if int64(ost.Dev) != dev || ost.Ino != st.Ino {
		_ = unix.Close(fd)
		return errNotInRoot
	}
	d := os.NewFile(uintptr(fd), name)
	names, err := d.Readdirnames(-1)
	if err != nil {
		_ = d.Close()
		return err
	}
	for _, n := range names {
		if err := removeAllAt(fd, n, dev, depth+1); err != nil {
			_ = d.Close()
			return err
		}
	}
	if err := d.Close(); err != nil {
		return err
	}
	if err := unix.Unlinkat(dirfd, name, unix.AT_REMOVEDIR); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	return nil
}

func tempName() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("temp name: %w", err)
	}
	return ".safefs-" + hex.EncodeToString(b[:]) + ".tmp", nil
}

type fileInfo struct {
	name string
	st   unix.Stat_t
}

func (i *fileInfo) Name() string       { return i.name }
func (i *fileInfo) Size() int64        { return i.st.Size }
func (i *fileInfo) ModTime() time.Time { return time.Unix(i.st.Mtim.Sec, i.st.Mtim.Nsec) }
func (i *fileInfo) IsDir() bool        { return i.Mode().IsDir() }
func (i *fileInfo) Sys() any           { return &i.st }

func (i *fileInfo) Mode() fs.FileMode {
	m := fs.FileMode(i.st.Mode & 0o777)
	switch i.st.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		m |= fs.ModeDir
	case unix.S_IFLNK:
		m |= fs.ModeSymlink
	case unix.S_IFIFO:
		m |= fs.ModeNamedPipe
	case unix.S_IFSOCK:
		m |= fs.ModeSocket
	case unix.S_IFCHR:
		m |= fs.ModeDevice | fs.ModeCharDevice
	case unix.S_IFBLK:
		m |= fs.ModeDevice
	}
	if i.st.Mode&unix.S_ISUID != 0 {
		m |= fs.ModeSetuid
	}
	if i.st.Mode&unix.S_ISGID != 0 {
		m |= fs.ModeSetgid
	}
	if i.st.Mode&unix.S_ISVTX != 0 {
		m |= fs.ModeSticky
	}
	return m
}
