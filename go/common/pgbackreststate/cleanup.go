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

// Package pgbackreststate defines the private pgbackrestd supervision-state
// contract shared by the daemon and lifecycle callers. It does not stop any
// process, infer liveness from PIDs, or remove/retire ownership evidence.
package pgbackreststate

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	InstanceLockName = "instance.lock"
	protocolStamp    = "multigres-pgbackrestd-owned-workers-v1\n"
)

var (
	ErrStateInUse       = errors.New("pgbackrestd state is still owned")
	ErrUnverifiedState  = errors.New("pgbackrestd supervision state is missing or unrecognized")
	ErrWorkersUnsettled = errors.New("pgbackrestd worker quiescence is unproven")
)

// IsPendingWorker identifies active ownership evidence. Even a malformed file,
// symlink or directory with this name is a cleanup veto. Audit records below
// recovered-workers are not active records.
func IsPendingWorker(name string) bool {
	return strings.HasPrefix(name, "worker-") && strings.HasSuffix(name, ".pending")
}

// ValidateServingProtocol is a startup compatibility check under the daemon's
// exclusive lock. Empty means uninitialized and is allowed ONLY for startup.
// This is NOT a quiescence check; cleanup must use AcquireCleanupGuard instead.
func ValidateServingProtocol(lock *os.File) error {
	if lock == nil {
		return ErrUnverifiedState
	}
	info, err := lock.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return ErrUnverifiedState
	}
	if info.Size() == 0 {
		return nil
	}
	return verifyStamp(lock)
}

// RecordSupervision is for daemon startup ONLY, after acquiring its exclusive
// instance lock and resolving previous worker ownership. It initializes the
// protocol stamp on that SAME inode (never replaces the lock), then syncs both
// file and directory. A known stamp is accepted; unknown nonempty versions are
// refused. Lifecycle cleanup callers MUST NOT call this to turn unverified or
// legacy state into permission to delete it.
func RecordSupervision(stateDir string, lock *os.File) error {
	if lock == nil {
		return ErrUnverifiedState
	}
	info, err := lock.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return ErrUnverifiedState
	}
	if info.Size() == 0 {
		n, err := lock.WriteAt([]byte(protocolStamp), 0)
		if err != nil {
			return err
		}
		if n != len(protocolStamp) {
			return io.ErrShortWrite
		}
	} else if err := verifyStamp(lock); err != nil {
		return err
	}
	if err := lock.Sync(); err != nil {
		return err
	}
	dir, err := os.Open(stateDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// Underlying inspection errors are rendered, not wrapped: in particular,
// os.ErrNotExist must never turn unverified evidence into already-clean success.
func verifyStamp(lock *os.File) error {
	info, err := lock.Stat()
	if err != nil {
		return fmt.Errorf("%w: %s", ErrUnverifiedState, err.Error())
	}
	if !info.Mode().IsRegular() || info.Size() != int64(len(protocolStamp)) {
		return ErrUnverifiedState
	}
	data := make([]byte, len(protocolStamp))
	if _, err := lock.ReadAt(data, 0); err != nil {
		return fmt.Errorf("%w: %s", ErrUnverifiedState, err.Error())
	}
	if string(data) != protocolStamp {
		return ErrUnverifiedState
	}
	return nil
}

// CleanupGuard holds the existing instance-lock inode and directory open until
// Close. It is a lease, not a point-in-time check to release before cleanup.
// Its fields intentionally expose no writable file handles.
type CleanupGuard struct {
	lock     *os.File
	dir      *os.File
	once     sync.Once
	closeErr error
}

func (g *CleanupGuard) Close() error {
	if g == nil {
		return nil
	}
	g.once.Do(func() { g.closeErr = errors.Join(g.lock.Close(), g.dir.Close()) })
	return g.closeErr
}

// AcquireCleanupGuard proves quiescence of this daemon's supervised workers and
// excludes another owner while the caller completes cleanup. It is read-only:
// no file/directory creation, truncation, marker retirement or repair. Missing
// state/lock, an unknown protocol, an active owner or ANY pending worker marker
// is an error. It does not borrow PID-namespace restart recovery for deletion.
//
// REQUIRED caller contract:
//   - Propagate every failed/unknown deprovision result BEFORE destructive clean.
//     This guard proves only pgbackrestd workers, not postgres or other services.
//   - Cover every affected StateDir, acquiring ALL guards before deleting any
//     data. An incomplete inventory or absent path is not evidence of quiescence.
//     Only explicitly known never-started resources can omit this requirement.
//   - Keep an independent lifecycle lock across stop, guard acquisition, clean
//     and guard release. That lock's inode must survive outside the deleted tree.
//     Deleting StateDir unlinks instance.lock; its held fd cannot exclude a new
//     inode created by an uncoordinated launcher. No starts/path replacement are
//     allowed during cleanup.
//   - Hold each guard until cleanup has completed or failed; propagate failures.
//
// Legacy workers not launched under this supervision protocol require separate
// verified teardown. Neither a new stamp nor a new process retroactively proves
// their exit. A valid stamp and no active marker mean recorded worker ownership
// ended under the protocol, NOT that PGDATA is a successful restore.
func AcquireCleanupGuard(stateDir string) (guard *CleanupGuard, err error) {
	if stateDir == "" {
		return nil, ErrUnverifiedState
	}
	stateDir, err = filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Open(stateDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open %s: %s", ErrUnverifiedState, stateDir, err.Error())
	}
	dir := os.NewFile(uintptr(fd), stateDir)
	defer func() {
		if err != nil {
			_ = dir.Close()
		}
	}()
	// No O_CREAT or O_TRUNC. O_RDWR permits exclusive flock on filesystems that
	// require a writable descriptor; the guard never writes through it.
	lockFD, err := unix.Openat(fd, InstanceLockName, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open existing instance lock: %s", ErrUnverifiedState, err.Error())
	}
	lock := os.NewFile(uintptr(lockFD), filepath.Join(stateDir, InstanceLockName))
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	info, err := lock.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrUnverifiedState
	}
	if err := unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrStateInUse, stateDir, err)
	}
	if err := verifyStamp(lock); err != nil {
		return nil, fmt.Errorf("%w: %s", err, stateDir)
	}
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("%w: list state: %s", ErrUnverifiedState, err.Error())
	}
	slices.SortFunc(entries, func(a, b os.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	for _, entry := range entries {
		if IsPendingWorker(entry.Name()) {
			return nil, fmt.Errorf("%w: %s", ErrWorkersUnsettled, filepath.Join(stateDir, entry.Name()))
		}
	}
	// Detect replacement that happened during inspection. The caller's retained
	// lifecycle lock is what excludes changes after this check and during clean.
	dirInfo, err := dir.Stat()
	if err != nil {
		return nil, err
	}
	namedDir, err := os.Lstat(stateDir)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrUnverifiedState, err.Error())
	}
	namedLock, err := os.Lstat(filepath.Join(stateDir, InstanceLockName))
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrUnverifiedState, err.Error())
	}
	if !os.SameFile(dirInfo, namedDir) || !os.SameFile(info, namedLock) {
		return nil, fmt.Errorf("%w: ownership paths changed", ErrUnverifiedState)
	}
	return &CleanupGuard{lock: lock, dir: dir}, nil
}
