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

package command

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
)

//nolint:gosec // G101: protocol response, not a credential.
const walProbeToken = "wal-wrapper-v1\n"

// RunWALCommand is the standalone executable entry point. No Cobra, logging
// or telemetry initialization is needed on the per-segment path.
func RunWALCommand(name string, args []string) error {
	dir, set := os.LookupEnv(backup.WALPoolerDirEnv)
	if set && !filepath.IsAbs(dir) {
		return fmt.Errorf("%s must be an absolute path", backup.WALPoolerDirEnv)
	}
	if !set {
		// A postmaster already running when multipooler attaches has the old
		// environment. Its pgBackRest passfile lives next to the client config.
		pass := os.Getenv("PGPASSFILE")
		if !filepath.IsAbs(pass) || filepath.Base(pass) != "pgbackrest.pgpass" || filepath.Base(filepath.Dir(pass)) != "pgbackrest" {
			return fmt.Errorf("cannot locate pooler directory for %s", name)
		}
		dir = filepath.Dir(filepath.Dir(pass))
	}
	// A read-only handshake for checking an existing postmaster's environment.
	// Print a token rather than relying on an old binary's exit status.
	if len(args) == 3 && args[0] == "--probe" && args[1] == "pooler-root" {
		if fmt.Sprintf("%x", sha256.Sum256([]byte(filepath.Clean(dir)))) != args[2] {
			return errors.New("WAL pooler directory mismatch")
		}
		fmt.Print(walProbeToken)
		return nil
	}
	if len(args) == 3 && args[0] == "--probe" && args[1] == "wal-wrapper" && args[2] == "version" {
		fmt.Print(walProbeToken)
		return nil
	}
	argc := 1
	if name == backup.RestoreWALExecutable {
		argc = 2
	}
	if len(args) != argc {
		return fmt.Errorf("%s requires %d arguments", name, argc)
	}
	argv := []string{"pgbackrest", "--stanza=" + backup.WALStanza, "--config=" + backup.ClientConfigPath(dir)}
	if name == backup.RestoreWALExecutable {
		argv = append(argv, "archive-get")
		return runTrackedRestore(filepath.Join(dir, constants.RestoreCommandPIDFile), append(argv, args...))
	}
	// Replace the wrapper rather than introducing a parent that could swallow
	// pgBackRest's exit status or a signal from the archiver.
	path, err := exec.LookPath("pgbackrest")
	if err != nil {
		return err
	}
	// pgBackRest 2.60 forwards explicit resets to archive-push:async via
	// cfgExecParam. A queue cap can acknowledge segments dropped on overflow.
	argv = append(argv, "--reset-archive-push-queue-max", "archive-push", args[0])
	//nolint:gosec // G702: fixed pgbackrest executable; paths are separate argv elements, not shell code.
	return syscall.Exec(path, argv, os.Environ())
}

// The postgres launcher and pg_rewind must agree on the absolute executable
// directory and pooler root, including standalone CLI starts and restarts.
func walChildEnv(dir string) []string {
	binDir, _ := walSiblingDir()
	return walChildEnvForInstall(context.TODO(), &walInstall, binDir, dir)
}

func walChildEnvForInstall(ctx context.Context, cache *walInstallCache, binDir, dir string) []string {
	// Always override the inherited value, even when we cannot validate the
	// siblings. An empty value fails the stored shell command's expansion.
	approved := ""
	if filepath.IsAbs(binDir) {
		if version, _ := cache.check(ctx, binDir, true); version == backup.WALWrapperVersion {
			approved = binDir
		}
	}
	env := []string{backup.WALBinDirEnv + "=" + approved}
	if dir != "" {
		env = append(env, backup.WALPoolerDirEnv+"="+dir)
	}
	return env
}

// Cache definitive results by file identity. Transient failures are retried.
var walInstall walInstallCache

var walExecutableNames = [...]string{backup.ArchiveWALExecutable, backup.RestoreWALExecutable}

type walInstallCache struct {
	mu      sync.Mutex
	entries map[string]*walProbeEntry
}

type walProbeEntry struct {
	info                os.FileInfo
	done                chan struct{}
	compatible, unknown bool
}

func walSiblingDir() (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(self) {
		return "", errors.New("pgctld executable path is not absolute")
	}
	return filepath.Dir(self), nil
}

func installedWALVersion(ctx context.Context, force bool) (version uint32, unknown bool) {
	dir, err := walSiblingDir()
	if err != nil {
		return 0, true
	}
	return walInstall.check(ctx, dir, force)
}

func walExecutableInfo(path string) (info os.FileInfo, unknown bool) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, !os.IsNotExist(err) && !os.IsPermission(err)
	}
	// Do not approve symlinks or hard links that may share the mutable
	// pgctld binary. The installer puts independent copies beside pgctld.
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0o111 == 0 || fi.Sys().(*syscall.Stat_t).Nlink != 1 {
		return nil, false
	}
	return fi, false
}

func sameWALExecutable(a, b os.FileInfo) bool {
	return a != nil && b != nil && os.SameFile(a, b) && a.ModTime().Equal(b.ModTime()) && a.Size() == b.Size() && a.Mode() == b.Mode()
}

func (cache *walInstallCache) check(ctx context.Context, dir string, force bool) (version uint32, unknown bool) {
	// Both handshakes share a deadline; each WaitDelay additionally bounds
	// the stdout drain if a descendant keeps the pipe open.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var infos [len(walExecutableNames)]os.FileInfo
	for i, name := range walExecutableNames {
		fi, u := walExecutableInfo(filepath.Join(dir, name))
		if fi == nil && !u {
			return 0, false
		}
		infos[i] = fi
		unknown = unknown || u
	}
	for i, name := range walExecutableNames {
		if infos[i] == nil {
			continue
		}
		ok, u := cache.checkFile(ctx, filepath.Join(dir, name), infos[i], force)
		if !ok && !u {
			return 0, false
		}
		unknown = unknown || u
	}
	if unknown || ctx.Err() != nil {
		return 0, true
	}
	return backup.WALWrapperVersion, false
}

func (cache *walInstallCache) checkFile(ctx context.Context, path string, fi os.FileInfo, force bool) (compatible, unknown bool) {
	cache.mu.Lock()
	if prev := cache.entries[path]; prev != nil && sameWALExecutable(prev.info, fi) {
		select {
		case <-prev.done:
			if !force {
				cache.mu.Unlock()
				return prev.compatible, prev.unknown
			}
		default:
			cache.mu.Unlock()
			// An ordinary Status need not wait for another RPC's subprocess.
			if !force {
				return false, true
			}
			select {
			case <-prev.done:
				return prev.compatible, prev.unknown
			case <-ctx.Done():
				return false, true
			}
		}
	}
	if ctx.Err() != nil {
		cache.mu.Unlock()
		return false, true
	}
	entry := &walProbeEntry{info: fi, done: make(chan struct{})}
	if cache.entries == nil {
		cache.entries = make(map[string]*walProbeEntry)
	}
	cache.entries[path] = entry
	cache.mu.Unlock()

	compatible, unknown = probeWALExecutable(ctx, path)
	// Never memoize the result against a file that changed during the probe.
	if now, _ := walExecutableInfo(path); !sameWALExecutable(fi, now) {
		compatible, unknown = false, true
	}

	cache.mu.Lock()
	entry.compatible, entry.unknown = compatible, unknown
	if unknown && cache.entries[path] == entry {
		delete(cache.entries, path)
	}
	close(entry.done)
	cache.mu.Unlock()
	return compatible, unknown
}

func probeWALExecutable(ctx context.Context, path string) (compatible, unknown bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var out walProbeOutput
	//nolint:gocritic // The fixed handshake needs os/exec to bound pipe drain with WaitDelay.
	cmd := exec.CommandContext(ctx, path, "--probe", "wal-wrapper", "version")
	cmd.Env = append(os.Environ(), backup.WALPoolerDirEnv+"=/")
	cmd.Stdout = &out
	// Leave stderr on /dev/null: only exact stdout establishes compatibility.
	cmd.WaitDelay = 100 * time.Millisecond
	out.cancel = cancel
	err := cmd.Run()
	if out.overflow {
		return false, false
	}
	if ctx.Err() != nil || errors.Is(err, exec.ErrWaitDelay) {
		return false, true
	}
	if err != nil {
		if _, exited := errors.AsType[*exec.ExitError](err); exited || errors.Is(err, syscall.ENOEXEC) || os.IsNotExist(err) || os.IsPermission(err) {
			return false, false
		}
		return false, true
	}
	return string(out.buf[:out.n]) == walProbeToken, false
}

// More than a single token is incompatible. Stop the probe rather than
// buffering (or continuing to drain) an unbounded stream.
type walProbeOutput struct {
	buf      [len(walProbeToken)]byte
	n        int
	overflow bool
	cancel   context.CancelFunc
}

func (out *walProbeOutput) Write(p []byte) (int, error) {
	n := copy(out.buf[out.n:], p)
	out.n += n
	if n < len(p) {
		out.overflow = true
		out.cancel()
		return n, errors.New("excess WAL wrapper probe output")
	}
	return n, nil
}
