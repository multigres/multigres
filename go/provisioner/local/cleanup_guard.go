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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/multigres/multigres/go/common/pgbackreststate"
)

// The outer boundary is outside every subtree removed by Teardown. Keep this
// inode across clean/reprovision, so another launcher cannot open a new lock.
// Lock order: outer -> sorted installation lifecycle -> owner (shutdown) ->
// sorted server cleanup guards. Release guards before lifecycle and outer.
func (p *localProvisioner) lockSidecarLifecycle() (*os.File, error) {
	return (PgBackRestdPaths{Dir: p.getRootWorkingDir()}).lockFile(".sidecar-lifecycle.lock")
}

type (
	sidecarLifecycleKey   struct{}
	sidecarLifecycleLease struct {
		provisioner *localProvisioner
		active      atomic.Bool
	}
)

// Bootstrap -> ProvisionDatabase -> provisionPgctld share one outer lease;
// independent callers must acquire the stable inode themselves.
func (p *localProvisioner) enterSidecarLifecycle(ctx context.Context) (context.Context, func() error, error) {
	if lease, ok := ctx.Value(sidecarLifecycleKey{}).(*sidecarLifecycleLease); ok && lease.provisioner == p && lease.active.Load() {
		return ctx, func() error { return nil }, nil
	}
	lock, err := p.lockSidecarLifecycle()
	if err != nil {
		return ctx, nil, err
	}
	lease := &sidecarLifecycleLease{provisioner: p}
	lease.active.Store(true)
	return context.WithValue(ctx, sidecarLifecycleKey{}, lease), func() error {
		lease.active.Store(false)
		return lock.Close()
	}, nil
}

// Inventory and deletion share this single destructive-footprint definition.
func (p *localProvisioner) cleanupRoots() []string {
	return []string{p.getDataDir(), filepath.Join(p.getRootWorkingDir(), "sockets"), filepath.Join(p.getRootWorkingDir(), "spool"), p.getLogsDir(), p.getStateDir()}
}

func (p *localProvisioner) sidecarCleanupInventory(services []*LocalProvisionedService) ([]PgBackRestdPaths, error) {
	return p.sidecarCleanupInventoryForRoots(services, p.cleanupRoots(), true)
}

func installationAffected(pooler string, roots []string) bool {
	pooler, err := filepath.Abs(pooler)
	if err != nil {
		return true
	}
	for _, candidate := range []string{pooler, NewPgBackRestdPaths(pooler).Dir} {
		for _, root := range roots {
			root, err = filepath.Abs(root)
			if err != nil {
				return true
			}
			for _, pair := range [][2]string{{root, candidate}, {candidate, root}} {
				rel, err := filepath.Rel(pair[0], pair[1])
				if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					return true
				}
			}
		}
	}
	return false
}

func (p *localProvisioner) sidecarCleanupInventoryForRoots(services []*LocalProvisionedService, roots []string, allConfigured bool) ([]PgBackRestdPaths, error) {
	byDir := map[string]PgBackRestdPaths{}
	add := func(pooler string) error {
		abs, err := filepath.Abs(pooler)
		if err != nil {
			return err
		}
		paths := NewPgBackRestdPaths(abs)
		byDir[paths.Dir] = paths
		return nil
	}
	for _, cell := range p.config.Cells {
		if cell.Pgctld.PoolerDir != "" && (allConfigured || installationAffected(cell.Pgctld.PoolerDir, roots)) {
			if err := add(cell.Pgctld.PoolerDir); err != nil {
				return nil, err
			}
		}
	}
	for _, service := range services {
		if service.Service == "pgctld" || service.Service == "multipooler" || service.Service == "minigres" {
			if service.DataDir == "" {
				if service.Service == "pgctld" {
					return nil, errors.New("pgctld state lacks pooler directory")
				}
				continue
			}
			// A declared pooler DataDir remains an affected installation even if
			// PGDATA/state is missing (for example, during a failed restore).
			if !allConfigured && !installationAffected(service.DataDir, roots) {
				continue
			}
			if err := add(service.DataDir); err != nil {
				return nil, err
			}
		}
	}
	for _, candidate := range roots {
		root, err := filepath.Abs(candidate)
		if err != nil {
			return nil, err
		}
		// A pooler equal to a deletion root has its sidecar sibling OUTSIDE
		// that root. Inventory this supported boundary layout explicitly.
		sibling := NewPgBackRestdPaths(root).Dir
		if _, err := os.Lstat(sibling); err == nil {
			if err := add(root); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if path == root && errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			if strings.HasPrefix(entry.Name(), "pgbackrestd-") {
				if !entry.IsDir() {
					return fmt.Errorf("unverified sidecar directory %s", path)
				}
				name := strings.TrimPrefix(entry.Name(), "pgbackrestd-")
				if name == "" {
					return fmt.Errorf("unrecognized sidecar directory %s", path)
				}
				if err := add(filepath.Join(filepath.Dir(path), name)); err != nil {
					return err
				}
				// Opaque supervision state is only interpreted by the shared helper.
				return filepath.SkipDir
			}
			// Cover PGDATA even when its sibling state/ownership is missing. A
			// missing sidecar must not turn live database data into ordinary debris.
			if entry.Name() == "pg_data" {
				if err := add(filepath.Dir(path)); err != nil {
					return err
				}
				if entry.IsDir() {
					return filepath.SkipDir
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	dirs := make([]string, 0, len(byDir))
	for dir := range byDir {
		dirs = append(dirs, dir)
	}
	slices.Sort(dirs)
	out := make([]PgBackRestdPaths, 0, len(dirs))
	for _, dir := range dirs {
		if info, err := os.Lstat(dir); err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf("unverified sidecar directory %s", dir)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		out = append(out, byDir[dir])
	}
	return out, nil
}

type sidecarCleanupExclusion struct {
	paths      []PgBackRestdPaths
	lifecycles []*os.File
	guards     []*pgbackreststate.CleanupGuard
}

func lockSidecarInstallations(paths []PgBackRestdPaths) (*sidecarCleanupExclusion, error) {
	held := &sidecarCleanupExclusion{paths: paths}
	for _, path := range paths {
		lock, err := path.lockFile("lifecycle.lock")
		if err != nil {
			return nil, errors.Join(err, held.Close())
		}
		held.lifecycles = append(held.lifecycles, lock)
	}
	return held, nil
}

// The sidecar guard proves only its own workers. Every installation also
// needs its own successful pgctld/PostgreSQL shutdown record; absence is never
// inferred to mean never-started. Shutdown updates these loaded records in place.
func (h *sidecarCleanupExclusion) requirePostgresShutdown(services []*LocalProvisionedService) error {
	stopped := map[string]bool{}
	for _, service := range services {
		if service.Service != "pgctld" || service.DataDir == "" {
			continue
		}
		pooler, err := filepath.Abs(service.DataDir)
		if err != nil {
			return err
		}
		confirmed := service.Metadata["shutdown_attempted"] == true && service.Metadata["shutdown_complete"] == true && service.Metadata["postgres_shutdown_confirmed"] == true
		prior, seen := stopped[pooler]
		stopped[pooler] = confirmed && (!seen || prior)
	}
	for _, paths := range h.paths {
		pooler := filepath.Join(filepath.Dir(paths.Dir), strings.TrimPrefix(filepath.Base(paths.Dir), "pgbackrestd-"))
		if !stopped[pooler] {
			return fmt.Errorf("PostgreSQL shutdown evidence missing for installation %s; preserving all data and state", pooler)
		}
	}
	return nil
}

func (h *sidecarCleanupExclusion) acquireCleanupGuards() error {
	for _, path := range h.paths {
		// An absent state directory is NOT never-started evidence. No configured
		// or discovered installation is skipped based only on missing files.
		guard, err := pgbackreststate.AcquireCleanupGuard(path.StateDir)
		if err != nil {
			return fmt.Errorf("sidecar quiescence unverified at %s: %w", path.StateDir, err)
		}
		h.guards = append(h.guards, guard)
	}
	return nil
}

func (h *sidecarCleanupExclusion) Close() error {
	var errs []error
	for _, guard := range slices.Backward(h.guards) {
		errs = append(errs, guard.Close())
	}
	h.guards = nil
	for _, lifecycle := range slices.Backward(h.lifecycles) {
		errs = append(errs, lifecycle.Close())
	}
	h.lifecycles = nil
	return errors.Join(errs...)
}

func (h *sidecarCleanupExclusion) preservedPaths() []string {
	var paths []string
	for _, p := range h.paths {
		paths = append(paths, filepath.Join(p.StateDir, pgbackreststate.InstanceLockName), filepath.Join(p.Dir, "lifecycle.lock"), filepath.Join(p.Dir, "provisioner.lock"))
	}
	return paths
}

// cleanupPreservingLocks removes only unprotected children, preserving the
// server lock inode, local lock inodes and all their ancestor directories.
// All lifecycle and authoritative state guards must remain held throughout.
func cleanupPreservingLocks(path string, keep []string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	ancestor := false
	for _, protected := range keep {
		if path == protected {
			return nil
		}
		rel, err := filepath.Rel(path, protected)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			ancestor = true
		}
	}
	if !ancestor {
		return os.RemoveAll(path)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("refusing cleanup through non-directory lock ancestor %s", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := cleanupPreservingLocks(filepath.Join(path, entry.Name()), keep); err != nil {
			return err
		}
	}
	return nil
}
