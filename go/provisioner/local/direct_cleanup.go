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
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// deprovisionSelected is shared by single-service and database deprovision.
// The caller holds the stable outer lifecycle lease. Capture the full footprint
// and every affected sidecar before shutdown or removal of any service record.
func (p *localProvisioner) deprovisionSelected(ctx context.Context, selected, all []*LocalProvisionedService, clean bool) (resultErr error) {
	if len(selected) == 0 {
		return nil
	}
	var roots, logs, records []string
	seen := map[string]bool{}
	add := func(path string) error {
		if path == "" {
			return nil
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		if !seen[abs] {
			roots = append(roots, abs)
			seen[abs] = true
		}
		return nil
	}
	for _, service := range selected {
		if err := add(service.DataDir); err != nil {
			return err
		}
		if service.LogFile != "" {
			logs = append(logs, service.LogFile)
		}
		records = append(records, service.statePath)
	}
	if clean {
		if err := p.validateDirectCleanupFootprint(append(append([]string{}, roots...), logs...)); err != nil {
			return err
		}
	}
	// Include record paths in inventory, but delete records last. In particular,
	// a guard failure leaves the path mapping and shutdown evidence recoverable.
	footprint := append(append(append([]string{}, roots...), logs...), records...)
	inventory, err := p.sidecarCleanupInventoryForRoots(all, footprint, false)
	if err != nil {
		return err
	}
	held, err := lockSidecarInstallations(inventory)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, held.Close()) }()
	for _, phase := range []string{"application", "pgctld", "etcd"} {
		for _, service := range selected {
			kind := "application"
			if service.Service == "pgctld" || service.Service == "etcd" {
				kind = service.Service
			}
			if kind != phase {
				continue
			}
			if err := p.shutdownService(ctx, service); err != nil {
				return err
			}
		}
	}
	if !clean {
		return nil
	} // Retain stopped records for a later guarded clean.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := held.requirePostgresShutdown(all); err != nil {
		return err
	}
	if err := held.acquireCleanupGuards(); err != nil {
		return err
	}
	keep := held.preservedPaths()
	for _, root := range roots {
		if err := cleanupPreservingLocks(root, keep); err != nil {
			return err
		}
	}
	for _, log := range logs {
		if err := removeFilePreservingLocks(log, keep); err != nil {
			return err
		}
	}
	for _, record := range records {
		if err := removeFilePreservingLocks(record, keep); err != nil {
			return err
		}
		// The complete marker was durable before deletion. A resurrected record
		// after a crash therefore cannot authorize another numeric-PID signal.
		dir, err := os.Open(filepath.Dir(record))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
			return err
		}
	}
	return nil
}

// Direct cleanup never owns the control-plane namespace. Reject both ancestors
// and descendants before shutdown, not just during the eventual deletion pass.
// Teardown's separately guarded, records-last state cleanup is not routed here.
func (p *localProvisioner) validateDirectCleanupFootprint(targets []string) error {
	boundaries := []string{filepath.Join(p.getRootWorkingDir(), ".sidecar-lifecycle.lock"), p.getStateDir()}
	for _, target := range targets {
		for _, boundary := range boundaries {
			for _, canonical := range []bool{false, true} {
				a, err := cleanupComparisonPath(target, canonical)
				if err != nil {
					return err
				}
				b, err := cleanupComparisonPath(boundary, canonical)
				if err != nil {
					return err
				}
				for _, pair := range [][2]string{{a, b}, {b, a}} {
					rel, err := filepath.Rel(pair[0], pair[1])
					if err != nil {
						return err
					}
					if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
						return fmt.Errorf("unsafe direct cleanup footprint %s overlaps control-plane boundary %s", target, boundary)
					}
				}
			}
		}
	}
	return nil
}

// Resolve the existing prefix as well as fully existing paths, so a missing
// leaf under a symlink cannot hide an overlap. Resolution errors fail closed.
func cleanupComparisonPath(path string, canonical bool) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil || !canonical {
		return abs, err
	}
	prefix := abs
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(prefix)
		if err == nil {
			for _, part := range slices.Backward(suffix) {
				resolved = filepath.Join(resolved, part)
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) || filepath.Dir(prefix) == prefix {
			return "", err
		}
		suffix = append(suffix, filepath.Base(prefix))
		prefix = filepath.Dir(prefix)
	}
}

// Log and record cleanup must not turn a file request into recursive directory
// deletion. It also must never unlink a protected lock or its directory.
func removeFilePreservingLocks(path string, keep []string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	for _, protected := range keep {
		if path == protected {
			return nil
		}
		rel, err := filepath.Rel(path, protected)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("refusing to unlink lock ancestor %s", path)
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
