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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/multigres/multigres/go/tools/fileutil"
)

// loadShutdownServices rejects unknown or unreadable records rather than
// assuming their processes are absent. WalkDir gives deterministic ordering.
func (p *localProvisioner) loadShutdownServices() (services []*LocalProvisionedService, resultErr error) {
	rootPath := p.getStateDir()
	root, err := os.OpenRoot(rootPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	walkErr := fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			parts := strings.Split(path, "/")
			if path == "." || path == "dbs" || (len(parts) == 2 && parts[0] == "dbs") {
				return nil
			}
			return filepath.SkipDir
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlinked service state %s", path)
		}
		raw, err := root.ReadFile(path)
		if err != nil {
			return err
		}
		var service LocalProvisionedService
		if err := json.Unmarshal(raw, &service); err != nil {
			return fmt.Errorf("unreadable service state %s: %w", path, err)
		}
		switch service.Service {
		case "etcd", "multiadmin", "multiorch", "multipooler", "minigres", "multigateway", "pgctld":
		default:
			return fmt.Errorf("unknown service state %s", path)
		}
		if service.ID == "" || service.PID <= 0 || entry.Name() != service.Service+"_"+service.ID+".json" {
			return fmt.Errorf("invalid service identity in %s", path)
		}
		service.statePath = filepath.Join(rootPath, filepath.FromSlash(path))
		services = append(services, &service)
		return nil
	})
	return services, walkErr
}

func persistShutdownState(service *LocalProvisionedService) error {
	raw, err := json.Marshal(service)
	if err != nil {
		return err
	}
	if err := fileutil.AtomicWriteFile(service.statePath, raw, 0o600); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(service.statePath))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (p *localProvisioner) shutdownService(ctx context.Context, service *LocalProvisionedService) error {
	if service.Metadata["shutdown_complete"] == true {
		return nil
	}
	if service.Metadata["shutdown_attempted"] == true {
		return errors.New("previous shutdown outcome is unknown; refusing to signal a possibly reused service PID")
	}
	if service.Metadata == nil {
		service.Metadata = map[string]any{}
	}
	// Persist intent BEFORE any inherited numeric-PID shutdown path. A failed
	// or interrupted attempt is never repeated automatically against that PID.
	service.Metadata["shutdown_attempted"] = true
	if err := persistShutdownState(service); err != nil {
		return err
	}
	var err error
	if service.Service == "pgctld" {
		err = p.deprovisionPgctldLocked(ctx, service)
	} else {
		err = p.stopProcessByPID(ctx, service.Service, service.PID)
	}
	if err != nil {
		return err
	}
	if service.Service == "pgctld" {
		service.Metadata["postgres_shutdown_confirmed"] = true
	}
	service.Metadata["shutdown_complete"] = true
	return persistShutdownState(service)
}
