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

package pgbackrestd

import (
	"errors"
	"fmt"
	"os"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/tools/fileutil"
)

// fileRepositoryScope pins this state directory to the repository it first
// used. It is a file of its own, apart from generations.json, because a
// generations file that no longer loads is set aside and the sidecar starts
// unconfigured, and the pin must outlive that.
const fileRepositoryScope = dirState + "/repository_scope"

// backupConfig is the backup configuration this sidecar was started with,
// scoped to its shard. The scope comes from the flags alone: no request can
// name a repository. backup.Config.ForShard owns the path: the catalog shard
// keeps the location's base path and every other shard gets
// tablegroups/<tablegroup>/<id>/<shard>.
func (c Config) backupConfig() (*backup.Config, error) {
	base, err := backup.NewConfig(c.location())
	if err != nil {
		return nil, err
	}
	return base.ForShard(c.TableGroup, c.TableGroupID, c.Shard)
}

// repositoryScope is the path of this shard's repository below the location's
// base path, "" for the catalog shard.
func (c Config) repositoryScope() (string, error) {
	bc, err := c.backupConfig()
	if err != nil {
		return "", err
	}
	return bc.RepositoryScope(), nil
}

func describeScope(scope string) string {
	if scope == "" {
		return "the location's base path"
	}
	return scope
}

// readScopePin returns the scope this state directory is pinned to. A state
// directory that holds a persisted configuration but no pin was in use before
// repositories were scoped, so its repository is the base path.
func (s *Server) readScopePin() (scope string, pinned bool, err error) {
	raw, err := os.ReadFile(s.cfg.stateSub(fileRepositoryScope))
	if err == nil {
		return string(raw), true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", false, fmt.Errorf("read %s: %w", fileRepositoryScope, err)
	}
	if _, statErr := os.Lstat(s.cfg.stateSub(fileGenerations)); statErr == nil {
		return "", true, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", false, fmt.Errorf("stat %s: %w", fileGenerations, statErr)
	}
	return "", false, nil
}

// checkScopePin fails when the flags scope to a different repository than the
// one this state directory has used. A changed --table-group, --tablegroup-id
// or --shard would otherwise move the sidecar to an empty repository and leave
// the shard's backups and WAL behind.
func (s *Server) checkScopePin() error {
	want, err := s.cfg.repositoryScope()
	if err != nil {
		return err
	}
	pinned, ok, err := s.readScopePin()
	if err != nil {
		return err
	}
	if ok && pinned != want {
		return fmt.Errorf("this state directory uses the repository at %s, but --table-group, --tablegroup-id and --shard select %s; "+
			"a changed scope would leave this shard's backups and WAL behind. If the change is intended, remove %s and restart",
			describeScope(pinned), describeScope(want), s.cfg.stateSub(fileRepositoryScope))
	}
	return nil
}

// pinScope records the repository the first render uses. It runs before the
// configuration naming that repository is written, so no repository is used
// without a pin. A configuration that predates pins gets its implied pin
// written here, so the file is the one record from then on.
func (s *Server) pinScope() error {
	if err := s.checkScopePin(); err != nil {
		return err
	}
	want, err := s.cfg.repositoryScope()
	if err != nil {
		return err
	}
	path := s.cfg.stateSub(fileRepositoryScope)
	if raw, err := os.ReadFile(path); err == nil && string(raw) == want {
		return nil
	}
	return fileutil.AtomicWriteFile(path, []byte(want), 0o600)
}
