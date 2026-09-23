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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/pgctld"
)

// serverVersion memoizes `SELECT version()` for the running postmaster. The
// version cannot change while the same postmaster runs, so there is no TTL;
// a new postmaster identity replaces the entry. pgctld serves one data
// directory per process, so a single slot suffices.
//
// ponytail: no coalescing. Status is polled by one monitor loop; a duplicate
// psql fork on a cold slot is rare and harmless.
var serverVersion struct {
	sync.Mutex
	key, value string
}

// serverVersionKey identifies the running postmaster by PID and start
// timestamp (postmaster.pid lines 1 and 3), not by the reusable PID alone. An
// incomplete or missing identity disables the cache instead of reusing a
// potentially different server's version. The file's mutable readiness line
// is deliberately excluded.
func serverVersionKey(dataDir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, constants.PostmasterPIDFile))
	if err != nil {
		return "", err
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) < 3 {
		return "", errors.New("incomplete postmaster identity")
	}
	pid, err := strconv.ParseInt(strings.TrimSpace(lines[0]), 10, 64)
	if err != nil || pid <= 0 {
		return "", errors.New("invalid postmaster PID")
	}
	start, err := strconv.ParseInt(strings.TrimSpace(lines[2]), 10, 64)
	if err != nil || start <= 0 {
		return "", errors.New("invalid postmaster start time")
	}
	return fmt.Sprintf("%d/%d", pid, start), nil
}

// cachedServerVersion returns the server version, reusing a successful lookup
// while the same postmaster remains active. Failed lookups are not cached, and
// a lookup that spans a postmaster replacement is discarded. It says nothing
// about health: callers must probe readiness live before trusting it.
func cachedServerVersion(ctx context.Context, config *pgctld.PostgresCtlConfig) string {
	key, err := serverVersionKey(config.PostgresDataDir)
	if err != nil {
		return getServerVersionWithConfig(ctx, config)
	}
	serverVersion.Lock()
	cached, hit := serverVersion.value, serverVersion.key == key
	serverVersion.Unlock()
	if hit {
		return cached
	}

	// Not held across the psql call: a slow psql must not block Status.
	value := getServerVersionWithConfig(ctx, config)
	if value == "" {
		return ""
	}
	if after, err := serverVersionKey(config.PostgresDataDir); err != nil || after != key {
		return "" // postmaster changed during the query; unclear which server answered
	}
	serverVersion.Lock()
	serverVersion.key, serverVersion.value = key, value
	serverVersion.Unlock()
	return value
}
