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

package manager

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/fileutil"
)

// databaseCacheFile holds a static leader's copy of its database's topology
// record, in the pooler directory.
const databaseCacheFile = "topo-database.pb"

// A static leader is the only pooler of its database, so nothing else can
// change its database record while it runs. It keeps a copy of the record it
// last read, and starts from that copy when the topology is unreachable;
// other poolers always read the topology.

// saveDatabaseCache keeps db as the static leader's copy of its database
// record. A failure is logged: the topology stays the source of truth.
func (pm *MultipoolerManager) saveDatabaseCache(db *clustermetadatapb.Database) {
	if !pm.config.StaticLeader {
		return
	}
	data, err := proto.Marshal(db)
	if err == nil {
		err = fileutil.AtomicWriteFile(filepath.Join(pm.record.PoolerDir(), databaseCacheFile), data, 0o600)
	}
	if err != nil {
		pm.logger.Warn("failed to keep a copy of the database record", "error", err)
	}
}

// loadDatabaseCache returns the static leader's copy of its database record,
// or nil if the pooler is not a static leader or has no copy yet.
func (pm *MultipoolerManager) loadDatabaseCache() (*clustermetadatapb.Database, error) {
	if !pm.config.StaticLeader {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(pm.record.PoolerDir(), databaseCacheFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading the copy of the database record: %w", err)
	}
	db := &clustermetadatapb.Database{}
	if err := proto.Unmarshal(data, db); err != nil {
		return nil, fmt.Errorf("parsing the copy of the database record: %w", err)
	}
	return db, nil
}
