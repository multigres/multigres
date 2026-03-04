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

package topoclient

import (
	"context"
	"errors"
	"fmt"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const bootstrapAttemptFile = "BootstrapAttempt"

// pathForBootstrapAttempt returns the topology path for the BootstrapAttempt record of a shard.
func pathForBootstrapAttempt(shardKey types.ShardKey) string {
	return path.Join(DatabasesPath, shardKey.Database, shardKey.TableGroup, shardKey.Shard, bootstrapAttemptFile)
}

// UpdateBootstrapAttempt reads the current BootstrapAttempt for the shard (nil if none),
// passes it to fn, and writes the result back if fn returns a non-nil record.
//
// Returns (true, nil)  if fn returned a non-nil record and it was successfully written.
// Returns (false, nil) if fn returned nil (chose not to write, e.g. rate-limited).
// Returns (false, err) if the topology read/write failed or fn returned an error.
//
// The shard lock must be held by the caller. Retries transparently on CAS conflicts.
func (ts *store) UpdateBootstrapAttempt(
	ctx context.Context,
	shardKey types.ShardKey,
	fn func(*clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error),
) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	filePath := pathForBootstrapAttempt(shardKey)

	for {
		// Read existing record and version.
		contents, version, err := ts.globalTopo.Get(ctx, filePath)

		var existing *clustermetadatapb.BootstrapAttempt
		switch {
		case err == nil:
			existing = &clustermetadatapb.BootstrapAttempt{}
			if unmarshalErr := proto.Unmarshal(contents, existing); unmarshalErr != nil {
				return false, fmt.Errorf("unmarshal bootstrap attempt: %w", unmarshalErr)
			}
		case errors.Is(err, &TopoError{Code: NoNode}):
			// existing remains nil
		default:
			return false, fmt.Errorf("get bootstrap attempt: %w", err)
		}

		// Let the caller decide what to write.
		updated, fnErr := fn(existing)
		if fnErr != nil {
			return false, fnErr
		}
		if updated == nil {
			// Caller chose not to write.
			return false, nil
		}

		newContents, err := proto.Marshal(updated)
		if err != nil {
			return false, fmt.Errorf("marshal bootstrap attempt: %w", err)
		}

		if existing == nil {
			// No existing record — create.
			_, err = ts.globalTopo.Create(ctx, filePath, newContents)
			if errors.Is(err, &TopoError{Code: NodeExists}) {
				// Narrow race: another caller created it concurrently. Retry.
				continue
			}
			if err != nil {
				return false, fmt.Errorf("create bootstrap attempt: %w", err)
			}
		} else {
			// Existing record — version-CAS update.
			_, err = ts.globalTopo.Update(ctx, filePath, newContents, version)
			if errors.Is(err, &TopoError{Code: BadVersion}) {
				// Narrow race: someone else updated concurrently. Retry.
				continue
			}
			if err != nil {
				return false, fmt.Errorf("update bootstrap attempt: %w", err)
			}
		}

		return true, nil
	}
}
