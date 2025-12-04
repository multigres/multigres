// Copyright 2024 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package topoclient

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/types"
)

// ShardsPath is the path component for shards in the topology hierarchy.
const ShardsPath = "shards"

type shardLock struct {
	types.ShardKey
}

var _ iTopoLock = (*shardLock)(nil)

func (s *shardLock) Type() string {
	return "shard"
}

func (s *shardLock) ResourceName() string {
	return s.ShardKey.String()
}

func (s *shardLock) Path() string {
	return fmt.Sprintf("%s/%s/%s/%s", DatabasesPath, s.Database, s.TableGroup, s.Shard)
}

// LockShard will lock the shard, and return:
// - a context with a locksInfo structure for future reference.
// - an unlock method
// - an error if anything failed.
//
// We currently use this method for the bootstrap action in multiorch to ensure
// exclusive access to a shard during initialization.
//
// Note: Shard locks use named locks (LockNameWithTTL) which create the lock path
// if it doesn't exist. If no TTL is specified via WithTTL option, it defaults to
// NamedLockTTL (24 hours).
func (ts *store) LockShard(ctx context.Context, shardKey types.ShardKey, action string, opts ...LockOption) (context.Context, func(*error), error) {
	// Prepend Named lock type - user-provided options can override this
	opts = append([]LockOption{WithType(Named)}, opts...)
	return ts.internalLock(ctx, &shardLock{ShardKey: shardKey}, action, opts...)
}

// TryLockShard will lock the shard, and return:
// - a context with a locksInfo structure for future reference.
// - an unlock method
// - an error if anything failed.
//
// `TryLockShard` is different from `LockShard`. If there is already a lock on given shard,
// then unlike `LockShard` instead of waiting and blocking the client it returns with
// `Lock already exists` error. With current implementation it may not be able to fail-fast
// for some scenarios. For example there is a possibility that a thread checks for lock for
// a given shard but by the time it acquires the lock, some other thread has already acquired it,
// in this case the client will block until the other caller releases the lock or the
// client call times out (just like standard `LockShard' implementation). In short the lock checking
// and acquiring is not under the same mutex in current implementation of `TryLockShard`.
//
// Note: Uses NamedNonBlocking lock type which creates the lock path if it doesn't exist.
func (ts *store) TryLockShard(ctx context.Context, shardKey types.ShardKey, action string) (context.Context, func(*error), error) {
	return ts.internalLock(ctx, &shardLock{ShardKey: shardKey}, action, WithType(NamedNonBlocking))
}

// CheckShardLocked can be called on a context to make sure we have the lock
// for a given shard.
func CheckShardLocked(ctx context.Context, shardKey types.ShardKey) error {
	return checkLocked(ctx, &shardLock{ShardKey: shardKey})
}
