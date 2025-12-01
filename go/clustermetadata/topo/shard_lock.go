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

package topo

import (
	"context"
	"fmt"
)

// ShardsPath is the path component for shards in the topology hierarchy.
const ShardsPath = "shards"

type shardLock struct {
	database, tableGroup, shard string
}

var _ iTopoLock = (*shardLock)(nil)

func (s *shardLock) Type() string {
	return "shard"
}

func (s *shardLock) ResourceName() string {
	return fmt.Sprintf("%s/%s/%s", s.database, s.tableGroup, s.shard)
}

func (s *shardLock) Path() string {
	return fmt.Sprintf("%s/%s/%s/%s", DatabasesPath, s.database, s.tableGroup, s.shard)
}

// LockShard will lock the shard, and return:
// - a context with a locksInfo structure for future reference.
// - an unlock method
// - an error if anything failed.
//
// We are currently only using this method to lock actions that would
// impact each-other. Most changes of the Shard object are done by
// UpdateShardFields, which is not locking the shard object. The
// current list of actions that lock a shard are:
// * all Multigres-controlled re-parenting operations:
//   - PlannedReparentShard
//   - EmergencyReparentShard
//
// * any multiorch recovery e.g
//   - RecoverDeadPrimary
//   - ElectNewPrimary
//   - FixPrimary
//
// * operations that we don't want to conflict with re-parenting:
//   - DeleteTablet when it's the shard's current primary
//
// Note: Shard locks use named locks (LockNameWithTTL) because shards don't have
// dedicated topo entries. Named locks don't require the path to exist.
// If no TTL is specified via WithTTL option, it defaults to NamedLockTTL (24 hours).
func (ts *store) LockShard(ctx context.Context, database, tableGroup, shard, action string, opts ...LockOption) (context.Context, func(*error), error) {
	// Prepend Named lock type - user-provided options can override this
	opts = append([]LockOption{WithType(Named)}, opts...)
	return ts.internalLock(ctx, &shardLock{
		database:   database,
		tableGroup: tableGroup,
		shard:      shard,
	}, action, opts...)
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
// # We are currently using `TryLockShard` during tablet discovery in multiorch recovery
//
// Note: Uses NamedNonBlocking lock type because shards don't have dedicated topo entries.
func (ts *store) TryLockShard(ctx context.Context, database, tableGroup, shard, action string) (context.Context, func(*error), error) {
	return ts.internalLock(ctx, &shardLock{
		database:   database,
		tableGroup: tableGroup,
		shard:      shard,
	}, action, WithType(NamedNonBlocking))
}

// CheckShardLocked can be called on a context to make sure we have the lock
// for a given shard.
func CheckShardLocked(ctx context.Context, database, tableGroup, shard string) error {
	return checkLocked(ctx, &shardLock{
		database:   database,
		tableGroup: tableGroup,
		shard:      shard,
	})
}
