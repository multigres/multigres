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

// Package shardingcatalogcache keeps the gateway's copy of each database's
// sharding catalog snapshot, fetched from the catalog shard's primary.
//
// Each database has one fetch loop, so fetches are single-flight per
// database. The loop fetches when the database is first seen, when the
// catalog primary's health stream reports a routing_version above the cached
// one, and on a backstop poll. A snapshot is installed only if it validates
// and its routing_version is at least the cached one, so the cache never goes
// backwards; a failed fetch keeps the old snapshot and the next trigger
// retries.
package shardingcatalogcache

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/shardingcatalog"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Source gives the current sharding catalog snapshot of a database. It is what
// the executor and planner take.
type Source interface {
	// Snapshot returns the database's current snapshot, or nil when none has
	// been fetched yet. The snapshot is immutable and may be held across
	// calls; a later call may return a newer one.
	Snapshot(database string) *shardingcatalog.Snapshot
}

// FetchFunc asks the catalog primary of database for its snapshot. since is
// the routing_version of the cached snapshot, nil when there is none.
type FetchFunc func(ctx context.Context, database string, since *int64) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error)

// fetchTimeout bounds one fetch.
const fetchTimeout = 30 * time.Second

// Cache holds a snapshot per database. It implements Source.
type Cache struct {
	ctx          context.Context
	logger       *slog.Logger
	fetch        FetchFunc
	pollInterval time.Duration
	metrics      *metrics

	mu        sync.Mutex
	databases map[string]*database
}

var _ Source = (*Cache)(nil)

// database is one database's cached snapshot and its fetch loop.
type database struct {
	name     string
	snapshot atomic.Pointer[shardingcatalog.Snapshot]
	// wake asks the loop to fetch. It holds at most one request, and a trigger
	// that finds it full is dropped: so however many triggers arrive while a
	// fetch runs, exactly one more fetch follows, and it starts after all of
	// them.
	wake chan struct{}
}

// New returns a cache that fetches with fetch and polls every pollInterval
// (no polling when it is not positive). Loops stop when ctx ends.
func New(ctx context.Context, logger *slog.Logger, fetch FetchFunc, pollInterval time.Duration) *Cache {
	m, err := newMetrics()
	if err != nil {
		logger.WarnContext(ctx, "failed to initialise some sharding catalog metrics", "error", err)
	}
	return &Cache{
		ctx:          ctx,
		logger:       logger,
		fetch:        fetch,
		pollInterval: pollInterval,
		metrics:      m,
		databases:    make(map[string]*database),
	}
}

// Snapshot implements Source.
func (c *Cache) Snapshot(name string) *shardingcatalog.Snapshot {
	c.mu.Lock()
	db := c.databases[name]
	c.mu.Unlock()
	if db == nil {
		return nil
	}
	return db.snapshot.Load()
}

// Databases returns the names of every database the cache has seen.
func (c *Cache) Databases() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.databases))
	for name := range c.databases {
		names = append(names, name)
	}
	return names
}

// ObserveCatalogShard records a health update from a pooler of the catalog
// shard of a database. primary is whether the pooler is the live routing
// primary, and routingVersion what it reported (zero from anything else).
// The first observation of a database starts its fetch loop. A fetch is
// triggered when the primary reports a version above the cached snapshot's,
// or when there is a primary and no snapshot yet. Cheap enough to call on
// every health message.
func (c *Cache) ObserveCatalogShard(name string, primary bool, routingVersion int64) {
	db := c.database(name)
	if !primary {
		return
	}
	snap := db.snapshot.Load()
	if snap == nil || routingVersion > snap.RoutingVersion() {
		db.trigger()
	}
}

// database returns the entry for name, creating it and starting its loop on
// first sight.
func (c *Cache) database(name string) *database {
	c.mu.Lock()
	defer c.mu.Unlock()
	db, ok := c.databases[name]
	if ok {
		return db
	}
	db = &database{name: name, wake: make(chan struct{}, 1)}
	c.databases[name] = db
	db.trigger()
	go c.run(db)
	return db
}

func (db *database) trigger() {
	select {
	case db.wake <- struct{}{}:
	default:
	}
}

// run is the database's fetch loop.
func (c *Cache) run(db *database) {
	var poll <-chan time.Time
	if c.pollInterval > 0 {
		ticker := time.NewTicker(c.pollInterval)
		defer ticker.Stop()
		poll = ticker.C
	}
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-db.wake:
		case <-poll:
		}
		c.refresh(db)
	}
}

// refresh fetches once and installs the result if it is newer.
func (c *Cache) refresh(db *database) {
	ctx, cancel := context.WithTimeout(c.ctx, fetchTimeout)
	defer cancel()
	cached := db.snapshot.Load()
	var since *int64
	if cached != nil {
		v := cached.RoutingVersion()
		since = &v
	}
	resp, err := c.fetch(ctx, db.name, since)
	if err != nil {
		if c.ctx.Err() != nil {
			return
		}
		c.metrics.fetchFailed(ctx, db.name, failureFetch)
		c.logger.WarnContext(ctx, "failed to fetch sharding catalog snapshot; keeping the cached one",
			"database", db.name, "cached_routing_version", versionOf(cached), "error", err)
		return
	}
	if resp.GetUnchanged() {
		return
	}
	pb := resp.GetSnapshot()
	if pb == nil {
		c.metrics.fetchFailed(ctx, db.name, failureInvalid)
		c.logger.WarnContext(ctx, "catalog primary returned neither a snapshot nor unchanged; keeping the cached one",
			"database", db.name, "cached_routing_version", versionOf(cached))
		return
	}
	// A primary that has not caught up (a former primary not yet demoted, see
	// the pooler's checkCatalogPrimary) can serve an older catalog. Same
	// version means same contents, so there is nothing to install either.
	if cached != nil && pb.GetRoutingVersion() <= cached.RoutingVersion() {
		if pb.GetRoutingVersion() < cached.RoutingVersion() {
			c.logger.WarnContext(ctx, "ignoring an older sharding catalog snapshot",
				"database", db.name, "routing_version", pb.GetRoutingVersion(), "cached_routing_version", cached.RoutingVersion())
		}
		return
	}
	snap, err := shardingcatalog.New(pb)
	if err != nil {
		c.metrics.fetchFailed(ctx, db.name, failureInvalid)
		c.logger.ErrorContext(ctx, "rejected sharding catalog snapshot; keeping the cached one",
			"database", db.name, "routing_version", pb.GetRoutingVersion(), "cached_routing_version", versionOf(cached), "error", err)
		return
	}
	// The loop is the only writer, so nothing raced this install.
	db.snapshot.Store(snap)
	c.metrics.installed(ctx, db.name, snap.RoutingVersion())
	c.logger.InfoContext(ctx, "installed sharding catalog snapshot",
		"database", db.name, "routing_version", snap.RoutingVersion(), "plan_epoch", snap.PlanEpoch(),
		"previous_routing_version", versionOf(cached), "tablegroups", len(snap.TableGroups()))
}

// versionOf returns s's routing_version, or -1 when there is no snapshot.
func versionOf(s *shardingcatalog.Snapshot) int64 {
	if s == nil {
		return -1
	}
	return s.RoutingVersion()
}
