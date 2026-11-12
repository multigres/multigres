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
// catalog primary's health stream reports a newer snapshot than the cached
// one, and on a backstop poll. A failed fetch keeps the old snapshot and the
// next trigger retries.
//
// Snapshots are ordered by (consensus rule, routing_version): the rule the
// serving primary held, then the version. A routing_version can be lost in a
// failover and reused by the next primary with different contents, but the
// next primary always serves under a higher rule. So a snapshot from a higher
// rule replaces the cached one whatever its version, a lower version within
// the same rule never does, and a snapshot from a lower rule (a former
// primary that has not learned it was superseded) never does.
//
// Entries are never evicted: the cache keeps a database's last good snapshot
// through any outage of its catalog shard. A database dropped and recreated
// under the same name would start its rules over, which the ordering refuses;
// that needs the database's identity, not just its name, once dropping a
// database is supported.
package shardingcatalogcache

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
// the cached snapshot's position, nil when there is none.
type FetchFunc func(ctx context.Context, database string, since *Position) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error)

// Position orders snapshots: the consensus rule the serving primary held,
// then the routing_version.
type Position struct {
	Rule           *clustermetadatapb.RuleNumber
	RoutingVersion int64
}

// Compare returns -1, 0 or 1 as p is before, equal to or after other.
func (p Position) Compare(other Position) int {
	if c := commonconsensus.CompareRuleNumbers(p.Rule, other.Rule); c != 0 {
		return c
	}
	switch {
	case p.RoutingVersion < other.RoutingVersion:
		return -1
	case p.RoutingVersion > other.RoutingVersion:
		return 1
	}
	return 0
}

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

// installed is a snapshot and the position it was served at.
type installed struct {
	position Position
	snapshot *shardingcatalog.Snapshot
}

// database is one database's cached snapshot and its fetch loop.
type database struct {
	name    string
	current atomic.Pointer[installed]
	// wake asks the loop to fetch. It holds at most one request, and a trigger
	// that finds it full is dropped: so however many triggers arrive while a
	// fetch runs, exactly one more fetch follows, and it starts after all of
	// them.
	wake chan struct{}

	// installsMu guards installs, which is closed and replaced on every
	// install to wake WaitForRoutingVersion's waiters.
	installsMu sync.Mutex
	installs   chan struct{}
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
	if cur := c.current(name); cur != nil {
		return cur.snapshot
	}
	return nil
}

// Installed returns the database's current snapshot together with the
// position it was served at, or nil when there is none.
func (c *Cache) Installed(name string) (*shardingcatalog.Snapshot, Position) {
	if cur := c.current(name); cur != nil {
		return cur.snapshot, cur.position
	}
	return nil, Position{}
}

// RoutingVersion returns the routing_version of the database's current
// snapshot, 0 when there is none.
func (c *Cache) RoutingVersion(name string) int64 {
	if cur := c.current(name); cur != nil {
		return cur.position.RoutingVersion
	}
	return 0
}

func (c *Cache) current(name string) *installed {
	c.mu.Lock()
	db := c.databases[name]
	c.mu.Unlock()
	if db == nil {
		return nil
	}
	return db.current.Load()
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
// primary; rule and routingVersion are what it reported (unset from anything
// else). The first observation of a database starts its fetch loop. A fetch
// is triggered when the primary reports a position after the cached
// snapshot's, or when there is a primary and no snapshot yet. Cheap enough to
// call on every health message.
func (c *Cache) ObserveCatalogShard(name string, primary bool, rule *clustermetadatapb.RuleNumber, routingVersion int64) {
	db := c.database(name)
	if !primary {
		return
	}
	cur := db.current.Load()
	if cur == nil || (Position{Rule: rule, RoutingVersion: routingVersion}).Compare(cur.position) > 0 {
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
	db = &database{name: name, wake: make(chan struct{}, 1), installs: make(chan struct{})}
	c.databases[name] = db
	db.trigger()
	go c.run(db)
	return db
}

// WaitForRoutingVersion returns once the database's installed snapshot has a
// routing_version greater than after, or with ctx's error when ctx ends
// first. It triggers a fetch when the snapshot is not yet past after, rather
// than wait for the next health update or poll, and then waits for installs.
// A database the cache has not seen is treated as ObserveCatalogShard treats
// one: its fetch loop starts, and the wait lasts until the first snapshot
// past after is installed. Any number of callers may wait on one database.
func (c *Cache) WaitForRoutingVersion(ctx context.Context, name string, after int64) error {
	db := c.database(name)
	triggered := false
	for {
		// Take the channel before reading the snapshot: an install between
		// the two closes the channel, so it is never missed.
		installs := db.installsChannel()
		if cur := db.current.Load(); cur != nil && cur.position.RoutingVersion > after {
			return nil
		}
		if !triggered {
			db.trigger()
			triggered = true
		}
		select {
		case <-installs:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (db *database) installsChannel() chan struct{} {
	db.installsMu.Lock()
	defer db.installsMu.Unlock()
	return db.installs
}

// notifyInstalled wakes every WaitForRoutingVersion waiter after an install.
func (db *database) notifyInstalled() {
	db.installsMu.Lock()
	defer db.installsMu.Unlock()
	close(db.installs)
	db.installs = make(chan struct{})
}

func (db *database) trigger() {
	select {
	case db.wake <- struct{}{}:
	default:
	}
}

// run is the database's fetch loop. It ends with the cache's context.
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

// refresh fetches once and installs the result if it is after the cached
// snapshot.
func (c *Cache) refresh(db *database) {
	ctx, cancel := context.WithTimeout(c.ctx, fetchTimeout)
	defer cancel()
	cur := db.current.Load()
	var since *Position
	if cur != nil {
		since = &cur.position
	}
	resp, err := c.fetch(ctx, db.name, since)
	if err != nil {
		if c.ctx.Err() != nil {
			return
		}
		c.metrics.fetchFailed(ctx, db.name, failureFetch)
		c.logger.WarnContext(ctx, "failed to fetch sharding catalog snapshot; keeping the cached one",
			append([]any{"database", db.name, "error", err}, cachedAttrs(cur)...)...)
		return
	}
	if resp.GetUnchanged() {
		return
	}
	pb := resp.GetSnapshot()
	if pb == nil {
		c.metrics.fetchFailed(ctx, db.name, failureInvalid)
		c.logger.WarnContext(ctx, "catalog primary returned neither a snapshot nor unchanged; keeping the cached one",
			append([]any{"database", db.name}, cachedAttrs(cur)...)...)
		return
	}
	pos := Position{Rule: resp.GetConsensusRule(), RoutingVersion: pb.GetRoutingVersion()}
	// A primary that has not learned it was superseded serves under an older
	// rule, or one that has not caught up an older version under the same
	// rule. The same position means the same contents, so there is nothing to
	// install either.
	if cur != nil && pos.Compare(cur.position) <= 0 {
		if pos.Compare(cur.position) < 0 {
			c.logger.WarnContext(ctx, "ignoring an older sharding catalog snapshot",
				append([]any{"database", db.name, "routing_version", pos.RoutingVersion, "consensus_rule", commonconsensus.FormatRuleNumber(pos.Rule)},
					cachedAttrs(cur)...)...)
		}
		return
	}
	snap, err := shardingcatalog.New(pb)
	if err != nil {
		c.metrics.fetchFailed(ctx, db.name, failureInvalid)
		c.logger.ErrorContext(ctx, "rejected sharding catalog snapshot; keeping the cached one",
			append([]any{"database", db.name, "routing_version", pos.RoutingVersion, "consensus_rule", commonconsensus.FormatRuleNumber(pos.Rule), "error", err},
				cachedAttrs(cur)...)...)
		return
	}
	// The loop is the only writer, so nothing raced this install.
	db.current.Store(&installed{position: pos, snapshot: snap})
	db.notifyInstalled()
	c.metrics.installed(ctx, db.name, snap.RoutingVersion())
	c.logger.InfoContext(ctx, "installed sharding catalog snapshot",
		append([]any{
			"database", db.name, "routing_version", snap.RoutingVersion(), "plan_epoch", snap.PlanEpoch(),
			"consensus_rule", commonconsensus.FormatRuleNumber(pos.Rule), "tablegroups", len(snap.TableGroups()),
		}, cachedAttrs(cur)...)...)
}

// cachedAttrs describes the cached snapshot for a log line.
func cachedAttrs(cur *installed) []any {
	if cur == nil {
		return []any{"cached", false}
	}
	return []any{
		"cached_routing_version", cur.position.RoutingVersion,
		"cached_consensus_rule", commonconsensus.FormatRuleNumber(cur.position.Rule),
	}
}
