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

// Package routing caches each database's routing rules in the gateway and
// applies them to application targets.
//
// A database's rules live in its catalog, on the primary of its default
// tablegroup. The gateway fetches a snapshot of them the first time it routes
// a statement of the database, and polls for a newer one afterwards. Until the
// first snapshot arrives the database's application traffic fails with a
// retryable error (57P03): routing it to the catalog's placement could reach
// the wrong side of an operation. Control traffic (credential lookups, the
// snapshot reads themselves) is never rewritten and needs no snapshot.
//
// Rules apply at execute time, as a single hop: a shard rule from tablegroup
// G rewrites a target of G's shard S to the rule's target shard when the
// rule's range covers S. The planner keeps routing to the catalog's
// placement.
package routing

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// SnapshotSource reads a database's routing snapshot from its catalog
// primary. With waitForFailover, a read that finds the catalog primary
// failing over waits for the new one, as application traffic does; without
// it the read fails at once.
type SnapshotSource interface {
	GetRoutingSnapshot(ctx context.Context, database string, waitForFailover bool) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error)
}

// Config tunes the cache.
type Config struct {
	// PollInterval is how often the snapshot of every database in use is
	// fetched again. Polling is the only way the gateway learns of a rule
	// change until catalog primaries publish the routing version on their
	// health stream; that signal is what will let this interval grow.
	PollInterval time.Duration
	// FetchTimeout bounds one background poll read.
	FetchTimeout time.Duration
	// FirstFetchTimeout bounds the read that the first statements of a
	// database wait for. That read waits out a failover of the catalog
	// primary in the failover buffer, which bounds it more tightly; each
	// waiting statement is also bounded by its own context. This is only a
	// backstop for a read that never returns.
	FirstFetchTimeout time.Duration
	// IdleTimeout is how long a database's routing is kept and polled after
	// its last statement.
	IdleTimeout time.Duration
}

// DefaultConfig is the configuration a gateway uses unless told otherwise.
var DefaultConfig = Config{
	PollInterval:      10 * time.Second,
	FetchTimeout:      2 * time.Second,
	FirstFetchTimeout: time.Minute,
	IdleTimeout:       10 * time.Minute,
}

// Cache holds the routing of every database the gateway serves. It is safe
// for concurrent use.
type Cache struct {
	src    SnapshotSource
	cfg    Config
	logger *slog.Logger
	now    func() time.Time

	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
	done    chan struct{}

	mu        sync.Mutex
	databases map[string]*databaseRouting
	listeners []func(database string)
}

// databaseRouting is the routing of one database.
type databaseRouting struct {
	snapshot atomic.Pointer[snapshot]
	lastUsed atomic.Int64 // unix nanoseconds

	mu    sync.Mutex
	fetch *fetchCall // the snapshot read in flight, if any
	// warnedUnimplemented is set once the catalog primary was found not to
	// serve snapshots.
	warnedUnimplemented bool
}

// fetchCall is one snapshot read, shared by everyone who waits for it.
type fetchCall struct {
	done     chan struct{}
	snapshot *snapshot
	err      error
}

// snapshot is a database's routing at one routing version.
type snapshot struct {
	version int64
	// rules maps a source tablegroup to its shard rules.
	rules map[string][]shardRule
}

// shardRule sends the routing identifiers of its source tablegroup in
// [start, end) to toShard of toTablegroup. A nil start or end is unbounded.
type shardRule struct {
	toTablegroup string
	toShard      string
	start, end   []byte
}

// NewCache returns a cache that reads snapshots from src. Start begins the
// poll. Cancelling ctx stops it, like Close.
func NewCache(ctx context.Context, src SnapshotSource, cfg Config, logger *slog.Logger) *Cache {
	ctx, cancel := context.WithCancel(ctx)
	return &Cache{
		src:       src,
		cfg:       cfg,
		logger:    logger,
		now:       time.Now,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		databases: map[string]*databaseRouting{},
	}
}

// OnChange registers fn to be called, after the new routing is installed,
// whenever a database's routing changes after the gateway first learned it.
// It must be called before Start.
func (c *Cache) OnChange(fn func(database string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listeners = append(c.listeners, fn)
}

// Start begins polling the routing of the databases in use.
func (c *Cache) Start() {
	if c.started.CompareAndSwap(false, true) {
		go c.pollLoop()
	}
}

// Close stops the poll and any snapshot read in flight.
func (c *Cache) Close() {
	c.cancel()
	if c.started.Load() {
		<-c.done
	}
}

// Resolve returns the target that serves target under its database's
// routing rules: target itself when no rule applies, a new target otherwise.
// A control target is returned as is. While the database's routing is not
// known it returns a retryable 57P03 error.
func (c *Cache) Resolve(ctx context.Context, target *querypb.Target) (*querypb.Target, error) {
	if target == nil || target.GetControl() {
		return target, nil
	}
	ref := target.GetShardRef()
	snap, err := c.snapshotFor(ctx, ref.GetDatabase())
	if err != nil {
		return nil, err
	}
	rules := snap.rules[ref.GetTableGroup()]
	if len(rules) == 0 {
		return target, nil
	}
	return resolveShard(target, rules)
}

// resolveShard applies the shard rules of target's tablegroup to target's
// shard.
func resolveShard(target *querypb.Target, rules []shardRule) (*querypb.Target, error) {
	ref := target.GetShardRef()
	kr, err := keyrange.ParseShardName(ref.GetShard())
	if err != nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "cannot route shard %q of tablegroup %q: %v", ref.GetShard(), ref.GetTableGroup(), err)
	}
	var match *shardRule
	for i := range rules {
		r := &rules[i]
		if !overlaps(kr.GetStart(), kr.GetEnd(), r.start, r.end) {
			continue
		}
		if match != nil || !covers(r.start, r.end, kr.GetStart(), kr.GetEnd()) {
			// Several rules split the shard: which one serves a statement
			// depends on its routing identifier, which the gateway does
			// not compute yet. Fail rather than guess.
			return nil, mterrors.Errorf(mtrpcpb.Code_UNIMPLEMENTED,
				"shard %q of tablegroup %q is split by routing rules, which needs per-row routing", ref.GetShard(), ref.GetTableGroup())
		}
		match = r
	}
	if match == nil {
		return target, nil
	}
	return &querypb.Target{
		ShardRef: &clustermetadatapb.ShardRef{Database: ref.GetDatabase(), TableGroup: match.toTablegroup, Shard: match.toShard},
		Mode:     target.GetMode(),
	}, nil
}

// snapshotFor returns database's routing, waiting for the first snapshot if
// the gateway has none yet.
func (c *Cache) snapshotFor(ctx context.Context, database string) (*snapshot, error) {
	d := c.database(database)
	d.lastUsed.Store(c.now().UnixNano())
	if snap := d.snapshot.Load(); snap != nil {
		return snap, nil
	}
	call := c.startFetch(database, d, true)
	select {
	case <-call.done:
	case <-ctx.Done():
		return nil, unknownRoutingError(database, ctx.Err())
	}
	if call.snapshot == nil {
		return nil, unknownRoutingError(database, call.err)
	}
	return call.snapshot, nil
}

// unknownRoutingError is the retryable error of application traffic of a
// database whose routing the gateway does not know.
func unknownRoutingError(database string, cause error) error {
	diagnostic := mterrors.NewPgError("ERROR", mterrors.PgSSCannotConnectNow,
		"the database's routing is not available yet; please retry", "")
	return mterrors.WithCode(
		mterrors.Wrapf(diagnostic, "routing of database %q is unknown: %v", database, cause),
		mtrpcpb.Code_UNAVAILABLE,
	)
}

// database returns the routing entry of database, creating it.
func (c *Cache) database(database string) *databaseRouting {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.databases[database]
	if d == nil {
		d = &databaseRouting{}
		c.databases[database] = d
	}
	return d
}

// startFetch starts a snapshot read of database, or joins the one in flight.
// first marks the read statements wait for, which waits out a failover of the
// catalog primary. The read a statement waits for is always a first one: the
// poll reads only databases whose routing the gateway already has.
func (c *Cache) startFetch(database string, d *databaseRouting, first bool) *fetchCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fetch != nil {
		return d.fetch
	}
	if snap := d.snapshot.Load(); first && snap != nil {
		// The statement found no snapshot, but the read it would have joined
		// installed one and finished since: it is done.
		call := &fetchCall{done: make(chan struct{}), snapshot: snap}
		close(call.done)
		return call
	}
	call := &fetchCall{done: make(chan struct{})}
	d.fetch = call
	timeout := c.cfg.FetchTimeout
	if first {
		timeout = c.cfg.FirstFetchTimeout
	}
	go func() {
		ctx, cancel := context.WithTimeout(c.ctx, timeout)
		call.snapshot, call.err = c.load(ctx, database, d, first)
		cancel()
		d.mu.Lock()
		d.fetch = nil
		d.mu.Unlock()
		close(call.done)
	}()
	return call
}

// load reads database's snapshot and installs it if it is newer than the one
// the gateway has. It returns the installed snapshot, which is the previous
// one if the read failed or was not newer, and the read's error.
func (c *Cache) load(ctx context.Context, database string, d *databaseRouting, waitForFailover bool) (*snapshot, error) {
	resp, err := c.src.GetRoutingSnapshot(ctx, database, waitForFailover)
	if isUnimplemented(err) {
		// The catalog primary predates routing rules, so the database has
		// none. Upgrade every pooler of a database's default tablegroup
		// before creating a rule; otherwise this reads as "no rules".
		d.mu.Lock()
		warn := !d.warnedUnimplemented
		d.warnedUnimplemented = true
		d.mu.Unlock()
		if warn {
			c.logger.WarnContext(ctx, "the catalog primary does not serve routing snapshots; routing the database as it is placed",
				"database", database)
		}
		resp, err = &multipoolermanagerdatapb.GetRoutingSnapshotResponse{Full: true}, nil
	}
	cur := d.snapshot.Load()
	if err != nil {
		if cur != nil {
			c.logger.DebugContext(ctx, "routing snapshot read failed; keeping the last one",
				"database", database, "routing_version", cur.version, "error", err)
		}
		return cur, err
	}

	next := newSnapshot(resp)
	if cur != nil && next.version <= cur.version {
		// The routing version only grows; an older snapshot (a lagging
		// read) is never installed.
		return cur, nil
	}
	d.snapshot.Store(next)
	if cur == nil {
		c.logger.InfoContext(ctx, "routing learned", "database", database, "routing_version", next.version, "rules", len(resp.GetRules()))
		return next, nil
	}
	c.logger.InfoContext(ctx, "routing changed", "database", database,
		"from_routing_version", cur.version, "routing_version", next.version, "rules", len(resp.GetRules()))
	c.notify(database)
	return next, nil
}

func (c *Cache) notify(database string) {
	c.mu.Lock()
	listeners := c.listeners
	c.mu.Unlock()
	for _, fn := range listeners {
		fn(database)
	}
}

// newSnapshot keeps the shard rules of resp. Table rules apply when a query
// is planned, which the gateway does not do per table yet, so they are not
// kept.
func newSnapshot(resp *multipoolermanagerdatapb.GetRoutingSnapshotResponse) *snapshot {
	snap := &snapshot{version: resp.GetRoutingVersion(), rules: map[string][]shardRule{}}
	for _, r := range resp.GetRules() {
		if r.GetScope() != multipoolermanagerdatapb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP {
			continue
		}
		snap.rules[r.GetFromTablegroup()] = append(snap.rules[r.GetFromTablegroup()], shardRule{
			toTablegroup: r.GetToTablegroup(),
			toShard:      r.GetToShard(),
			start:        nilIfEmpty(r.GetToKeyRange().GetStart()),
			end:          nilIfEmpty(r.GetToKeyRange().GetEnd()),
		})
	}
	return snap
}

// pollLoop reads every database's snapshot again each PollInterval, and
// forgets the databases no statement used for IdleTimeout.
func (c *Cache) pollLoop() {
	defer close(c.done)
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
		c.pollOnce()
	}
}

// pollOnce starts one snapshot read per database in use whose routing the
// gateway has. It does not wait: each read is bounded by FetchTimeout and
// joins any read already in flight. A database whose first read failed is
// read again by its next statement, which waits out a failover; the poll
// leaves it alone so a statement never waits on a short poll read.
func (c *Cache) pollOnce() {
	idleBefore := c.now().Add(-c.cfg.IdleTimeout).UnixNano()
	c.mu.Lock()
	var poll []*databaseRouting
	var names []string
	for name, d := range c.databases {
		if d.lastUsed.Load() < idleBefore {
			delete(c.databases, name)
			continue
		}
		if d.snapshot.Load() != nil {
			poll = append(poll, d)
			names = append(names, name)
		}
	}
	c.mu.Unlock()
	for i, d := range poll {
		c.startFetch(names[i], d, false)
	}
}

func isUnimplemented(err error) bool {
	if err == nil {
		return false
	}
	return status.Code(err) == codes.Unimplemented || mterrors.Code(err) == mtrpcpb.Code_UNIMPLEMENTED
}

// overlaps reports whether [aStart, aEnd) and [bStart, bEnd) intersect; a nil
// start or end is unbounded.
func overlaps(aStart, aEnd, bStart, bEnd []byte) bool {
	return before(aStart, bEnd) && before(bStart, aEnd)
}

// covers reports whether [outerStart, outerEnd) contains [innerStart,
// innerEnd).
func covers(outerStart, outerEnd, innerStart, innerEnd []byte) bool {
	startOK := outerStart == nil || (innerStart != nil && bytes.Compare(outerStart, innerStart) <= 0)
	endOK := outerEnd == nil || (innerEnd != nil && bytes.Compare(innerEnd, outerEnd) <= 0)
	return startOK && endOK
}

// before reports whether start < end, where a nil end is unbounded (a nil
// start already sorts first).
func before(start, end []byte) bool {
	return end == nil || bytes.Compare(start, end) < 0
}

func nilIfEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}
