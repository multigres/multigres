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

// Package routing applies each database's routing rules to application
// targets in the gateway.
//
// A database's rules are part of its sharding catalog snapshot, which the
// gateway's sharding catalog cache keeps (package shardingcatalogcache). The
// first statement of a database waits for its first snapshot. Until it
// arrives, the database's application traffic fails with a retryable error
// (57P03): routing it to the catalog's placement could reach the wrong side
// of an operation. A catalog primary that predates the snapshot means the
// database has no rules, and so does a gateway without a catalog cache (one
// with a single pooler).
//
// Rules apply at execute time, as a single hop: a shard rule from tablegroup
// G rewrites a target of G's shard S to the rule's target shard when the
// rule's range covers S. The planner keeps routing to the catalog's
// placement.
package routing

import (
	"bytes"
	"context"

	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// Catalog gives a database's sharding catalog snapshot to route by.
type Catalog interface {
	// Await returns database's snapshot, waiting for the first one when there
	// is none yet. It returns nil and no error when the database has no
	// catalog to route by, and an error when its routing is not known.
	Await(ctx context.Context, database string) (*shardingcatalog.Snapshot, error)
}

// Router applies the routing rules of each database's catalog to its
// application targets. It is safe for concurrent use.
type Router struct {
	catalog Catalog
}

// NewRouter returns a router over catalog. A nil catalog means no database
// has rules.
func NewRouter(catalog Catalog) *Router {
	return &Router{catalog: catalog}
}

// Resolve returns the target that serves target under its database's
// routing rules: target itself when no rule applies, a new target otherwise.
// While the database's routing is not known it returns a retryable 57P03
// error.
func (r *Router) Resolve(ctx context.Context, target *querypb.Target) (*querypb.Target, error) {
	if target == nil || r == nil || r.catalog == nil {
		return target, nil
	}
	ref := target.GetShardRef()
	snap, err := r.catalog.Await(ctx, ref.GetDatabase())
	if err != nil {
		return nil, unknownRoutingError(ref.GetDatabase(), err)
	}
	if snap == nil {
		return target, nil
	}
	tg, ok := snap.TableGroupByName(ref.GetTableGroup())
	if !ok {
		return target, nil
	}
	rules := tg.ShardRules()
	if len(rules) == 0 {
		return target, nil
	}
	return resolveShard(target, rules)
}

// resolveShard applies the shard rules of target's tablegroup to target's
// shard.
func resolveShard(target *querypb.Target, rules []*shardingcatalog.RoutingRule) (*querypb.Target, error) {
	ref := target.GetShardRef()
	kr, err := keyrange.ParseShardName(ref.GetShard())
	if err != nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "cannot route shard %q of tablegroup %q: %v", ref.GetShard(), ref.GetTableGroup(), err)
	}
	var match *shardingcatalog.RoutingRule
	for _, rule := range rules {
		to := rule.ToShard().KeyRange()
		start, end := nilIfEmpty(to.GetStart()), nilIfEmpty(to.GetEnd())
		if !overlaps(kr.GetStart(), kr.GetEnd(), start, end) {
			continue
		}
		if match != nil || !covers(start, end, kr.GetStart(), kr.GetEnd()) {
			// Several rules split the shard: which one serves a statement
			// depends on its routing identifier, which the gateway does
			// not compute yet. Fail rather than guess.
			return nil, mterrors.Errorf(mtrpcpb.Code_UNIMPLEMENTED,
				"shard %q of tablegroup %q is split by routing rules, which needs per-row routing", ref.GetShard(), ref.GetTableGroup())
		}
		match = rule
	}
	if match == nil {
		return target, nil
	}
	return &querypb.Target{
		ShardRef: &clustermetadatapb.ShardRef{
			Database:   ref.GetDatabase(),
			TableGroup: match.ToTableGroup().Name(),
			Shard:      match.ToShard().Name(),
		},
		Mode: target.GetMode(),
	}, nil
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
