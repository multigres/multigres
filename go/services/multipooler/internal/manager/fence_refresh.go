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
	"context"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/tools/retry"
)

// Reading admission fences (every pooler but a static leader)
//
// A pooler learns whether its scope is fenced by reading its database's
// catalog snapshot from the catalog primary, the way gateways read it, and
// applies the answer to its query server's fence gate (poolerserver
// fence_gate.go). It reads:
//
//   - on RefreshFences, which the operation that fences a scope calls on every
//     pooler of it;
//   - when the gate is unread: once the pooler is registered in the topology
//     after it starts, and each time it becomes primary. Until then it refuses
//     new work with MTF02, which the gateway retries with backoff;
//   - every fenceRecheckInterval while it is fenced, so that an unfence it was
//     not refreshed for is eventually applied. An unfenced pooler does not poll.
//
// A pooler reads only once it is registered in the topology. The coordinator
// that fences a scope lists the scope's poolers after it has recorded the
// fence, so a new pooler is either listed, and refreshed, or reads a catalog
// that already holds the fence.
//
// The gate applies a read only if its snapshot is not older, by consensus rule
// and then routing version, than the newest one it has applied (poolerserver
// fence_gate.go). A catalog primary that a failover deposed may still be found
// in the topology and still answer, with an older snapshot; that answer is
// refused, applying nothing, and read again.
//
// A read that fails applies nothing: an unread gate stays closed and the
// reader retries with backoff. A catalog primary on which the catalog was
// never created answers NOT_FOUND, which reads as no fences: every fence lives
// in the catalog. The catalog primary reads its own catalog in process, so its
// own start-up never waits on itself.

// fenceRecheckInterval is how often a fenced primary re-reads the fences. It
// bounds how long an unfence the pooler was not refreshed for (it was
// unreachable when the operation lifted the fence) keeps it closed. A fence is
// lifted after a routing switch, when the gateway already waits on the new
// routing for at most its fence window, so this need not be fast; it is short
// enough that a missed unfence is not an outage, and the read it costs (one
// snapshot from the catalog primary) is paid only by fenced poolers. A var so
// tests can shorten it.
var fenceRecheckInterval = 30 * time.Second

// Backoff of the background fence read after a failure.
var (
	fenceReadRetryBase = 100 * time.Millisecond
	fenceReadRetryMax  = 10 * time.Second
)

// fenceReadTimeout bounds one read of the catalog snapshot.
const fenceReadTimeout = 10 * time.Second

// RefreshFences reads the fences on this pooler's scope from the catalog
// primary and applies them to the fence gate. See the RPC's proto comment.
func (pm *MultipoolerManager) RefreshFences(ctx context.Context, req *multipoolermanagerdatapb.RefreshFencesRequest) (*multipoolermanagerdatapb.RefreshFencesResponse, error) {
	if !pm.fenceGateEnabled {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "this pooler serves a single unsharded database and has no admission fences")
	}
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	if d := req.GetDrainTimeout(); d != nil {
		if err := d.CheckValid(); err != nil || d.AsDuration() < 0 || d.AsDuration() > poolerserver.MaxFenceDrainTimeout {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid drain_timeout %v: it must be between 0 and %v", d, poolerserver.MaxFenceDrainTimeout)
		}
	}
	version, fenced, err := pm.refreshFences(ctx, req.GetMinRoutingVersion(), req.GetDrainTimeout().AsDuration())
	if err != nil {
		return nil, err
	}
	return &multipoolermanagerdatapb.RefreshFencesResponse{RoutingVersion: version, Fenced: fenced}, nil
}

// refreshFences reads the catalog snapshot, checks it is at minRoutingVersion
// or later, and applies whether it fences this pooler's scope, draining the
// pooler when it has just become fenced. It returns the snapshot's routing
// version and whether the scope is fenced. Reads may run concurrently; the
// gate orders them (poolerserver fence_gate.go). A read the gate refuses
// because a promotion reset it, or because the drain it ran or joined was
// cancelled, is read again; one it refuses as older than a snapshot already
// applied fails with UNAVAILABLE.
func (pm *MultipoolerManager) refreshFences(ctx context.Context, minRoutingVersion int64, drainTimeout time.Duration) (int64, bool, error) {
	for {
		epoch := pm.qsc.FenceEpoch()
		pos, fenced, err := pm.readFences(ctx)
		if err != nil {
			return 0, false, err
		}
		if pos.RoutingVersion < minRoutingVersion {
			return 0, false, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"the catalog primary's routing version is %d, below the requested %d", pos.RoutingVersion, minRoutingVersion)
		}
		err = pm.qsc.ApplyFence(ctx, poolerserver.FenceRead{Epoch: epoch, Position: pos, Fenced: fenced}, drainTimeout)
		if mterrors.Code(err) == mtrpcpb.Code_ABORTED && ctx.Err() == nil {
			continue
		}
		if err != nil {
			return 0, false, err
		}
		return pos.RoutingVersion, fenced, nil
	}
}

// readFences reads the catalog snapshot and returns its position and whether
// a fence applies to this pooler's scope. A database with no catalog yet reads
// as the zero position with no fences.
func (pm *MultipoolerManager) readFences(ctx context.Context) (shardingcatalog.Position, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, fenceReadTimeout)
	defer cancel()
	resp, err := pm.readCatalogSnapshot(ctx)
	if mterrors.Code(err) == mtrpcpb.Code_NOT_FOUND {
		return shardingcatalog.Position{}, false, nil
	}
	if err != nil {
		return shardingcatalog.Position{}, false, err
	}
	snap, err := shardingcatalog.New(resp.GetSnapshot())
	if err != nil {
		return shardingcatalog.Position{}, false, mterrors.WithCode(mterrors.Wrap(err, "the catalog primary served an invalid routing snapshot"), mtrpcpb.Code_UNAVAILABLE)
	}
	pos := shardingcatalog.Position{Rule: resp.GetConsensusRule(), RoutingVersion: snap.RoutingVersion()}
	return pos, pm.scopeFenced(snap), nil
}

// scopeFenced reports whether snap fences this pooler's scope: its whole
// tablegroup or its shard, by the definition gateways use
// (shardingcatalog.TableGroup.Fenced). The tablegroup is matched by catalog id
// when the pooler has one, which pins the incarnation it was provisioned for,
// and otherwise by name. The default tablegroup's poolers, and poolers of a
// tablegroup configured outside the catalog, have no id; the default
// tablegroup's name is permanent and never reused, so its name is as exact as
// an id. A tablegroup the snapshot does not hold has no fences.
func (pm *MultipoolerManager) scopeFenced(snap *shardingcatalog.Snapshot) bool {
	ref := pm.record.ShardRef()
	var g *shardingcatalog.TableGroup
	var ok bool
	if id := pm.record.TableGroupID(); id > 0 {
		g, ok = snap.TableGroupByID(id)
	} else {
		g, ok = snap.TableGroupByName(ref.GetTableGroup())
	}
	return ok && g.Fenced(ref.GetShard())
}

// readCatalogSnapshot reads the full catalog snapshot from the catalog
// primary. The catalog primary itself reads its own catalog in process; any
// other pooler finds the catalog primary in the topology and asks it. A
// catalog primary on which the catalog was never created answers NOT_FOUND; a
// catalog primary that cannot be found or reached fails with UNAVAILABLE.
func (pm *MultipoolerManager) readCatalogSnapshot(ctx context.Context) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	if pm.isCatalogShard() && pm.stateManager.RoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return pm.GetRoutingSnapshot(ctx, &multipoolermanagerdatapb.GetRoutingSnapshotRequest{})
	}
	if pm.config.ManagerClient == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "this pooler has no client to read the catalog primary with")
	}
	primary, err := commonconsensus.FindRoutingPrimary(ctx, pm.topoClient, &clustermetadatapb.ShardRef{
		Database: pm.record.ShardRef().GetDatabase(), TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard,
	})
	if err != nil {
		return nil, err
	}
	resp, err := pm.config.ManagerClient.GetRoutingSnapshot(ctx, primary, &multipoolermanagerdatapb.GetRoutingSnapshotRequest{})
	if err != nil {
		err = mterrors.FromGRPC(err)
		if mterrors.Code(err) == mtrpcpb.Code_NOT_FOUND {
			return nil, err
		}
		return nil, mterrors.WithCode(mterrors.Wrapf(err, "failed to read the routing snapshot from catalog primary %s",
			topoclient.ComponentIDString(primary.GetId())), mtrpcpb.Code_UNAVAILABLE)
	}
	return resp, nil
}

// startFenceReads starts the background fence reader once, on the manager's
// lifetime context. It runs from topology registration (see
// StartTopoRegistration), which it waits for before its first read.
func (pm *MultipoolerManager) startFenceReads() {
	if !pm.fenceGateEnabled {
		return
	}
	pm.fenceReadsOnce.Do(func() {
		go pm.runFenceReads(pm.shutdownCtx)
	})
}

// runFenceReads reads the fences whenever the gate of a primary is unread, and
// every fenceRecheckInterval while it is fenced, until ctx ends. It waits for
// the pooler's topology registration first. A failed read is retried with
// backoff, logging its reason; nothing but application work on a primary waits
// for it.
func (pm *MultipoolerManager) runFenceReads(ctx context.Context) {
	select {
	case <-pm.record.Registered():
	case <-ctx.Done():
		return
	}
	needed := pm.qsc.FenceGateChanged()
	backoff := retry.NewExponentialBackoff(fenceReadRetryBase, fenceReadRetryMax)
	recheck := false
	var lastErr string
	for {
		unread, closed := pm.qsc.FenceReadState()
		var wait <-chan time.Time
		if unread || (recheck && closed) {
			version, fenced, err := pm.refreshFences(ctx, 0, 0)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if msg := err.Error(); msg != lastErr {
					pm.logger.WarnContext(ctx, "failed to read admission fences; retrying", "error", err)
					lastErr = msg
				}
				wait = time.After(backoff.NextDelay())
			} else {
				pm.logger.InfoContext(ctx, "read admission fences", "routing_version", version, "fenced", fenced)
				backoff.Reset()
				lastErr = ""
				recheck = false
				if fenced {
					wait = time.After(fenceRecheckInterval)
				}
			}
		} else if closed {
			wait = time.After(fenceRecheckInterval)
		}
		select {
		case <-ctx.Done():
			return
		case <-needed:
		case <-wait:
			recheck = true
		}
	}
}
