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
	"sync"
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
//
// Acknowledging fences
//
// Once a read is applied and the gate of a routing primary is closed in the
// epoch the read was taken in (poolerserver QueryPoolerServer.
// FenceClosedOnPrimary), the pooler is drained for every fence covering its
// shard, and it records that on the catalog primary (AckFences, fence_acks.go):
// one ack per fence, naming its shard's catalog id.
// A replica, a gate that is open, unread or still draining (a failed drain),
// and a read whose epoch has passed (a promotion reset the gate) ack nothing.
// A pooler whose shard the snapshot does not hold has no shard to ack for.
//
// The read succeeds only once its acks are recorded: a failed ack fails
// RefreshFences, with the gate left closed, and the background reader, which
// re-reads a closed gate every fenceRecheckInterval and a failed read with
// backoff, sends it again. So that a recheck does not write the catalog each
// time, the pooler remembers what it acknowledged in the current epoch
// (fenceAckSet) and sends only what is new; a restart or a promotion starts a
// new epoch and acknowledges again, which only refreshes the recorded row.

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
// pooler when it has just become fenced. When that leaves a primary's gate
// closed in the read's epoch, it acknowledges the fences covering the
// pooler's shard (see the file comment) and fails if it cannot. It returns
// the snapshot's routing version and whether the scope is fenced. Reads may
// run concurrently; the gate orders them (poolerserver fence_gate.go). A read
// the gate refuses because a promotion reset it, or because the drain it ran
// or joined was cancelled, is read again; one it refuses as older than a
// snapshot already applied fails with UNAVAILABLE.
func (pm *MultipoolerManager) refreshFences(ctx context.Context, minRoutingVersion int64, drainTimeout time.Duration) (int64, bool, error) {
	for {
		epoch := pm.qsc.FenceEpoch()
		read, err := pm.readFences(ctx)
		if err != nil {
			return 0, false, err
		}
		if read.pos.RoutingVersion < minRoutingVersion {
			return 0, false, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"the catalog primary's routing version is %d, below the requested %d", read.pos.RoutingVersion, minRoutingVersion)
		}
		err = pm.qsc.ApplyFence(ctx, poolerserver.FenceRead{Epoch: epoch, Position: read.pos, Fenced: read.fenced}, drainTimeout)
		if mterrors.Code(err) == mtrpcpb.Code_ABORTED && ctx.Err() == nil {
			continue
		}
		if err != nil {
			return 0, false, err
		}
		if read.fenced && pm.qsc.FenceClosedOnPrimary(epoch) {
			if err := pm.ackFences(ctx, epoch, read); err != nil {
				return 0, false, err
			}
		}
		return read.pos.RoutingVersion, read.fenced, nil
	}
}

// fenceRead is what one read of the catalog snapshot found for this pooler.
type fenceRead struct {
	// pos is the snapshot's position: the zero Position when the database has
	// no catalog yet.
	pos shardingcatalog.Position
	// fenced reports whether a fence applies to the pooler's scope (see
	// scopeFences).
	fenced bool
	// shardID is the catalog id of the pooler's shard, 0 when the snapshot
	// does not hold it.
	shardID int64
	// fences are the fences covering that shard, which an ack names: the
	// whole-tablegroup fence, then the shard's own. Empty when shardID is 0.
	fences []*shardingcatalog.Fence
}

// readFences reads the catalog snapshot and returns what it holds for this
// pooler's scope. A database with no catalog yet reads as the zero position
// with no fences.
func (pm *MultipoolerManager) readFences(ctx context.Context) (fenceRead, error) {
	ctx, cancel := context.WithTimeout(ctx, fenceReadTimeout)
	defer cancel()
	resp, err := pm.readCatalogSnapshot(ctx)
	if mterrors.Code(err) == mtrpcpb.Code_NOT_FOUND {
		return fenceRead{}, nil
	}
	if err != nil {
		return fenceRead{}, err
	}
	snap, err := shardingcatalog.New(resp.GetSnapshot())
	if err != nil {
		return fenceRead{}, mterrors.WithCode(mterrors.Wrap(err, "the catalog primary served an invalid routing snapshot"), mtrpcpb.Code_UNAVAILABLE)
	}
	read := pm.scopeFences(snap)
	read.pos = shardingcatalog.Position{Rule: resp.GetConsensusRule(), RoutingVersion: snap.RoutingVersion()}
	return read, nil
}

// scopeFences returns whether snap fences this pooler's scope, its shard's
// catalog id and the fences covering that shard. Fenced means its whole
// tablegroup or its shard is fenced, by the definition gateways use
// (shardingcatalog.TableGroup.Fenced). The tablegroup is matched by catalog id
// when the pooler has one, which pins the incarnation it was provisioned for,
// and otherwise by name. The default tablegroup's poolers, and poolers of a
// tablegroup configured outside the catalog, have no id; the default
// tablegroup's name is permanent and never reused, so its name is as exact as
// an id. The shard is matched by name within the tablegroup. A tablegroup the
// snapshot does not hold has no fences; a shard it does not hold has no id
// and no fences to acknowledge, though a fence on its whole tablegroup still
// fences it.
func (pm *MultipoolerManager) scopeFences(snap *shardingcatalog.Snapshot) fenceRead {
	ref := pm.record.ShardRef()
	var g *shardingcatalog.TableGroup
	var ok bool
	if id := pm.record.TableGroupID(); id > 0 {
		g, ok = snap.TableGroupByID(id)
	} else {
		g, ok = snap.TableGroupByName(ref.GetTableGroup())
	}
	if !ok {
		return fenceRead{}
	}
	read := fenceRead{fenced: g.Fenced(ref.GetShard())}
	for _, sh := range g.Shards() {
		if sh.Name() != ref.GetShard() {
			continue
		}
		read.shardID = sh.ID()
		read.fences = g.FencesFor(sh)
		break
	}
	return read
}

// ackFences acknowledges, on the catalog primary, every fence of read that
// this pooler has not acknowledged in epoch yet (see the file comment), and
// remembers them once recorded. It fails with UNAVAILABLE when the catalog
// primary cannot record them.
func (pm *MultipoolerManager) ackFences(ctx context.Context, epoch uint64, read fenceRead) error {
	fences := pm.fenceAcks.unacked(epoch, read.fences)
	if len(fences) == 0 {
		return nil
	}
	poolerID := string(topoclient.ComponentIDString(pm.serviceID))
	var rule string
	if r := pm.routingRule(); r != nil {
		rule = commonconsensus.FormatRuleNumber(r)
	}
	req := &multipoolermanagerdatapb.AckFencesRequest{}
	for _, f := range fences {
		req.Acks = append(req.Acks, &multipoolermanagerdatapb.FenceAck{
			FenceId: f.ID(), ShardId: read.shardID, PoolerId: poolerID, ConsensusRule: rule,
		})
	}
	ctx, cancel := context.WithTimeout(ctx, fenceReadTimeout)
	defer cancel()
	if err := pm.sendFenceAcks(ctx, req); err != nil {
		return mterrors.WithCode(mterrors.Wrap(err, "failed to record the fence acks on the catalog primary"), mtrpcpb.Code_UNAVAILABLE)
	}
	pm.fenceAcks.record(epoch, fences)
	pm.logger.InfoContext(ctx, "acknowledged admission fences", "shard_id", read.shardID, "fences", len(fences))
	return nil
}

// sendFenceAcks sends req to the catalog primary, the way readCatalogSnapshot
// reads it: the catalog primary records its own acks in process.
func (pm *MultipoolerManager) sendFenceAcks(ctx context.Context, req *multipoolermanagerdatapb.AckFencesRequest) error {
	if pm.isCatalogPrimaryRole() {
		_, err := pm.AckFences(ctx, req)
		return err
	}
	primary, err := pm.findCatalogPrimary(ctx)
	if err != nil {
		return err
	}
	if _, err := pm.config.ManagerClient.AckFences(ctx, primary, req); err != nil {
		return mterrors.Wrapf(mterrors.FromGRPC(err), "catalog primary %s", topoclient.ComponentIDString(primary.GetId()))
	}
	return nil
}

// fenceAckSet is the fence ids a pooler has acknowledged in one fence gate
// epoch. Only the newest epoch is kept. The fence id alone names the ack: a
// fence belongs to one tablegroup and its id is never reused, and the pooler's
// shard in that tablegroup is fixed.
type fenceAckSet struct {
	mu    sync.Mutex
	epoch uint64
	acked map[int64]bool
}

// unacked returns the fences not acknowledged in epoch.
func (s *fenceAckSet) unacked(epoch uint64, fences []*shardingcatalog.Fence) []*shardingcatalog.Fence {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch != epoch {
		return fences
	}
	var out []*shardingcatalog.Fence
	for _, f := range fences {
		if !s.acked[f.ID()] {
			out = append(out, f)
		}
	}
	return out
}

// record remembers that fences were acknowledged in epoch. A newer epoch
// forgets the older one's acks; an older epoch is ignored.
func (s *fenceAckSet) record(epoch uint64, fences []*shardingcatalog.Fence) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch < s.epoch {
		return
	}
	if epoch > s.epoch || s.acked == nil {
		s.epoch = epoch
		s.acked = make(map[int64]bool)
	}
	for _, f := range fences {
		s.acked[f.ID()] = true
	}
}

// readCatalogSnapshot reads the full catalog snapshot from the catalog
// primary. The catalog primary itself reads its own catalog in process; any
// other pooler finds the catalog primary in the topology and asks it. A
// catalog primary on which the catalog was never created answers NOT_FOUND; a
// catalog primary that cannot be found or reached fails with UNAVAILABLE.
func (pm *MultipoolerManager) readCatalogSnapshot(ctx context.Context) (*multipoolermanagerdatapb.GetRoutingSnapshotResponse, error) {
	if pm.isCatalogPrimaryRole() {
		return pm.GetRoutingSnapshot(ctx, &multipoolermanagerdatapb.GetRoutingSnapshotRequest{})
	}
	primary, err := pm.findCatalogPrimary(ctx)
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

// isCatalogPrimaryRole reports whether this pooler serves the catalog shard
// with the routing role PRIMARY, so it serves the catalog itself.
func (pm *MultipoolerManager) isCatalogPrimaryRole() bool {
	return pm.isCatalogShard() && pm.stateManager.RoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
}

// findCatalogPrimary finds this database's catalog primary in the topology. It
// fails with UNAVAILABLE when there is none, or no client to reach it with.
func (pm *MultipoolerManager) findCatalogPrimary(ctx context.Context) (*clustermetadatapb.Multipooler, error) {
	if pm.config.ManagerClient == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "this pooler has no client to reach the catalog primary with")
	}
	return commonconsensus.FindRoutingPrimary(ctx, pm.topoClient, &clustermetadatapb.ShardRef{
		Database: pm.record.ShardRef().GetDatabase(), TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard,
	})
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
