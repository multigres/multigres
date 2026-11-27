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

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// ============================================================================
// Admission fence acks (catalog shard only)
//
// An ack (multigres.admission_fence_ack) records that a shard's primary was
// drained for a fence (fence_refresh.go): one row per fence and shard. One ack
// per shard is enough, since every later primary of the shard starts with its
// gate unread and refuses work until it reads the fence. Deleting a fence
// deletes its acks (ON DELETE CASCADE), so a missing ack means no guarantee.
// Acks are not in the routing snapshot: recording one takes no catalog_state
// lock and advances no version. Nothing reads them yet.
//
// Each ack takes a FOR KEY SHARE lock on its fence row before inserting, so a
// concurrent unfence either committed first (the ack is skipped) or waits and
// deletes the ack with the fence: the insert never fails its foreign key. Its
// shard cannot be deleted under it either: shards go only with their
// tablegroup, which is refused while the tablegroup has a fence.
// ============================================================================

const (
	// lockAckedFenceSQL locks the fence an ack names and reports whether the
	// shard is in its scope: the fence's own shard or, for a fence on a whole
	// tablegroup, a shard of that tablegroup. No row means the fence does not
	// exist.
	lockAckedFenceSQL = `SELECT coalesce(f.shard_id = s.shard_id OR (f.shard_id IS NULL AND s.tablegroup_id = f.tablegroup_id), false)
FROM multigres.admission_fence f
LEFT JOIN multigres.shard s ON s.shard_id = $2
WHERE f.fence_id = $1
FOR KEY SHARE OF f`

	// upsertFenceAckSQL records an ack, replacing an existing one, so the row
	// names the latest primary to ack (after a failover, the new one).
	upsertFenceAckSQL = `INSERT INTO multigres.admission_fence_ack (fence_id, shard_id, pooler_id, consensus_rule)
VALUES ($1, $2, $3, $4)
ON CONFLICT (fence_id, shard_id) DO UPDATE
	SET pooler_id = EXCLUDED.pooler_id, consensus_rule = EXCLUDED.consensus_rule, acked_at = now()`
)

// AckFences records that the sender's shard is drained for each fence. See
// the file comment and the RPC's proto comment for the contract.
func (pm *MultipoolerManager) AckFences(ctx context.Context, req *multipoolermanagerdatapb.AckFencesRequest) (*multipoolermanagerdatapb.AckFencesResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	// As GetRoutingSnapshot: a pooler of the catalog shard that is not its
	// elected primary answers UNAVAILABLE, so the sender finds the primary
	// and asks again.
	if err := pm.checkCatalogShard(); err != nil {
		return nil, err
	}
	if _, err := pm.catalogPrimaryRule(ctx); err != nil {
		return nil, mterrors.WithCode(err, mtrpcpb.Code_UNAVAILABLE)
	}
	if err := checkFenceAcks(req.GetAcks()); err != nil {
		return nil, err
	}
	if err := pm.recordFenceAcks(ctx, req.GetAcks()); err != nil {
		return nil, err
	}
	return &multipoolermanagerdatapb.AckFencesResponse{}, nil
}

// checkFenceAcks checks the shape of a list of acks: at least one, each with
// positive ids and a pooler_id.
func checkFenceAcks(acks []*multipoolermanagerdatapb.FenceAck) error {
	if len(acks) == 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "no fence acks")
	}
	for _, a := range acks {
		if a.GetFenceId() <= 0 || a.GetShardId() <= 0 {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"a fence ack needs a positive fence_id and shard_id, got fence %d, shard %d", a.GetFenceId(), a.GetShardId())
		}
		if a.GetPoolerId() == "" {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "the ack of fence %d for shard %d has no pooler_id", a.GetFenceId(), a.GetShardId())
		}
	}
	return nil
}

// recordFenceAcks records acks in one transaction (see the file comment). An
// ack of a fence that no longer exists is skipped. An ack whose shard is not
// in its fence's scope fails the whole call with FAILED_PRECONDITION.
func (pm *MultipoolerManager) recordFenceAcks(ctx context.Context, acks []*multipoolermanagerdatapb.FenceAck) error {
	qs, err := pm.catalogQueryService()
	if err != nil {
		return err
	}
	execCtx, cancel := context.WithTimeout(ctx, tableGroupOpTimeout)
	defer cancel()
	tx, err := qs.BeginAdmin(execCtx)
	if err != nil {
		return mterrors.Wrap(err, "failed to begin fence ack transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, a := range acks {
		if err := pm.recordFenceAck(execCtx, tx, a); err != nil {
			return err
		}
	}
	if err := tx.Commit(execCtx); err != nil {
		return mterrors.Wrap(err, "failed to commit fence acks")
	}
	return nil
}

// recordFenceAck records one ack in tx.
func (pm *MultipoolerManager) recordFenceAck(ctx context.Context, tx executor.InternalTx, a *multipoolermanagerdatapb.FenceAck) error {
	res, err := tx.QueryArgs(ctx, lockAckedFenceSQL, a.GetFenceId(), a.GetShardId())
	if err != nil {
		return mterrors.Wrapf(err, "failed to read admission fence %d", a.GetFenceId())
	}
	if len(res.Rows) == 0 {
		// The fence was lifted: there is nothing left to acknowledge.
		return nil
	}
	var inScope bool
	if err := executor.ScanRow(res.Rows[0], &inScope); err != nil {
		return mterrors.Wrapf(err, "failed to read admission fence %d", a.GetFenceId())
	}
	if !inScope {
		pm.logger.WarnContext(ctx, "refusing a fence ack for a shard outside the fence's scope",
			"fence_id", a.GetFenceId(), "shard_id", a.GetShardId(), "pooler_id", a.GetPoolerId())
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"shard %d is not in the scope of admission fence %d", a.GetShardId(), a.GetFenceId())
	}
	if _, err := tx.QueryArgs(ctx, upsertFenceAckSQL, a.GetFenceId(), a.GetShardId(),
		a.GetPoolerId(), a.GetConsensusRule()); err != nil {
		return mterrors.Wrapf(err, "failed to record the ack of admission fence %d", a.GetFenceId())
	}
	return nil
}
